"""
A股连板分析工具 - 统一入口
包含数据获取、分析、可视化功能
"""
import pandas as pd
import numpy as np
from pytdx.hq import TdxHq_API
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import time
import os
import threading
import json
import argparse
from typing import Optional
from function.stock_concepts import get_stock_concepts
import requests
from functools import wraps
import random
from depend.config import config
from depend.di_container import container
from depend.services import CompositeDataFetcher
from depend.backup_manager import backup_manager
from depend.monitoring import monitoring_manager
from utils.logging_utils import StructuredLogger, performance_monitor, log_performance

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_with_backoff(max_retries=None, base_delay=None, max_delay=None, backoff_factor=None):
    """
    重试装饰器，带指数退避
    """
    # Use config values if not provided
    max_retries = max_retries or config.MAX_RETRIES
    base_delay = base_delay or config.BASE_DELAY
    max_delay = max_delay or config.MAX_DELAY
    backoff_factor = backoff_factor or config.BACKOFF_FACTOR

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"函数 {func.__name__} 在重试 {max_retries} 次后仍然失败: {str(e)}")
                        raise e
                    # 指数退避延迟
                    delay = min(base_delay * (backoff_factor ** retries) + random.uniform(0, 1), max_delay)
                    logger.warning(f"函数 {func.__name__} 执行失败，第 {retries} 次重试，{delay:.2f}秒后重试: {str(e)}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# Constants
DEFAULT_START_DATE = config.DEFAULT_START_DATE
DEFAULT_OUTPUT_FILE = config.DEFAULT_OUTPUT_FILE


def validate_stock_data(df):
    """
    验证股票数据的完整性与合理性
    使用注入的 DataValidator 服务

    Args:
        df (pd.DataFrame): 股票数据DataFrame

    Returns:
        tuple: (is_valid, validation_report)
    """
    validator = container.get('data_validator')
    return validator.validate(df)


def validate_date_format(date_str):
    """
    验证日期格式是否为 YYYYMMDD

    Args:
        date_str (str): 日期字符串

    Returns:
        bool: 格式正确返回True，否则返回False
    """
    if not date_str or not isinstance(date_str, str):
        return False

    if len(date_str) != 8:
        return False

    try:
        # 尝试解析日期
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])

        # 验证日期是否有效
        datetime.date(year, month, day)
        return True
    except (ValueError, TypeError):
        return False


def get_default_end_date():
    """
    获取默认结束日期，基于当前时间。
    如果市场已收盘（15:00后），使用今天日期。
    如果市场未收盘（15:00前），使用昨天日期。

    Returns:
        str: 格式为 'YYYYMMDD' 的日期字符串
    """
    now = datetime.datetime.now()
    current_time = now.time()

    # Market closes at 15:00
    market_close_time = datetime.time(15, 0, 0)

    if current_time >= market_close_time:
        # Market has closed, use today's date
        target_date = now.date()
    else:
        # Market hasn't closed, use yesterday's date
        target_date = now.date() - datetime.timedelta(days=1)

    # Skip weekends
    while target_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        target_date = target_date - datetime.timedelta(days=1)

    return target_date.strftime('%Y%m%d')

DEFAULT_END_DATE = get_default_end_date()
DEFAULT_OUTPUT_FILE = config.DEFAULT_OUTPUT_FILE
MAX_WORKERS = config.MAX_WORKERS  # Reduced workers to avoid connection spamming

# Thread-local storage for PyTDX connections
thread_local = threading.local()

def get_thread_api():
    """Get or create a thread-local PyTDX API connection."""
    if not hasattr(thread_local, "api"):
        api = TdxHq_API(heartbeat=True)
        # Try connecting
        try:
            primary_server = config.PYTDX_SERVERS[0]
            if api.connect(primary_server[0], primary_server[1], time_out=config.REQUEST_TIMEOUT):
                thread_local.api = api
                return api
            # Fallbacks
            for server in config.PYTDX_SERVERS[1:]:
                if api.connect(server[0], server[1], time_out=config.REQUEST_TIMEOUT):
                    thread_local.api = api
                    return api
        except:
            pass
        thread_local.api = None
    return thread_local.api


def _disconnect_all_tdx_connections():
    """
    断开所有 PyTDX 连接，防止程序因心跳线程而无法退出
    """
    try:
        # 关闭 thread_local 中的 API 连接
        if hasattr(thread_local, 'api') and thread_local.api:
            try:
                thread_local.api.disconnect()
                # 停止心跳线程（设置 heartbeat 暂停标志）
                if hasattr(thread_local.api, 'heartbeat_pause'):
                    thread_local.api.heartbeat_pause = True
                logger.debug("Disconnected thread_local TDX API")
            except:
                pass
            thread_local.api = None

        # 关闭服务中的连接（如果存在）
        data_fetcher = container.get('data_fetcher')
        if hasattr(data_fetcher, 'composite_fetcher') and data_fetcher.composite_fetcher:
            composite = data_fetcher.composite_fetcher
            if hasattr(composite, 'pytdx_fetcher') and composite.pytdx_fetcher:
                if hasattr(composite.pytdx_fetcher, 'api') and composite.pytdx_fetcher.api:
                    try:
                        composite.pytdx_fetcher.api.disconnect()
                        # 停止心跳线程
                        if hasattr(composite.pytdx_fetcher.api, 'heartbeat_pause'):
                            composite.pytdx_fetcher.api.heartbeat_pause = True
                        logger.debug("Disconnected pytdx_fetcher API")
                    except:
                        pass
                    composite.pytdx_fetcher.api = None

        # 终止所有非守护线程（强制退出 pytdx 心跳线程）
        import threading
        for thread in threading.enumerate():
            if hasattr(thread, 'name') and 'pytdx' in thread.name.lower():
                logger.debug(f"Found pytdx thread: {thread.name}, will be terminated on exit")

        logger.info("All TDX connections disconnected")
    except Exception as e:
        logger.warning(f"Error disconnecting TDX connections: {e}")


class DataFetcher:
    def __init__(self, output_file=DEFAULT_OUTPUT_FILE, data_fetcher_service=None, data_storage=None):
        # Use injected service or get from container
        self.data_fetcher_service = data_fetcher_service or container.get('data_fetcher')
        self.data_storage = data_storage or container.get('data_storage')
        self.output_file = output_file


    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=15)
    def get_stock_list(self):
        """
        获取所有A股股票列表

        Returns:
            list: 包含股票信息的字典列表，每个字典包含 symbol, code, name 等字段
        """
        return self.data_fetcher_service.get_stock_list()

    def fetch_daily_data_with_date_range(self, code, market, start_date, end_date):
        """
        使用注入的服务获取指定日期范围的日线数据

        Args:
            code (str): 股票代码
            market (int): 市场代码 (0: 深圳, 1: 上海)
            start_date (str): 开始日期，格式 'YYYYMMDD'
            end_date (str): 结束日期，格式 'YYYYMMDD'

        Returns:
            pd.DataFrame: 包含日线数据的DataFrame，如果获取失败则返回None
        """
        return self.data_fetcher_service.fetch_daily_data(code, market, start_date, end_date)

    @retry_with_backoff(max_retries=2, base_delay=1, max_delay=10)
    def fetch_daily_akshare_with_date_range(self, code, symbol, start_date, end_date):
        """
        使用AkShare获取指定日期范围的日线数据（备用方法）

        Args:
            code (str): 股票代码
            symbol (str): 股票代码（带交易所后缀）
            start_date (str): 开始日期，格式 'YYYYMMDD'
            end_date (str): 结束日期，格式 'YYYYMMDD'

        Returns:
            pd.DataFrame: 包含日线数据的DataFrame，如果获取失败则返回空DataFrame
        """
        try:
            # AkShare is HTTP based, thread-safe usually
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty:
                return pd.DataFrame()

            df = df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount'
            })
            df['date'] = df['date'].astype(str).str.replace('-', '')
            return df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        except Exception as e:
            logger.error(f"AkShare fetch failed for {code}: {e}")
            raise  # Re-raise the exception to trigger the retry decorator

    def process_stock_with_date_range(self, stock_info, start_date, end_date):
        """
        处理单个股票在指定日期范围内的数据

        Args:
            stock_info (dict): 包含股票信息的字典，包含 symbol, code, name 等字段
            start_date (str): 开始日期，格式 'YYYYMMDD'
            end_date (str): 结束日期，格式 'YYYYMMDD'

        Returns:
            pd.DataFrame: 包含股票日线数据的DataFrame，如果获取失败则返回None
        """
        symbol = stock_info['symbol']
        code = stock_info['code']
        name = stock_info['name']

        market = 1 if symbol.endswith('.SH') else 0

        # Use the composite fetcher which handles both PyTDX and AkShare
        df = self.fetch_daily_data_with_date_range(code, market, start_date, end_date)

        if df is not None and not df.empty:
            df['symbol'] = symbol
            df['name'] = name
            return df

        return None

    def get_existing_data_date_range(self):
        """Get the date range of existing data in the output file."""
        if not os.path.exists(self.output_file):
            return None, None

        try:
            df = pd.read_parquet(self.output_file)
            if df.empty:
                return None, None

            # Convert date column to string format if it's not already
            df['date'] = df['date'].astype(str)
            min_date = df['date'].min()
            max_date = df['date'].max()

            # Ensure dates are in YYYYMMDD format
            min_date = min_date.replace('-', '').replace('/', '').replace('.', '')
            max_date = max_date.replace('-', '').replace('/', '').replace('.', '')

            return min_date, max_date
        except Exception as e:
            logger.error(f"Error reading existing data: {e}")
            return None, None

    def get_next_trading_day(self, date_str):
        """Simple function to get next trading day - accounts for weekends."""
        # This is a simplified version - in production, should check against actual trading calendar
        # For now, increment the date and skip weekends
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])

        import datetime
        current_date = datetime.date(year, month, day)

        # Keep incrementing until we find a weekday (Monday=0, Sunday=6)
        next_date = current_date + datetime.timedelta(days=1)
        while next_date.weekday() > 4:  # 5=Saturday, 6=Sunday
            next_date = next_date + datetime.timedelta(days=1)

        return next_date.strftime('%Y%m%d')

    def run(self, start_date=None, end_date=None, incremental=True):
        """
        Run the data fetcher.

        Args:
            start_date (str): Start date in format 'YYYYMMDD'. If None, uses DEFAULT_START_DATE
            end_date (str): End date in format 'YYYYMMDD'. If None, uses DEFAULT_END_DATE
            incremental (bool): Whether to perform incremental update or full refresh
        """
        self.start_date = start_date or DEFAULT_START_DATE
        self.end_date = end_date or DEFAULT_END_DATE

        # If incremental update is enabled and file exists, determine the date range to fetch
        if incremental and os.path.exists(self.output_file):
            logger.info("Incremental mode enabled. Checking existing data...")
            existing_min_date, existing_max_date = self.get_existing_data_date_range()

            if existing_min_date is not None and existing_max_date is not None:
                logger.info(f"Existing data range: {existing_min_date} to {existing_max_date}")

                # If the requested end date is not later than existing max date, no need to fetch
                if self.end_date <= existing_max_date:
                    logger.info(f"Requested end date {self.end_date} is not later than existing max date {existing_max_date}. No new data to fetch.")
                    return

                # Fetch only data from the day after existing max date to requested end date
                fetch_start_date = self.get_next_trading_day(existing_max_date)
                if fetch_start_date > self.end_date:
                    logger.info(f"No new trading days between {existing_max_date} and {self.end_date}")
                    return

                logger.info(f"Fetching incremental data from {fetch_start_date} to {self.end_date}")
                # Update the date range for fetching
                self.start_date = fetch_start_date
            else:
                logger.info("No existing data found or error reading existing data. Performing full fetch.")
        else:
            logger.info(f"Full refresh mode. Fetching data from {self.start_date} to {self.end_date}")

        stocks = self.get_stock_list()
        if not stocks:
            logger.error("No stocks found.")
            return

        all_data = []
        logger.info(f"Starting fetch for {len(stocks)} stocks with {MAX_WORKERS} workers...")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_stock = {executor.submit(self.process_stock_with_date_range, stock_info, self.start_date, self.end_date): stock_info for stock_info in stocks}

            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res is not None:
                    all_data.append(res)

                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(stocks)} stocks...")

        # 断开所有 PyTDX 连接，防止程序不退出
        _disconnect_all_tdx_connections()

        if all_data:
            new_data_df = pd.concat(all_data, ignore_index=True)
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            for col in numeric_cols:
                new_data_df[col] = pd.to_numeric(new_data_df[col], errors='coerce')

            # Validate the new data before processing
            is_valid, validation_report = validate_stock_data(new_data_df)
            if not is_valid:
                logger.warning(f"新获取的数据验证失败: {validation_report}")
                # Optionally, we could filter out invalid data or stop processing
                # For now, we'll continue but log the issues
            else:
                logger.info(f"新获取的 {len(new_data_df)} 条数据验证通过")

            # If incremental mode and file exists, combine with existing data
            if incremental and os.path.exists(self.output_file):
                logger.info("Loading existing data to combine with new data...")
                try:
                    existing_df = pd.read_parquet(self.output_file)
                    # Validate existing data
                    is_valid_existing, validation_report_existing = validate_stock_data(existing_df)
                    if not is_valid_existing:
                        logger.warning(f"现有数据验证失败: {validation_report_existing}")

                    # Combine existing and new data
                    combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
                    # Remove duplicates based on date and symbol
                    combined_df = combined_df.drop_duplicates(subset=['date', 'symbol'], keep='last')
                    # Sort by date and symbol for consistency
                    combined_df = combined_df.sort_values(['symbol', 'date']).reset_index(drop=True)

                    # Validate combined data
                    is_valid_combined, validation_report_combined = validate_stock_data(combined_df)
                    if not is_valid_combined:
                        logger.warning(f"合并后的数据验证失败: {validation_report_combined}")
                    else:
                        logger.info(f"合并后的 {len(combined_df)} 条数据验证通过")

                    final_df = combined_df
                    logger.info(f"Combined {len(existing_df)} existing rows with {len(new_data_df)} new rows, total {len(final_df)} rows after deduplication")
                except Exception as e:
                    logger.error(f"Error combining existing and new data: {e}")
                    final_df = new_data_df
            else:
                final_df = new_data_df

            # Add error handling for saving the parquet file
            try:
                # Save using the storage service which includes backup functionality
                self.data_storage.save(final_df, self.output_file)
                logger.info(f"Saved {len(final_df)} rows to {self.output_file}")
            except Exception as e:
                logger.error(f"Error saving data to {self.output_file}: {e}")
                # Try to save to a backup file
                backup_file = self.output_file.replace('.parquet', '_backup.parquet')
                try:
                    final_df.to_parquet(backup_file, index=False)
                    logger.info(f"Saved data to backup file: {backup_file}")
                except Exception as backup_error:
                    logger.error(f"Error saving to backup file: {backup_error}")
        else:
            logger.info("No new data fetched.")


class Analyzer:
    def __init__(self, input_file: str = DEFAULT_OUTPUT_FILE, output_ladder: str = config.DEFAULT_LADDER_FILE, output_promotion: str = config.DEFAULT_PROMOTION_FILE,
                 data_validator=None, data_storage=None):
        self.input_file = input_file
        self.output_ladder = output_ladder
        self.output_promotion = output_promotion
        self.df = None
        self.concepts_cache = {}
        self.data_validator = data_validator or container.get('data_validator')
        self.data_storage = data_storage or container.get('data_storage')

    @log_performance("analyzer_process", {"component": "analyzer"})

    def load_data(self, chunk_size=None):
        """
        加载数据，支持可选的分块加载以优化内存使用。

        Args:
            chunk_size (int, optional): 每次加载的行数。如果为None，则加载所有数据。

        Returns:
            bool: 加载成功返回True，失败返回False
        """
        if not os.path.exists(self.input_file):
            logger.error(f"Input file {self.input_file} not found.")
            return False

        # 由于pandas的read_parquet不支持chunksize参数，我们直接加载整个文件
        # 如果需要处理大文件，可以考虑其他方案
        self.df = pd.read_parquet(self.input_file)
        return True

    def calculate_limit_price(self, row, prev_close):
        if pd.isna(prev_close):
            return np.nan

        # Determine multiplier
        if 'ST' in row['name']:
            multiplier = 1.05
        elif row['symbol'].startswith(('30', '68')): # ChiNext, STAR
            multiplier = 1.20
        else: # Main board
            multiplier = 1.10

        # Calculate limit price (Standard A-share rounding: round to 2 decimals)
        # Note: Python round() rounds to nearest even number for .5 cases sometimes.
        # Financial rounding usually requires standard rounding.
        # But simple round(x, 2) is often close enough for screening.
        # Strict formula: int(prev_close * (1 + ratio) * 100 + 0.5) / 100
        # Let's use the strict formula to be safe.

        limit = int(prev_close * multiplier * 100 + 0.49999) / 100.0
        return limit

    def identify_limit_ups(self):
        logger.info("identifying limit ups...")
        self.df.sort_values(by=['symbol', 'date'], inplace=True)

        # Vectorized prev_close
        self.df['prev_close'] = self.df.groupby('symbol')['close'].shift(1)

        # We need to apply logic row by row or vectorized?
        # Vectorized is hard due to "ST" name check and code check mix.
        # But we can split df into groups.

        # 1. ST stocks
        st_mask = self.df['name'].str.contains('ST', na=False)
        # 2. 20% stocks (30/68)
        twenty_pct_mask = self.df['symbol'].str.startswith(('30', '68'))
        # 3. 10% stocks (Rest)
        ten_pct_mask = (~st_mask) & (~twenty_pct_mask)

        # Calculate Limit Prices
        # Function to calc limit: int(prev * (1+r) * 100 + 0.5) / 100
        # We can use lambda

        def calc_limit_vec(prev_closes, ratio):
            return (np.floor(prev_closes * (1 + ratio) * 100 + 0.5) / 100)

        self.df['limit_price'] = np.nan

        # Apply 10%
        if ten_pct_mask.any():
            self.df.loc[ten_pct_mask, 'limit_price'] = calc_limit_vec(self.df.loc[ten_pct_mask, 'prev_close'], 0.10)

        # Apply 20%
        if twenty_pct_mask.any():
            self.df.loc[twenty_pct_mask, 'limit_price'] = calc_limit_vec(self.df.loc[twenty_pct_mask, 'prev_close'], 0.20)

        # Apply 5% (ST) - Note: ST overrides 20%? Usually ST on ChiNext is 20%?
        # Actually ChiNext ST is 20%. Main board ST is 5%.
        # Refinement: ST on 30xx/68xx is 20%. ST on Main is 5%.
        # So exclude 20% from ST mask for 5% rule.
        st_main_mask = st_mask & (~twenty_pct_mask)
        if st_main_mask.any():
            self.df.loc[st_main_mask, 'limit_price'] = calc_limit_vec(self.df.loc[st_main_mask, 'prev_close'], 0.05)

        # Compare
        # Allow small float error margin? Usually strict equal for limit up price,
        # but user said ">= ... - 1e-4".
        self.df['is_limit_up'] = self.df['close'] >= (self.df['limit_price'] - 0.001)

        # Filter only necessary columns for processing to save memory if needed
        # But we need full data.

    def calculate_consecutive_days(self):
        logger.info("Calculating consecutive days...")
        # Logic:
        # If is_limit_up is False, count = 0
        # If True, count = prev_count + 1

        # This is hard to vectorize fully across groups without loop, but we can use cumsum approach.
        # Group by symbol.

        # A common trick:
        # group = (is_limit_up != is_limit_up.shift()).cumsum()
        # count = groupby(group).cumcount() + 1
        # mask where is_limit_up is False => 0

        self.df['group'] = (self.df['is_limit_up'] != self.df.groupby('symbol')['is_limit_up'].shift(1)).cumsum()
        self.df['consecutive_limit_up_days'] = self.df.groupby(['symbol', 'group']).cumcount() + 1
        self.df.loc[~self.df['is_limit_up'], 'consecutive_limit_up_days'] = 0

    def calculate_next_day_open_change(self):
        logger.info("Calculating next day opening auction price change...")
        # Sort by symbol and date to ensure proper chronological order
        self.df.sort_values(by=['symbol', 'date'], inplace=True)

        # Calculate the next day's opening price change
        # Group by symbol and shift the 'open' price to align with current day's data
        self.df['next_day_open'] = self.df.groupby('symbol')['open'].shift(-1)
        self.df['next_day_open_change_pct'] = (
            (self.df['next_day_open'] - self.df['close']) / self.df['close'] * 100
        ).round(2)

        # Fill NaN values with 0 for the last available date for each stock
        self.df['next_day_open_change_pct'].fillna(0, inplace=True)

    def identify_board_type(self):
        logger.info("Identifying board types...")
        # Default empty
        self.df['board_type'] = None

        limit_ups = self.df[self.df['is_limit_up']].copy()

        # One-word: high == low
        one_word_mask = limit_ups['high'] == limit_ups['low']
        limit_ups.loc[one_word_mask, 'board_type'] = '一字板'

        # T-shaped: open == high == close and low < close
        t_shape_mask = (limit_ups['open'] == limit_ups['high']) & \
                       (limit_ups['high'] == limit_ups['close']) & \
                       (limit_ups['low'] < limit_ups['close'])
        limit_ups.loc[t_shape_mask, 'board_type'] = 'T字板'

        # Turnover: The rest
        turnover_mask = limit_ups['board_type'].isna()
        limit_ups.loc[turnover_mask, 'board_type'] = '换手板'

        self.df.update(limit_ups)

    def validate_processed_data(self):
        """
        验证处理后的数据
        """
        validation_report = []

        # 检查涨停识别的合理性
        limit_up_stocks = self.df[self.df['is_limit_up'] == True]
        if len(limit_up_stocks) > 0:
            # 检查涨停价格计算是否合理
            invalid_limit_prices = limit_up_stocks[
                (limit_up_stocks['close'] < limit_up_stocks['limit_price'] - 0.01) |
                (limit_up_stocks['close'] > limit_up_stocks['limit_price'] + 0.01)
            ]
            if len(invalid_limit_prices) > 0:
                validation_report.append(f"涨停价格计算异常: {len(invalid_limit_prices)} 条记录")

        # 检查连续涨停天数的合理性
        consecutive_limit_ups = self.df[self.df['consecutive_limit_up_days'] > 0]
        if len(consecutive_limit_ups) > 0:
            max_consecutive = consecutive_limit_ups['consecutive_limit_up_days'].max()
            if max_consecutive > 20:  # 一般连续涨停不会超过20天
                validation_report.append(f"连续涨停天数异常: 最大值为 {max_consecutive} 天")

        return len(validation_report) == 0, validation_report

    @log_performance("analyzer_process", {"component": "analyzer"})
    def process(self, chunk_size=None):
        """
        执行完整的数据分析流程，包括涨停识别、连续涨停天数计算、板块类型识别等

        Args:
            chunk_size (int, optional): 数据分块大小，用于内存优化
        """
        # Log operation details
        logger.info("Starting data analysis process", {
            "input_file": self.input_file,
            "output_ladder": self.output_ladder,
            "output_promotion": self.output_promotion,
            "chunk_size": chunk_size
        })

        if not self.load_data(chunk_size=chunk_size):
            return

        # Time each analysis step
        timer_id = performance_monitor.start_timer("identify_limit_ups")
        self.identify_limit_ups()
        duration = performance_monitor.end_timer(timer_id)
        logger.info("Completed limit up identification", {
            "duration_seconds": round(duration, 4)
        })

        timer_id = performance_monitor.start_timer("calculate_consecutive_days")
        self.calculate_consecutive_days()
        duration = performance_monitor.end_timer(timer_id)
        logger.info("Completed consecutive days calculation", {
            "duration_seconds": round(duration, 4)
        })

        timer_id = performance_monitor.start_timer("calculate_next_day_open_change")
        self.calculate_next_day_open_change()
        duration = performance_monitor.end_timer(timer_id)
        logger.info("Completed next day opening auction price change calculation", {
            "duration_seconds": round(duration, 4)
        })

        timer_id = performance_monitor.start_timer("identify_board_type")
        self.identify_board_type()
        duration = performance_monitor.end_timer(timer_id)
        logger.info("Completed board type identification", {
            "duration_seconds": round(duration, 4)
        })

        # Filter for Ladder: consecutive >= 1 (include single-day limit-ups)
        timer_id = performance_monitor.start_timer("filter_ladder_data")
        ladder_df = self.df[self.df['consecutive_limit_up_days'] >= 1].copy()
        duration = performance_monitor.end_timer(timer_id)
        logger.info(f"Filtered ladder data: {len(ladder_df)} records", {
            "duration_seconds": round(duration, 4)
        })

        # Fetch concepts only for these ladder stocks
        unique_symbols = ladder_df['symbol'].unique()
        logger.info(f"Fetching concepts for {len(unique_symbols)} stocks in ladder...")

        # Fetch concepts for each stock
        def fetch_concept_for_stock(symbol):
            result = get_stock_concepts(symbol)
            if result and 'concepts' in result:
                return symbol, result['concepts']
            return symbol, []

        # Use ThreadPoolExecutor to fetch concepts in parallel
        timer_id = performance_monitor.start_timer("fetch_concepts_parallel")
        concept_map = {}
        with ThreadPoolExecutor(max_workers=config.CONCEPT_FETCH_WORKERS) as executor:
            futures = {executor.submit(fetch_concept_for_stock, symbol): symbol for symbol in unique_symbols}
            for i, future in enumerate(as_completed(futures)):
                symbol, concepts = future.result()
                concept_map[symbol] = concepts
                if (i + 1) % 100 == 0:
                    logger.info(f"Fetched concepts for {i + 1}/{len(unique_symbols)} stocks...")

        duration = performance_monitor.end_timer(timer_id)
        logger.info(f"Completed fetching concepts for {len(concept_map)} stocks", {
            "duration_seconds": round(duration, 4),
            "stocks_count": len(concept_map)
        })

        # Map concepts back to ladder_df
        timer_id = performance_monitor.start_timer("map_concepts_to_dataframe")
        ladder_df['concept_themes'] = ladder_df['symbol'].map(concept_map)
        duration = performance_monitor.end_timer(timer_id)
        logger.info("Completed mapping concepts to dataframe", {
            "duration_seconds": round(duration, 4)
        })

        # Save Ladder using injected storage service
        timer_id = performance_monitor.start_timer("save_ladder_data")
        self.data_storage.save(ladder_df, self.output_ladder)
        duration = performance_monitor.end_timer(timer_id)
        logger.info(f"Saved ladder data to {self.output_ladder}", {
            "duration_seconds": round(duration, 4),
            "records_count": len(ladder_df)
        })

        # Validate processed data
        is_valid, validation_report = self.validate_processed_data()
        if not is_valid:
            logger.warning(f"数据验证发现问题: {validation_report}")
        else:
            logger.info("数据验证通过")

        # Calculate Promotion Rates
        # Group by date + consecutive_days
        # For each date D, count(N board).
        # Check date D+1, how many of those stocks became N+1 board.
        timer_id = performance_monitor.start_timer("calculate_promotion_rates")

        stats = []
        dates = sorted(self.df['date'].unique())

        for i in range(len(dates) - 1):
            curr_date = dates[i]
            next_date = dates[i+1]

            # Get stocks with N boards today
            curr_df = self.df[self.df['date'] == curr_date]
            next_df = self.df[self.df['date'] == next_date]

            # Max consecutive days today
            max_boards = curr_df['consecutive_limit_up_days'].max()
            if pd.isna(max_boards) or max_boards == 0:
                continue

            for n in range(1, int(max_boards) + 1):
                # Stocks that were N board today
                n_board_stocks = curr_df[curr_df['consecutive_limit_up_days'] == n]['symbol'].tolist()
                total_n = len(n_board_stocks)

                if total_n == 0:
                    continue

                # Check their status tomorrow
                # They must have consecutive_limit_up_days == n + 1
                promoted_stocks = next_df[
                    (next_df['symbol'].isin(n_board_stocks)) &
                    (next_df['consecutive_limit_up_days'] == n + 1)
                ]
                promoted_count = len(promoted_stocks)

                rate = promoted_count / total_n if total_n > 0 else 0

                stats.append({
                    'date': curr_date,
                    'board_level': n,
                    'total': total_n,
                    'promoted': promoted_count,
                    'promotion_rate': rate
                })

        # Convert stats list to DataFrame and save promotion rates using injected storage service
        stats_df = pd.DataFrame(stats)
        duration = performance_monitor.end_timer(timer_id)
        logger.info(f"Completed promotion rates calculation", {
            "duration_seconds": round(duration, 4),
            "stats_records_count": len(stats_df)
        })

        timer_id = performance_monitor.start_timer("save_promotion_data")
        self.data_storage.save(stats_df, self.output_promotion)
        duration = performance_monitor.end_timer(timer_id)
        logger.info(f"Saved promotion rates to {self.output_promotion}", {
            "duration_seconds": round(duration, 4),
            "records_count": len(stats_df)
        })


@log_performance("generate_ladder_data_for_html", {"component": "visualizer"})
def generate_ladder_data_for_html(ladder_file: str = config.DEFAULT_LADDER_FILE, output_file: str = config.DEFAULT_LADDER_JS_FILE, chunk_size: int = config.CHUNK_SIZE):
    """Generate ladder data for HTML visualization."""
    logger.info("Starting ladder data generation", {
        "ladder_file": ladder_file,
        "output_file": output_file,
        "chunk_size": chunk_size
    })

    # Load the ladder data
    if not os.path.exists(ladder_file):
        logger.error(f"Ladder file not found: {ladder_file}")
        return

    # 由于pandas的read_parquet不支持chunksize参数，我们直接加载整个文件
    timer_id = performance_monitor.start_timer("load_ladder_data")
    df = pd.read_parquet(ladder_file)
    load_duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Loaded ladder data: {len(df)} records", {
        "duration_seconds": round(load_duration, 4),
        "columns_count": len(df.columns)
    })

    # Get all unique dates
    timer_id = performance_monitor.start_timer("get_unique_dates")
    unique_dates = sorted(df['date'].unique(), reverse=True)
    duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Found {len(unique_dates)} unique dates", {
        "duration_seconds": round(duration, 4)
    })

    # Create a dictionary to store data for each date
    timer_id = performance_monitor.start_timer("process_ladder_data")

    # Sort dataframe by date and symbol to prepare for next day calculation
    df_sorted = df.sort_values(['symbol', 'date']).reset_index(drop=True)

    # Calculate next day opening change percentage (vectorized - ~10x faster)
    # Get next day's open price using groupby shift
    df_sorted['next_day_open'] = df_sorted.groupby('symbol')['open'].shift(-1)

    # Calculate percentage change from day's close to next day's open (vectorized)
    df_sorted['next_day_open_change_pct'] = (
        ((df_sorted['next_day_open'] - df_sorted['close']) / df_sorted['close'].replace(0, np.nan)) * 100
    ).round(2)

    # Handle NaN cases (last trading day for each stock, zero close, etc.)
    df_sorted['next_day_open_change_pct'] = df_sorted['next_day_open_change_pct'].fillna(0.0)

    # Remove temporary column
    df_with_next_change = df_sorted.drop(columns=['next_day_open'])

    all_dates_data = {}

    for i, date in enumerate(unique_dates):
        # Filter data for this date
        date_data = df_with_next_change[df_with_next_change['date'] == date].copy()

        # Group by consecutive limit up days
        grouped = date_data.groupby('consecutive_limit_up_days').apply(
            lambda x: x[['symbol', 'name', 'close', 'consecutive_limit_up_days', 'concept_themes', 'next_day_open_change_pct']].to_dict('records')
        ).to_dict()

        # Convert to the format expected by the HTML
        formatted_data = {}
        for level, stocks in grouped.items():
            formatted_data[level] = []
            for stock in stocks:
                # Convert concept_themes to list if it's a numpy array
                concepts = stock['concept_themes']
                if hasattr(concepts, 'tolist'):
                    concepts = concepts.tolist()
                elif not isinstance(concepts, list):
                    concepts = list(concepts) if concepts else []

                formatted_data[level].append({
                    'code': stock['symbol'],
                    'name': stock['name'],
                    'price': stock['close'],
                    'limitUpDays': stock['consecutive_limit_up_days'],
                    'conceptThemes': concepts,
                    'nextDayOpenChangePct': stock.get('next_day_open_change_pct', 0.0)
                })

        # 将日期格式化为 YYYYMMDD 格式，与前端期望的格式一致
        date_str = pd.to_datetime(date).strftime('%Y%m%d')
        all_dates_data[date_str] = formatted_data

        # Log progress every 10 dates
        if (i + 1) % 10 == 0:
            logger.info(f"Processed {i + 1}/{len(unique_dates)} dates...")

    process_duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Completed processing ladder data", {
        "duration_seconds": round(process_duration, 4),
        "dates_processed": len(all_dates_data)
    })

    # Save as JS file that can be loaded by the HTML
    timer_id = performance_monitor.start_timer("save_ladder_js_file")
    js_content = f"// 自动生成的连板数据文件\nwindow.LADDER_DATA = {json.dumps(all_dates_data, ensure_ascii=False)};"

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(js_content)

    save_duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Saved ladder data to {output_file}", {
        "duration_seconds": round(save_duration, 4),
        "dates_count": len(all_dates_data)
    })

    logger.info(f"Generated ladder data for {len(all_dates_data)} dates to {output_file}")
    logger.info(f"Latest date in data: {unique_dates[0] if unique_dates else 'None'}")

    # Also create a simple summary
    latest_date = unique_dates[0] if unique_dates else None
    if latest_date:
        latest_data = df[df['date'] == latest_date]
        logger.info(f"Latest date ({latest_date}) has {len(latest_data)} limit-up stocks")
        level_counts = latest_data['consecutive_limit_up_days'].value_counts().sort_index(ascending=False)
        logger.info("Distribution by consecutive days:")
        for level, count in level_counts.items():
            logger.info(f"  {level}连板: {count}只")




def generate_kline_data(input_file: str = config.DEFAULT_OUTPUT_FILE, output_file: str = config.DEFAULT_KLINE_JS_FILE, chunk_size: int = config.CHUNK_SIZE, ladder_only: bool = True):
    """Generate K-line JS data file for HTML visualization.

    Args:
        ladder_only: If True, only generate K-line data for limit-up stocks in ladder_data.js
    """
    logger.info("Starting K-line data generation", {
        "input_file": input_file,
        "output_file": output_file,
        "chunk_size": chunk_size,
        "ladder_only": ladder_only
    })

    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    # 获取连板股票代码列表
    limit_up_symbols = set()
    if ladder_only:
        ladder_file = os.path.join(os.path.dirname(output_file) if output_file else 'data', 'ladder_data.js')
        if os.path.exists(ladder_file):
            import re
            with open(ladder_file, 'r', encoding='utf-8') as f:
                content = f.read()
                limit_up_symbols = set(re.findall(r'"code":\s*"([^"]+)"', content))
            logger.info(f"Loaded {len(limit_up_symbols)} limit-up stocks from ladder_data.js")
        else:
            logger.warning(f"Ladder file not found: {ladder_file}, generating for all stocks")

    # 加载数据
    timer_id = performance_monitor.start_timer("load_kline_data")
    df = pd.read_parquet(input_file)
    load_duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Loaded K-line data: {len(df)} records", {
        "duration_seconds": round(load_duration, 4),
        "columns_count": len(df.columns)
    })

    # 如果只生成连板股票K线，则先筛选
    if limit_up_symbols:
        df = df[df['symbol'].isin(limit_up_symbols)].copy()
        logger.info(f"Filtered to {len(df)} records for {len(limit_up_symbols)} limit-up stocks")

    timer_id = performance_monitor.start_timer("process_kline_data")
    df_sorted = df.sort_values(['symbol', 'date'])
    df_sorted['date_formatted'] = pd.to_datetime(df_sorted['date'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
    grouped = df_sorted.groupby('symbol')
    kline_data = {}
    total_symbols = len(grouped)

    for i, (symbol, group) in enumerate(grouped):
        kline_data[symbol] = {
            'name': group['name'].iloc[0] if 'name' in group.columns else '',
            'dates': group['date_formatted'].tolist(),
            'values': group[['open', 'close', 'low', 'high']].values.tolist(),
            'volumes': group['volume'].tolist()
        }

        # Log progress every 100 symbols
        if (i + 1) % 100 == 0:
            logger.info(f"Processed {i + 1}/{total_symbols} symbols...")

    process_duration = performance_monitor.end_timer(timer_id)
    logger.info(f"Completed processing K-line data", {
        "duration_seconds": round(process_duration, 4),
        "symbols_count": len(kline_data)
    })

    timer_id = performance_monitor.start_timer("save_kline_js_file")
    js_content = f"// 自动生成的K线数据文件\nwindow.KLINE_DATA_GLOBAL = {json.dumps(kline_data, ensure_ascii=False)};"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(js_content)
    save_duration = performance_monitor.end_timer(timer_id)

    logger.info(f"Saved K-line data to {output_file}", {
        "duration_seconds": round(save_duration, 4),
        "symbols_count": len(kline_data)
    })

    # 生成精简版（只包含最近日期的连板股票）
    generate_kline_data_core(input_file)


def generate_kline_data_core(input_file: str = config.DEFAULT_OUTPUT_FILE):
    """Generate core K-line JS data file（只包含最近日期的连板股票）"""
    logger.info("Starting core K-line data generation...")

    # 获取 ladder_data.js 并解析最近日期的股票
    ladder_file = os.path.join('data', 'ladder_data.js')
    if not os.path.exists(ladder_file):
        logger.warning(f"Ladder file not found: {ladder_file}")
        return

    with open(ladder_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # 解析 JSON
    json_start = content.find('{')
    json_end = content.rfind('}') + 1
    ladder_data = json.loads(content[json_start:json_end])

    # 获取最后一个日期
    dates = sorted(ladder_data.keys())
    last_date = dates[-1]

    # 获取最近日期的股票代码
    recent_stock_codes = set()
    for board in ladder_data[last_date].values():
        for stock in board:
            recent_stock_codes.add(stock['code'])

    logger.info(f"Core K-line data for date {last_date}: {len(recent_stock_codes)} stocks")

    # 读取 parquet 并筛选
    df = pd.read_parquet(input_file)
    df_recent = df[df['symbol'].isin(recent_stock_codes)].copy()
    df_recent = df_recent.sort_values(['symbol', 'date'])
    df_recent['date_formatted'] = pd.to_datetime(df_recent['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    # 生成精简版 K 线数据
    kline_data = {}
    grouped = df_recent.groupby('symbol')

    for symbol, group in grouped:
        kline_data[symbol] = {
            'name': group['name'].iloc[0],
            'dates': group['date_formatted'].tolist(),
            'values': group[['open', 'close', 'low', 'high']].values.tolist(),
            'volumes': group['volume'].tolist()
        }

    # 保存精简版（多行格式）
    core_output_file = os.path.join('data', 'kline_data_core.js')
    js_content = f"// 精简版K线数据（只含最近日期连板股票）\nwindow.KLINE_DATA_CORE = {json.dumps(kline_data, ensure_ascii=False)};"
    with open(core_output_file, 'w', encoding='utf-8') as f:
        f.write(js_content)

    logger.info(f"Saved core K-line data to {core_output_file}")
    logger.info(f"Core symbols count: {len(kline_data)}")


def main():
    parser = argparse.ArgumentParser(description='A股连板分析工具 - 统一入口')
    parser.add_argument('command', choices=['fetch', 'analyze', 'generate-ladder', 'ladder', 'l', 'generate-kline', 'kline', 'k', 'full'],
                        help='执行的命令: fetch(获取数据), analyze(分析数据), generate-ladder/ladder/l(生成阶梯数据), generate-kline/kline/k(生成K线数据JS), full(完整流程)')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYYMMDD')
    parser.add_argument('--input-file', type=str, default=config.DEFAULT_OUTPUT_FILE, help='输入文件')
    parser.add_argument('--output-file', type=str, help='输出文件名')
    parser.add_argument('--incremental', action='store_true', default=True, help='是否增量更新')
    parser.add_argument('--full-refresh', action='store_true', help='完全刷新模式')
    parser.add_argument('--chunk-size', type=int, default=config.CHUNK_SIZE, help='数据处理块大小，用于内存优化')
    parser.add_argument('--all-stocks', action='store_true', help='生成全部股票K线数据（默认只生成连板股票）')

    args = parser.parse_args()

    # Validate date formats if provided
    if args.start_date and not validate_date_format(args.start_date):
        logger.error(f"Invalid start date format: {args.start_date}. Expected format: YYYYMMDD")
        return

    if args.end_date and not validate_date_format(args.end_date):
        logger.error(f"Invalid end date format: {args.end_date}. Expected format: YYYYMMDD")
        return

    # Determine output file name
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = config.DEFAULT_OUTPUT_FILE  # Use the default file name 'stock_daily_latest.parquet'

    if args.command == 'fetch':
        fetcher = DataFetcher(output_file=output_file, data_storage=container.get('data_storage'))
        fetcher.run(
            start_date=args.start_date,
            end_date=args.end_date,
            incremental=not args.full_refresh
        )
        _disconnect_all_tdx_connections()
    elif args.command == 'analyze':
        analyzer = Analyzer(input_file=args.input_file)
        analyzer.process(chunk_size=args.chunk_size)
    elif args.command in ['generate-ladder', 'ladder', 'l']:
        generate_ladder_data_for_html(chunk_size=args.chunk_size)
    elif args.command in ['generate-kline', 'kline', 'k']:
        out_js = args.output_file if args.output_file else config.DEFAULT_KLINE_JS_FILE
        generate_kline_data(input_file=args.input_file, output_file=out_js, chunk_size=args.chunk_size, ladder_only=not args.all_stocks)
    elif args.command == 'full':
        # 执行完整流程
        logger.info("开始执行完整分析流程...")

        # 1. 获取数据
        fetcher = DataFetcher(output_file=output_file, data_storage=container.get('data_storage'))
        fetcher.run(
            start_date=args.start_date,
            end_date=args.end_date,
            incremental=not args.full_refresh
        )

        # 2. 分析数据
        analyzer = Analyzer(input_file=output_file)
        analyzer.process(chunk_size=args.chunk_size)

        # 3. 生成阶梯数据
        generate_ladder_data_for_html(chunk_size=args.chunk_size)

        # 4. 生成K线数据（默认只生成连板股票）
        generate_kline_data(input_file=output_file, output_file=config.DEFAULT_KLINE_JS_FILE,
                            chunk_size=args.chunk_size, ladder_only=not args.all_stocks)

        logger.info("完整分析流程完成！")

        # 断开所有连接（已经在内层调用，这里再次确保）
        _disconnect_all_tdx_connections()


def save_monitoring_metrics():
    """保存监控指标"""
    try:
        monitoring_manager.save_metrics()
        logger.info("已保存监控指标")
    except Exception as e:
        logger.error(f"保存监控指标失败: {e}")


if __name__ == "__main__":
    try:
        main()
    finally:
        # 确保程序退出前断开所有 TDX 连接
        _disconnect_all_tdx_connections()
        # 保存监控指标
        save_monitoring_metrics()