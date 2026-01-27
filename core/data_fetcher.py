"""
数据获取模块
从 main.py 中提取的 DataFetcher 类
"""
import os
import logging
import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any

from depend.config import config
from depend.di_container import container
from utils.logging_utils import performance_monitor

logger = logging.getLogger(__name__)

# ==================== 常量定义 ====================
# 连接配置
DEFAULT_REQUEST_TIMEOUT = 10
DEFAULT_MAX_RETRIES = 3

# 数据处理配置
DEFAULT_CHUNK_SIZE = 50000

# 日期格式
DATE_FORMAT = '%Y%m%d'

# 市场代码
MARKET_SZ = 0  # 深圳
MARKET_SH = 1  # 上海

# 交易所后缀
EXCHANGE_SZ = '.SZ'
EXCHANGE_SH = '.SH'


def validate_date_format(date_str: str) -> bool:
    """
    验证日期格式是否为 YYYYMMDD

    Args:
        date_str: 日期字符串

    Returns:
        格式正确返回True，否则返回False
    """
    if not date_str or not isinstance(date_str, str):
        return False

    if len(date_str) != 8:
        return False

    try:
        year = int(date_str[0:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        datetime.date(year, month, day)
        return True
    except (ValueError, TypeError):
        return False


def get_default_end_date() -> str:
    """
    获取默认结束日期，基于当前时间。
    如果市场已收盘（15:00后），使用今天日期。
    如果市场未收盘（15:00前），使用昨天日期。

    Returns:
        格式为 'YYYYMMDD' 的日期字符串
    """
    now = datetime.datetime.now()
    current_time = now.time()

    # Market closes at 15:00
    market_close_time = datetime.time(15, 0, 0)

    if current_time >= market_close_time:
        target_date = now.date()
    else:
        target_date = now.date() - datetime.timedelta(days=1)

    # Skip weekends
    while target_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
        target_date = target_date - datetime.timedelta(days=1)

    return target_date.strftime(DATE_FORMAT)


def detect_market_from_symbol(symbol: str) -> int:
    """
    根据股票代码检测市场

    Args:
        symbol: 股票代码（如 000001.SZ）

    Returns:
        市场代码 0=深圳, 1=上海
    """
    return MARKET_SH if symbol.endswith(EXCHANGE_SH) else MARKET_SZ


def normalize_stock_code(code: str, symbol: str) -> str:
    """
    标准化股票代码

    Args:
        code: 股票代码（如 000001）
        symbol: 股票代码（带交易所后缀，如 000001.SZ）

    Returns:
        标准化的股票代码
    """
    return symbol if '.' in symbol else code


def get_next_trading_day(date_str: str) -> str:
    """
    获取下一个交易日（简单的跳过周末逻辑）

    Args:
        date_str: 格式为 'YYYYMMDD' 的日期字符串

    Returns:
        下一个交易日，格式为 'YYYYMMDD'
    """
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    day = int(date_str[6:8])

    current_date = datetime.date(year, month, day)

    # 增加日期直到找到工作日
    next_date = current_date + datetime.timedelta(days=1)
    while next_date.weekday() > 4:  # 5=Saturday, 6=Sunday
        next_date = next_date + datetime.timedelta(days=1)

    return next_date.strftime(DATE_FORMAT)


class DataFetcher:
    """股票数据获取器"""

    def __init__(
        self,
        output_file: str = None,
        data_fetcher_service=None,
        data_storage=None
    ):
        self.data_fetcher_service = data_fetcher_service or container.get('data_fetcher')
        self.data_storage = data_storage or container.get('data_storage')
        self.output_file = output_file or config.DEFAULT_OUTPUT_FILE

    def get_stock_list_fallback_from_existing_data(self) -> List[Dict[str, Any]]:
        """
        从现有数据中提取股票列表（当数据源失败时的备选方案）

        Returns:
            包含股票信息的字典列表
        """
        try:
            if os.path.exists(self.output_file):
                df = pd.read_parquet(self.output_file)
                symbols = df['symbol'].unique().tolist()
                stock_list = [
                    {'symbol': s, 'code': s.split('.')[0], 'name': ''}
                    for s in symbols
                ]
                logger.info(f"Loaded {len(stock_list)} stocks from existing data")
                return stock_list
        except FileNotFoundError as e:
            logger.warning(f"Data file not found: {e}")
        except ValueError as e:
            logger.warning(f"Error reading parquet file: {e}")
        except Exception as e:
            logger.warning(f"Failed to load stock list from existing data: {e}")
        return []

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """
        获取所有A股股票列表

        Returns:
            包含股票信息的字典列表，每个字典包含 symbol, code, name 等字段
        """
        try:
            return self.data_fetcher_service.get_stock_list()
        except ConnectionError as e:
            logger.warning(f"Connection error fetching stock list: {e}")
            return self.get_stock_list_fallback_from_existing_data()
        except TimeoutError as e:
            logger.warning(f"Timeout fetching stock list: {e}")
            return self.get_stock_list_fallback_from_existing_data()
        except Exception as e:
            logger.warning(f"Unexpected error fetching stock list: {e}")
            fallback_list = self.get_stock_list_fallback_from_existing_data()
            if fallback_list:
                return fallback_list
            raise

    def fetch_daily_data_with_date_range(
        self,
        code: str,
        market: int,
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        使用注入的服务获取指定日期范围的日线数据

        Args:
            code: 股票代码
            market: 市场代码 (0: 深圳, 1: 上海)
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'

        Returns:
            包含日线数据的DataFrame，如果获取失败则返回None
        """
        try:
            return self.data_fetcher_service.fetch_daily_data(
                code, market, start_date, end_date
            )
        except (ConnectionError, TimeoutError) as e:
            logger.warning(f"Network error fetching {code}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching daily data for {code}: {e}")
            return None

    def process_stock_with_date_range(
        self,
        stock_info: Dict[str, Any],
        start_date: str,
        end_date: str
    ) -> Optional[pd.DataFrame]:
        """
        处理单个股票在指定日期范围内的数据

        Args:
            stock_info: 包含股票信息的字典，包含 symbol, code, name 等字段
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'

        Returns:
            包含股票日线数据的DataFrame，如果获取失败则返回None
        """
        symbol = stock_info['symbol']
        code = stock_info['code']
        name = stock_info.get('name', '')

        market = detect_market_from_symbol(symbol)

        df = self.fetch_daily_data_with_date_range(code, market, start_date, end_date)

        if df is not None and not df.empty:
            df['symbol'] = symbol
            df['name'] = name
            return df

        return None

    def get_existing_data_date_range(self) -> tuple:
        """
        获取现有数据文件的日期范围

        Returns:
            (min_date, max_date) 元组，如果不存在的返回 (None, None)
        """
        if not os.path.exists(self.output_file):
            return None, None

        try:
            df = pd.read_parquet(self.output_file)
            if df.empty:
                return None, None

            df['date'] = df['date'].astype(str)
            min_date = df['date'].min()
            max_date = df['date'].max()

            # Normalize date format
            min_date = min_date.replace('-', '').replace('/', '').replace('.', '')
            max_date = max_date.replace('-', '').replace('/', '').replace('.', '')

            return min_date, max_date
        except FileNotFoundError:
            logger.warning(f"Data file not found: {self.output_file}")
            return None, None
        except ValueError as e:
            logger.error(f"Error reading parquet file: {e}")
            return None, None
        except Exception as e:
            logger.error(f"Unexpected error reading existing data: {e}")
            return None, None

    def _combine_data_incremental(
        self,
        new_data_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        合并增量数据与现有数据（事务性处理）

        Args:
            new_data_df: 新获取的数据

        Returns:
            合并后的数据

        Raises:
            Exception: 如果合并失败但原有数据已损坏
        """
        logger.info("Loading existing data to combine with new data...")

        try:
            existing_df = pd.read_parquet(self.output_file)

            # 验证现有数据格式
            if existing_df.empty:
                logger.warning("Existing data is empty, using new data only")
                return new_data_df

            if not set(new_data_df.columns).issubset(set(existing_df.columns)):
                raise ValueError("Column mismatch between existing and new data")

            # 统一 date 列的数据类型
            if 'date' in existing_df.columns and 'date' in new_data_df.columns:
                existing_date_type = existing_df['date'].dtype
                new_date_type = new_data_df['date'].dtype
                logger.info(f"Existing date type: {existing_date_type}, New date type: {new_date_type}")

                # 如果现有数据中 date 是 datetime 或 int 类型，则将新数据的 date 也转换为相应类型
                if pd.api.types.is_datetime64_any_dtype(existing_date_type):
                    logger.info(f"Converting date column from {new_date_type} to datetime")
                    new_data_df['date'] = pd.to_datetime(new_data_df['date'], format='%Y%m%d', errors='coerce')
                elif pd.api.types.is_integer_dtype(existing_date_type):
                    logger.info(f"Converting date column from {new_date_type} to int")
                    new_data_df['date'] = pd.to_numeric(new_data_df['date'], errors='coerce').astype(existing_date_type)

            # 创建合并后的数据副本
            combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)

            # 移除重复数据（基于日期和代码，保留最新的）
            combined_df = combined_df.drop_duplicates(
                subset=['date', 'symbol'], keep='last'
            )

            # 排序确保一致性
            combined_df = combined_df.sort_values(
                ['symbol', 'date']
            ).reset_index(drop=True)

            logger.info(f"Combined {len(existing_df)} existing rows with "
                       f"{len(new_data_df)} new rows, total {len(combined_df)} rows")

            return combined_df

        except FileNotFoundError:
            logger.warning(f"Existing data file not found: {self.output_file}")
            return new_data_df
        except (ValueError, KeyError) as e:
            logger.error(f"Data format error during merge: {e}")
            logger.info("Using new data only to prevent corruption")
            return new_data_df
        except Exception as e:
            logger.error(f"Unexpected error combining data: {e}")
            # 保存新数据到备份文件以防数据丢失
            backup_file = self.output_file.replace('.parquet', '_new_only.parquet')
            try:
                new_data_df.to_parquet(backup_file, index=False)
                logger.info(f"New data saved to backup: {backup_file}")
            except Exception as backup_error:
                logger.error(f"Failed to save backup: {backup_error}")
            raise RuntimeError(f"Data merge failed: {e}")

    def _save_data(self, df: pd.DataFrame) -> bool:
        """
        保存数据到文件

        Args:
            df: 要保存的数据

        Returns:
            是否保存成功
        """
        try:
            self.data_storage.save(df, self.output_file)
            logger.info(f"Saved {len(df)} rows to {self.output_file}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to {self.output_file}: {e}")
            # 尝试保存到备份文件
            backup_file = self.output_file.replace('.parquet', '_backup.parquet')
            try:
                df.to_parquet(backup_file, index=False)
                logger.info(f"Saved data to backup file: {backup_file}")
                return True
            except Exception as backup_error:
                logger.error(f"Error saving to backup file: {backup_error}")
                return False

    def run(
        self,
        start_date: str = None,
        end_date: str = None,
        incremental: bool = True
    ) -> bool:
        """
        执行数据获取流程

        Args:
            start_date: 开始日期，格式 'YYYYMMDD'
            end_date: 结束日期，格式 'YYYYMMDD'
            incremental: 是否增量更新

        Returns:
            是否成功获取数据
        """
        from core.connection_manager import disconnect_all_tdx_connections

        self.start_date = start_date or config.DEFAULT_START_DATE
        self.end_date = end_date or get_default_end_date()

        # 增量模式处理
        if incremental and os.path.exists(self.output_file):
            logger.info("Incremental mode enabled. Checking existing data...")
            existing_min_date, existing_max_date = self.get_existing_data_date_range()

            if existing_min_date is not None and existing_max_date is not None:
                logger.info(f"Existing data range: {existing_min_date} to {existing_max_date}")

                # 如果请求的结束日期不晚于现有最大日期，无需获取新数据
                if self.end_date <= existing_max_date:
                    logger.info(f"Requested end date {self.end_date} is not later than "
                               f"existing max date {existing_max_date}. No new data to fetch.")
                    return True

                # 只获取从现有最大日期之后到请求结束日期的数据
                fetch_start_date = get_next_trading_day(existing_max_date)
                if fetch_start_date > self.end_date:
                    logger.info(f"No new trading days between {existing_max_date} and {self.end_date}")
                    return True

                logger.info(f"Fetching incremental data from {fetch_start_date} to {self.end_date}")
                self.start_date = fetch_start_date
            else:
                logger.info("No existing data found or error reading existing data. "
                          "Performing full fetch.")
        else:
            logger.info(f"Full refresh mode. Fetching data from {self.start_date} to {self.end_date}")

        stocks = self.get_stock_list()
        if not stocks:
            logger.error("No stocks found.")
            return False

        all_data = []
        logger.info(f"Starting fetch for {len(stocks)} stocks with {config.MAX_WORKERS} workers...")

        timer_id = performance_monitor.start_timer("fetch_stocks")

        try:
            with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                future_to_stock = {
                    executor.submit(self.process_stock_with_date_range, stock_info, self.start_date, self.end_date): stock_info
                    for stock_info in stocks
                }

                for i, future in enumerate(as_completed(future_to_stock)):
                    try:
                        result = future.result()
                        if result is not None:
                            all_data.append(result)
                    except Exception as e:
                        stock_info = future_to_stock[future]
                        
                        # 特别处理PyTDX协议错误
                        if "head_buf is not 0x10" in str(e):
                            logger.error(f"PyTDX protocol error for {stock_info.get('code', 'unknown')}: {e}")
                            # 短暂休眠后尝试重置连接
                            import time
                            time.sleep(0.5)
                            try:
                                from .connection_manager import disconnect_all_tdx_connections
                                disconnect_all_tdx_connections()
                            except Exception as reset_error:
                                logger.error(f"Failed to reset connections: {reset_error}")
                        else:
                            logger.error(f"Error processing {stock_info.get('code', 'unknown')}: {e}")

                    if (i + 1) % 100 == 0:
                        logger.info(f"Processed {i + 1}/{len(stocks)} stocks...")

        finally:
            duration = performance_monitor.end_timer(timer_id)
            logger.info(f"Completed stock fetching in {duration:.2f} seconds")

        # 断开所有 PyTDX 连接
        disconnect_all_tdx_connections()

        if not all_data:
            logger.info("No new data fetched.")
            return True

        # 处理获取的数据
        new_data_df = pd.concat(all_data, ignore_index=True)

        # 转换数值列
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_cols:
            new_data_df[col] = pd.to_numeric(new_data_df[col], errors='coerce')

        # 验证数据
        from core.helpers import validate_stock_data
        is_valid, validation_report = validate_stock_data(new_data_df)
        if not is_valid:
            logger.warning(f"新获取的数据验证失败: {validation_report}")
        else:
            logger.info(f"新获取的 {len(new_data_df)} 条数据验证通过")

        # 合并数据（增量模式）或直接保存（全量模式）
        if incremental and os.path.exists(self.output_file):
            try:
                final_df = self._combine_data_incremental(new_data_df)
            except RuntimeError:
                logger.error("Data merge failed, refusing to overwrite existing data")
                return False
        else:
            final_df = new_data_df

        return self._save_data(final_df)
