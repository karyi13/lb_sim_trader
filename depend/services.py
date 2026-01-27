"""
服务实现模块
"""
import pandas as pd
import numpy as np
from pytdx.hq import TdxHq_API
import baostock as bs
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import time
import os
import threading
import json
from typing import Optional
from pathlib import Path
from .interfaces import DataFetcherInterface, DataProcessorInterface, DataValidatorInterface, DataStorageInterface
from .config import config
from function.stock_concepts import get_stock_concepts
import requests
from .backup_manager import backup_manager
from .monitoring import monitoring_manager
import time


logger = logging.getLogger(__name__)

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
        except (ConnectionError, OSError, TimeoutError) as e:
            logger.warning(f"PyTDX connection failed: {e}")
        except Exception as e:
            if "head_buf is not 0x10" in str(e):
                logger.warning(f"PyTDX connection failed due to protocol error: {e}")
            else:
                logger.error(f"Unexpected PyTDX connection error: {e}")
        thread_local.api = None
    return thread_local.api


class PyTDXDataFetcher:
    """PyTDX数据获取实现"""

    def __init__(self):
        self.main_api = TdxHq_API()
        self.connected = False
        self.connect_main()

    def connect_main(self):
        try:
            primary_server = config.PYTDX_SERVERS[0]
            if self.main_api.connect(primary_server[0], primary_server[1], time_out=config.REQUEST_TIMEOUT):
                self.connected = True
        except (ConnectionError, OSError, TimeoutError) as e:
            logger.warning(f"Main API connection failed: {e}")
        except Exception as e:
            if "head_buf is not 0x10" in str(e):
                logger.warning(f"Main API connection failed due to protocol error: {e}")
            else:
                logger.error(f"Unexpected connection error: {e}")

    def get_security_list_pytdx(self, market=0):
        """Get A-share stock list from PyTDX directly."""
        try:
            # Get security list bytes from PyTDX
            data = self.main_api.get_security_list(market, 0)
            if not data:
                return []

            # Convert bytes to list of stocks
            stocks = []
            for stock in data:
                # stock格式: (code, name, pre close, vol amount)
                code = stock[0]
                name = stock[1].decode('utf-8', errors='ignore') if isinstance(stock[1], bytes) else str(stock[1])

                if not code:
                    continue

                # Filter B-shares (900, 200)
                if code.startswith('900') or code.startswith('200'):
                    continue

                # Determine market suffix
                if code.startswith('6'):  # Shanghai A-shares and STAR
                    full_symbol = f"{code}.SH"
                else:  # Shenzhen A-shares and ChiNext
                    full_symbol = f"{code}.SZ"

                stocks.append({'symbol': full_symbol, 'code': code, 'name': name})

            return stocks
        except Exception as e:
            if "head_buf is not 0x10" in str(e):
                logger.warning(f"Failed to get PyTDX security list for market {market} due to protocol error: {e}")
            else:
                logger.warning(f"Failed to get PyTDX security list for market {market}: {e}")
            return []

    def get_stock_list(self):
        """Get all A-share stocks via PyTDX."""
        stocks = []

        # Market 0 = Shenzhen, Market 1 = Shanghai
        for market in [0, 1]:
            try:
                market_stocks = self.get_security_list_pytdx(market)
                stocks.extend(market_stocks)
            except Exception as e:
                if "head_buf is not 0x10" in str(e):
                    logger.warning(f"Failed to get stocks for market {market} due to protocol error: {e}")
                else:
                    logger.warning(f"Failed to get stocks for market {market}: {e}")

        logger.info(f"Found {len(stocks)} A-share stocks from PyTDX.")
        return stocks

    def fetch_daily_data(self, code, market, start_date, end_date):
        """Fetch daily data using Thread-Local PyTDX with specific date range."""
        start_time = time.time()
        api = get_thread_api()
        success = False
        error_msg = ""
        
        if not api:
            monitoring_manager.record_request(success=False, response_time=time.time()-start_time,
                                            error_msg=f"PyTDX API connection failed for {code}")
            return None

        try:
            # market: 0 - SZ, 1 - SH
            # category: 9 - Day
            # Fetch more bars than needed to ensure we cover the date range
            data = api.get_security_bars(9, market, code, 0, 400)
            if not data:
                monitoring_manager.record_request(success=False, response_time=time.time()-start_time,
                                                error_msg=f"No data returned from PyTDX for {code}")
                return None

            df = api.to_df(data)
            df['date'] = df['datetime'].apply(lambda x: x[:10].replace('-', ''))
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

            if df.empty:
                monitoring_manager.record_request(success=True, response_time=time.time()-start_time,
                                               error_msg=f"Empty data for {code} in date range {start_date}-{end_date}")
                return pd.DataFrame()

            df = df.rename(columns={'vol': 'volume'})
            result = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            success = True
            return result

        except Exception as e:
            error_msg = f"PyTDX fetch failed for {code}: {str(e)}"
            
            # 特别处理head_buf错误
            if "head_buf is not 0x10" in str(e):
                logger.error(f"PyTDX protocol error for {code}: {str(e)}")
                # 重置连接，以便下次获取新的连接
                if hasattr(thread_local, "api"):
                    try:
                        thread_local.api.disconnect()
                    except:
                        pass
                    delattr(thread_local, "api")
            else:
                logger.error(error_msg)
                
            return None
        finally:
            response_time = time.time() - start_time
            monitoring_manager.record_request(success=success, response_time=response_time, error_msg=error_msg)


class AkShareDataFetcher:
    """AkShare数据获取实现"""

    def __init__(self):
        self.rate_limiter = None
        try:
            from core.concurrency_control import get_akshare_rate_limiter
            self.rate_limiter = get_akshare_rate_limiter()
        except ImportError:
            logger.warning("Rate limiter not available, proceeding without rate limiting")

    def get_stock_list(self):
        """Get A-share stock list via AkShare."""
        try:
            # 获取沪深A股列表
            stock_info = ak.stock_info_a_code_name()
            
            stocks = []
            for _, row in stock_info.iterrows():
                code = row['code']
                name = row['name']
                
                # 过滤B股
                if code.startswith('900') or code.startswith('200'):
                    continue
                
                # 确定市场后缀
                if code.startswith('6'):  # 上海A股和科创板
                    full_symbol = f"{code}.SH"
                else:  # 深圳A股和创业板
                    full_symbol = f"{code}.SZ"
                
                stocks.append({
                    'symbol': full_symbol,
                    'code': code,
                    'name': name
                })
            
            logger.info(f"Found {len(stocks)} A-share stocks from AkShare.")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stock list from AkShare: {e}")
            return []

    def fetch_daily_data(self, code, symbol, start_date, end_date):
        """Fetch daily data using AkShare with specific date range."""
        start_time = time.time()
        success = False
        error_msg = ""
        max_retries = config.MAX_RETRIES
        last_exception = None

        try:
            # 应用速率限制
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # 重试循环
            for attempt in range(max_retries):
                try:
                    # AkShare 使用不同的日期格式
                    ak_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                    ak_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                    
                    # 获取股票历史数据
                    df = ak.stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date=ak_start,
                        end_date=ak_end,
                        adjust=""
                    )
                    
                    if df is not None and not df.empty:
                        # 重命名列以匹配预期格式
                        df = df.rename(columns={
                            '日期': 'date',
                            '开盘': 'open',
                            '收盘': 'close', 
                            '最高': 'high',
                            '最低': 'low',
                            '成交量': 'volume',
                            '成交额': 'amount'
                        })
                        
                        # 转换日期格式
                        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
                        
                        # 添加股票代码
                        df['symbol'] = symbol
                        
                        success = True
                        return df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
                    else:
                        # 返回空DataFrame而不是抛出异常
                        logger.debug(f"AkShare returned empty data for {code}")
                        return pd.DataFrame()
                        
                except ValueError as e:
                    # 数据格式错误，重试无意义
                    error_msg = f"AkShare data format error for {code}: {str(e)}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg) from e

                except Exception as e:
                    last_exception = e
                    error_msg = f"AkShare fetch failed (attempt {attempt + 1}/{max_retries}) for {code}: {str(e)}"

                    if attempt < max_retries - 1:
                        delay = self._get_exponential_backoff_delay(attempt)
                        logger.warning(f"{error_msg}, waiting {delay:.2f}s before retry...")
                        time.sleep(delay)
                    else:
                        logger.error(error_msg)

            # 所有重试都失败
            if last_exception:
                raise RuntimeError(f"AkShare fetch failed for {code} after {max_retries} attempts") from last_exception

            # 返回空DataFrame而不是抛出异常
            error_msg = f"AkShare fetch failed for {code}: unknown error"
            logger.error(error_msg)
            return pd.DataFrame()

        finally:
            response_time = time.time() - start_time
            monitoring_manager.record_request(
                success=success,
                response_time=response_time,
                error_msg=error_msg
            )

    def _get_exponential_backoff_delay(self, attempt):
        """计算指数退避延迟"""
        base_delay = config.BASE_DELAY
        max_delay = config.MAX_DELAY
        delay = min(base_delay * (config.BACKOFF_FACTOR ** attempt), max_delay)
        return delay


class BaostockDataFetcher:
    """Baostock数据获取实现"""

    def __init__(self):
        self.connected = False
        self.connect()

    def connect(self):
        """连接Baostock服务"""
        try:
            ld = bs.login()
            if ld.error_code != '0':
                logger.warning(f"Baostock login failed: {ld.error_msg}")
                self.connected = False
            else:
                self.connected = True
                logger.info("Baostock connected successfully")
        except Exception as e:
            logger.warning(f"Baostock connection error: {e}")
            self.connected = False

    def get_stock_list(self):
        """Get A-share stock list via Baostock."""
        try:
            if not self.connected:
                self.connect()

            stocks = []
            # 获取沪深A股列表 - 使用 query_stock_basic
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                logger.error(f"Baostock stock list query failed: {rs.error_msg}")
                return []

            # 解析股票列表
            # 返回格式: [code, name, ipoDate, outDate, type, status]
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                code = row[0]  # sh.600000 或 sz.000001
                name = row[1]
                stock_type = row[4]  # 1:股票, 2:指数
                status = row[5]  # 1:正常上市

                # 只获取正常上市的股票，跳过指数
                if stock_type != '1' or status != '1':
                    continue

                # 过滤B股（900, 200）和基金(50, 51)
                if '900' in code or '200' in code or code.startswith(('50', '51')):
                    continue

                # 转换为标准格式
                # sh.600000 -> 600000.SH
                if 'sh.' in code:
                    symbol = f"{code[3:]}.SH"
                else:
                    symbol = f"{code[3:]}.SZ"

                stocks.append({
                    'symbol': symbol,
                    'code': code[3:],
                    'name': name
                })

            logger.info(f"Found {len(stocks)} A-share stocks from Baostock.")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stock list from Baostock: {e}")
            return []

    def fetch_daily_data(self, code, symbol, start_date, end_date):
        """Fetch daily data using Baostock."""
        start_time = time.time()
        success = False
        error_msg = ""
        max_retries = config.MAX_RETRIES
        last_exception = None

        try:
            if not self.connected:
                self.connect()

            # 转换日期格式 Baostock: yyyy-MM-dd
            baostock_start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
            baostock_end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

            # 转换股票代码格式: 600000 -> sh.600000
            if '.SH' in symbol:
                baostock_code = f"sh.{code}"
            else:
                baostock_code = f"sz.{code}"

            # 重试循环
            for attempt in range(max_retries):
                try:
                    rs = bs.query_history_k_data_plus(
                        baostock_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=baostock_start,
                        end_date=baostock_end,
                        frequency="d",
                        adjustflag="3"  # 不复权
                    )

                    if rs.error_code == '0' and (rs.error_code == '0') and not rs.empty():
                        data_list = []
                        while (rs.error_code == '0') & rs.next():
                            data_list.append(rs.get_row_data())

                        if data_list:
                            df = pd.DataFrame(data_list, columns=[
                                'date', 'open', 'high', 'low', 'close', 'volume', 'amount'
                            ])

                            # 转换数据类型
                            df['date'] = df['date'].str.replace('-', '').astype(str)
                            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                                df[col] = pd.to_numeric(df[col], errors='coerce')

                            # 添加股票代码
                            df['symbol'] = symbol

                            # 移除空行
                            df = df.dropna(subset=['date', 'open', 'high', 'low', 'close'])

                            success = True
                            return df
                        else:
                            logger.debug(f"Baostock returned empty data for {code}")
                            return pd.DataFrame()
                    else:
                        error_msg = f"Baostock query failed (attempt {attempt + 1}/{max_retries}) for {code}: {rs.error_msg}"

                        if attempt < max_retries - 1:
                            delay = self._get_exponential_backoff_delay(attempt)
                            logger.warning(f"{error_msg}, waiting {delay:.2f}s before retry...")
                            time.sleep(delay)
                        else:
                            logger.error(error_msg)

                except Exception as e:
                    last_exception = e
                    error_msg = f"Baostock fetch failed (attempt {attempt + 1}/{max_retries}) for {code}: {str(e)}"

                    if attempt < max_retries - 1:
                        delay = self._get_exponential_backoff_delay(attempt)
                        logger.warning(f"{error_msg}, waiting {delay:.2f}s before retry...")
                        time.sleep(delay)
                    else:
                        logger.error(error_msg)

            # 所有重试都失败
            if last_exception:
                raise RuntimeError(f"Baostock fetch failed for {code} after {max_retries} attempts") from last_exception

            error_msg = f"Baostock fetch failed for {code}: unknown error"
            logger.error(error_msg)
            return pd.DataFrame()

        finally:
            response_time = time.time() - start_time
            monitoring_manager.record_request(
                success=success,
                response_time=response_time,
                error_msg=error_msg
            )

    def _get_exponential_backoff_delay(self, attempt):
        """计算指数退避延迟"""
        base_delay = config.BASE_DELAY
        max_delay = config.MAX_DELAY
        delay = min(base_delay * (config.BACKOFF_FACTOR ** attempt), max_delay)
        return delay


class CompositeDataFetcher:
    """复合数据获取器，结合PyTDX和备用源"""

    def __init__(self, pytdx_fetcher: PyTDXDataFetcher, baostock_fetcher: BaostockDataFetcher):
        self.pytdx_fetcher = pytdx_fetcher
        self.baostock_fetcher = baostock_fetcher

    def get_stock_list(self):
        """获取股票列表，优先使用Baostock，失败则使用PyTDX"""
        try:
            stocks = self.baostock_fetcher.get_stock_list()
            if stocks:
                return stocks
            logger.warning("Baostock returned empty stock list, trying PyTDX...")
        except Exception as e:
            logger.warning(f"Baostock stock list failed: {e}, trying PyTDX...")

        try:
            stocks = self.pytdx_fetcher.get_stock_list()
            if stocks:
                return stocks
        except Exception as e:
            logger.warning(f"PyTDX stock list also failed: {e}")

        # 两个都失败，返回空列表但不会中断程序
        logger.error("No stocks found from any source.")
        return []

    def fetch_daily_data(self, code, market, start_date, end_date):
        """获取日线数据，优先使用PyTDX，失败后使用Baostock"""
        # Try PyTDX first
        df = self.pytdx_fetcher.fetch_daily_data(code, market, start_date, end_date)

        # Fallback to Baostock - catch exceptions to avoid breaking the whole process
        if df is None or df.empty:
            try:
                symbol = f"{code}.SH" if market == 1 else f"{code}.SZ"
                df = self.baostock_fetcher.fetch_daily_data(code, symbol, start_date, end_date)
            except Exception as e:
                # Log error but don't raise - PyTDX data that succeeded will be saved
                logger.debug(f"Baostock fallback failed for {code}: {str(e)}")
                df = None

        return df


class DataValidator:
    """数据验证实现"""
    
    def validate(self, df):
        """
        验证股票数据的完整性与合理性
        
        Args:
            df (pd.DataFrame): 股票数据DataFrame
            
        Returns:
            tuple: (is_valid, validation_report)
        """
        if df.empty:
            return False, ["数据为空"]
        
        validation_report = []
        is_valid = True
        
        # 检查必需列是否存在
        required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            validation_report.append(f"缺少必要列: {missing_columns}")
            is_valid = False
        
        # 检查数据类型
        if 'date' in df.columns:
            # 尝试转换日期格式
            try:
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
                invalid_dates = df['date'].isna().sum()
                if invalid_dates > 0:
                    validation_report.append(f"无效日期格式: {invalid_dates} 条记录")
            except:
                validation_report.append("日期格式转换失败")
                is_valid = False
        
        # 检查数值列的合理性
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                # 检查负值
                negative_values = (df[col] < 0).sum()
                if negative_values > 0:
                    validation_report.append(f"{col} 列存在 {negative_values} 个负值")
                
                # 检查异常值（如价格为0或异常高）
                if col in ['open', 'high', 'low', 'close']:
                    zero_prices = (df[col] == 0).sum()
                    if zero_prices > 0:
                        validation_report.append(f"{col} 列存在 {zero_prices} 个零价格")
                    
                    # 检查价格是否合理（比如超过10000元的股票可能需要检查）
                    high_prices = (df[col] > 10000).sum()
                    if high_prices > 0:
                        validation_report.append(f"{col} 列存在 {high_prices} 个异常高价格(>10000)")
        
        # 检查 OHLC 关系的合理性
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            # 检查 high >= open, high >= low, high >= close
            invalid_high = ((df['high'] < df['open']) & (df['high'] < df['low']) & (df['high'] < df['close'])).sum()
            if invalid_high > 0:
                validation_report.append(f"high 值小于所有其他价格的记录数: {invalid_high}")
            
            # 检查 low <= open, low <= high, low <= close
            invalid_low = ((df['low'] > df['open']) & (df['low'] > df['high']) & (df['low'] > df['close'])).sum()
            if invalid_low > 0:
                validation_report.append(f"low 值大于所有其他价格的记录数: {invalid_low}")
        
        # 检查symbol列是否有有效值
        if 'symbol' in df.columns:
            invalid_symbols = df['symbol'].isna().sum() + (df['symbol'].str.len() == 0).sum()
            if invalid_symbols > 0:
                validation_report.append(f"symbol 列存在 {invalid_symbols} 个无效值")
        
        return is_valid, validation_report


class DataStorage:
    """数据存储实现"""
    
    def __init__(self):
        self.backup_manager = backup_manager
    
    def save(self, df, filepath):
        """
        保存数据到文件
        
        Args:
            df (pd.DataFrame): 要保存的数据
            filepath (str): 文件路径
        """
        try:
            # 在保存前进行备份
            self.backup_manager.backup_if_exists(filepath)
            
            # 保存数据
            df.to_parquet(filepath, index=False)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
            
            # 清理旧备份
            self.backup_manager.cleanup_old_backups(filepath)
            
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {e}")
            # 尝试恢复备份
            if self.backup_manager.has_backup(filepath):
                logger.info("Attempting to restore from backup...")
                source_path = Path(filepath)
                backup_pattern = f"{source_path.stem}_*{source_path.suffix}"
                backups = list(self.backup_manager.backup_dir.glob(backup_pattern))
                if backups:
                    latest_backup = sorted(backups, key=lambda x: x.stat().st_mtime, reverse=True)[0]
                    self.backup_manager.restore_from_backup(str(latest_backup), filepath)
            raise


class DataProcessor:
    """数据处理实现"""
    
    def process(self, df):
        """
        处理股票数据
        
        Args:
            df (pd.DataFrame): 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if df.empty:
            return df
        
        # 确保数值列为适当的数据类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 确保日期列为字符串格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y%m%d').astype(str)
        
        # 按股票代码和日期排序
        if 'symbol' in df.columns and 'date' in df.columns:
            df = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        
        return df