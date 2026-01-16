"""
服务实现模块
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
from typing import Optional
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
            logger.error(f"Unexpected connection error: {e}")

    def get_stock_list(self):
        """Get all A-share stocks."""
        try:
            logger.info("Fetching stock list via AkShare...")
            stock_df = ak.stock_zh_a_spot_em()
            stocks = []
            for _, row in stock_df.iterrows():
                symbol = str(row['代码'])
                name = row['名称']
                if symbol.startswith(('900', '200')):
                    continue

                if symbol.startswith(('60', '68')):
                    full_symbol = f"{symbol}.SH"
                elif symbol.startswith(('00', '30')):
                    full_symbol = f"{symbol}.SZ"
                else:
                    continue

                stocks.append({'symbol': full_symbol, 'code': symbol, 'name': name})

            logger.info(f"Found {len(stocks)} A-share stocks.")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stock list: {e}")
            raise  # Re-raise the exception to trigger the retry decorator

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
            logger.error(error_msg)
            return None
        finally:
            response_time = time.time() - start_time
            monitoring_manager.record_request(success=success, response_time=response_time, error_msg=error_msg)


class AkShareDataFetcher:
    """AkShare数据获取实现"""
    
    def get_stock_list(self):
        """获取股票列表 - 使用AkShare"""
        try:
            logger.info("Fetching stock list via AkShare...")
            stock_df = ak.stock_zh_a_spot_em()
            stocks = []
            for _, row in stock_df.iterrows():
                symbol = str(row['代码'])
                name = row['名称']
                if symbol.startswith(('900', '200')):
                    continue

                if symbol.startswith(('60', '68')):
                    full_symbol = f"{symbol}.SH"
                elif symbol.startswith(('00', '30')):
                    full_symbol = f"{symbol}.SZ"
                else:
                    continue

                stocks.append({'symbol': full_symbol, 'code': symbol, 'name': name})

            logger.info(f"Found {len(stocks)} A-share stocks.")
            return stocks
        except Exception as e:
            logger.error(f"Error fetching stock list: {e}")
            raise

    def fetch_daily_data(self, code, symbol, start_date, end_date):
        """Fetch daily data using AkShare (fallback) with specific date range."""
        start_time = time.time()
        success = False
        error_msg = ""

        try:
            # AkShare is HTTP based, thread-safe usually
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if df.empty:
                monitoring_manager.record_request(success=True, response_time=time.time()-start_time,
                                               error_msg=f"Empty data from AkShare for {code}")
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
            result = df[['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
            success = True
            return result
        except Exception as e:
            error_msg = f"AkShare fetch failed for {code}: {str(e)}"
            logger.error(error_msg)
            raise  # Re-raise the exception to trigger the retry decorator
        finally:
            response_time = time.time() - start_time
            monitoring_manager.record_request(success=success, response_time=response_time, error_msg=error_msg)


class CompositeDataFetcher:
    """复合数据获取器，结合PyTDX和AkShare"""
    
    def __init__(self, pytdx_fetcher: PyTDXDataFetcher, akshare_fetcher: AkShareDataFetcher):
        self.pytdx_fetcher = pytdx_fetcher
        self.akshare_fetcher = akshare_fetcher

    def get_stock_list(self):
        """获取股票列表，优先使用AkShare"""
        return self.akshare_fetcher.get_stock_list()

    def fetch_daily_data(self, code, market, start_date, end_date):
        """获取日线数据，优先使用PyTDX，失败后使用AkShare"""
        # Try PyTDX first
        df = self.pytdx_fetcher.fetch_daily_data(code, market, start_date, end_date)

        # Fallback to AkShare
        if df is None or df.empty:
            symbol = f"{code}.SH" if market == 1 else f"{code}.SZ"  # Construct symbol for AkShare
            df = self.akshare_fetcher.fetch_daily_data(code, symbol, start_date, end_date)

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
            invalid_ohlc = (
                (df['high'] < df['low']) | 
                (df['high'] < df['open']) | 
                (df['high'] < df['close']) | 
                (df['low'] > df['open']) | 
                (df['low'] > df['close'])
            ).sum()
            if invalid_ohlc > 0:
                validation_report.append(f"OHLC关系不合理: {invalid_ohlc} 条记录")
                is_valid = False
        
        # 检查重复数据
        duplicate_rows = df.duplicated(subset=['symbol', 'date']).sum()
        if duplicate_rows > 0:
            validation_report.append(f"存在 {duplicate_rows} 条重复数据")
        
        # 检查缺失值
        total_missing = df.isnull().sum().sum()
        if total_missing > 0:
            validation_report.append(f"存在 {total_missing} 个缺失值")
        
        return is_valid, validation_report


class DataStorage:
    """数据存储实现"""

    def save(self, data, file_path: str, create_backup: bool = False):
        """保存数据到文件，根据文件扩展名选择格式"""
        # 如果目标文件存在且需要备份，则创建备份
        if create_backup and os.path.exists(file_path):
            backup_manager.create_backup(file_path)

        try:
            if file_path.endswith('.parquet'):
                data.to_parquet(file_path, index=False)
            elif file_path.endswith('.csv'):
                data.to_csv(file_path, index=False)
            elif file_path.endswith('.json'):
                data.to_json(file_path, orient='records', force_ascii=False, indent=2)
            else:
                # Default to parquet
                data.to_parquet(file_path, index=False)

            logger.info(f"Saved data to {file_path}")
        except Exception as e:
            logger.error(f"Error saving data to {file_path}: {e}")
            # Try to save to a backup file
            if file_path.endswith('.parquet'):
                backup_file = file_path.replace('.parquet', '_backup.parquet')
            elif file_path.endswith('.csv'):
                backup_file = file_path.replace('.csv', '_backup.csv')
            elif file_path.endswith('.json'):
                backup_file = file_path.replace('.json', '_backup.json')
            else:
                backup_file = file_path + '_backup'

            try:
                if file_path.endswith('.parquet'):
                    data.to_parquet(backup_file, index=False)
                elif file_path.endswith('.csv'):
                    data.to_csv(backup_file, index=False)
                elif file_path.endswith('.json'):
                    data.to_json(backup_file, orient='records', force_ascii=False, indent=2)
                else:
                    data.to_parquet(backup_file, index=False)

                logger.info(f"Saved data to backup file: {backup_file}")
            except Exception as backup_error:
                logger.error(f"Error saving to backup file: {backup_error}")

    def load(self, file_path: str, from_backup: bool = False):
        """从文件加载数据，根据文件扩展名选择格式"""
        if from_backup:
            # 尝试从备份恢复
            backup_files = backup_manager.list_backups(f"*{os.path.basename(file_path)}*")
            if backup_files:
                # 使用最新的备份
                latest_backup = sorted(backup_files, key=os.path.getmtime, reverse=True)[0]
                backup_manager.restore_from_backup(latest_backup, file_path)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found")

        if file_path.endswith('.parquet'):
            return pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path.endswith('.json'):
            return pd.read_json(file_path)
        else:
            # Default to parquet
            return pd.read_parquet(file_path)