"""
接口定义模块
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional


class DataFetcherInterface(ABC):
    """数据获取接口"""
    
    @abstractmethod
    def get_stock_list(self):
        """获取股票列表"""
        pass
    
    @abstractmethod
    def fetch_daily_data(self, code, market, start_date, end_date):
        """获取日线数据"""
        pass


class DataProcessorInterface(ABC):
    """数据处理接口"""
    
    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理数据"""
        pass


class DataValidatorInterface(ABC):
    """数据验证接口"""
    
    @abstractmethod
    def validate(self, data: pd.DataFrame) -> tuple[bool, list]:
        """验证数据"""
        pass


class DataStorageInterface(ABC):
    """数据存储接口"""
    
    @abstractmethod
    def save(self, data: pd.DataFrame, file_path: str):
        """保存数据"""
        pass
    
    @abstractmethod
    def load(self, file_path: str) -> pd.DataFrame:
        """加载数据"""
        pass