"""
配置文件
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    # 数据文件路径
    DEFAULT_OUTPUT_FILE: str = os.path.join('data', 'stock_daily_latest.parquet')
    DEFAULT_LADDER_FILE: str = os.path.join('data', 'limit_up_ladder.parquet')
    DEFAULT_PROMOTION_FILE: str = os.path.join('data', 'promotion_rates.csv')
    DEFAULT_LADDER_JS_FILE: str = os.path.join('data', 'ladder_data.js')
    DEFAULT_KLINE_JS_FILE: str = os.path.join('data', 'kline_data.js')
    
    # 默认日期
    DEFAULT_START_DATE: str = '20241220'
    
    # 网络请求配置
    REQUEST_TIMEOUT: int = 10
    MAX_RETRIES: int = 3
    BASE_DELAY: int = 1
    MAX_DELAY: int = 15
    BACKOFF_FACTOR: int = 2
    
    # 并发配置
    MAX_WORKERS: int = 20
    CONCEPT_FETCH_WORKERS: int = 10
    
    # 数据处理配置
    CHUNK_SIZE: int = 50000
    
    # PyTDX 服务器配置
    PYTDX_SERVERS: list = None  # 默认服务器列表将在 __post_init__ 中设置
    
    # 东方财富API配置
    EASTMONEY_API_URL: str = "http://push2.eastmoney.com/api/qt/stock/get"
    
    def __post_init__(self):
        if self.PYTDX_SERVERS is None:
            self.PYTDX_SERVERS = [
                ('121.36.81.195', 7709),
                ('119.147.212.81', 7709),
                ('47.107.75.159', 7709)
            ]


# 创建全局配置实例
config = Config()