"""
监控模块 - 用于跟踪数据获取成功率等指标
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


@dataclass
class FetchMetrics:
    """数据获取指标"""
    date: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    success_rate: float = 0.0
    avg_response_time: float = 0.0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MonitoringManager:
    """监控管理器"""
    
    def __init__(self, metrics_file: str = "data/metrics.json"):
        self.metrics_file = Path(metrics_file)
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
        self.current_metrics = FetchMetrics(date=datetime.now().strftime("%Y-%m-%d"))
        
    def record_request(self, success: bool, response_time: float = 0.0, error_msg: str = ""):
        """记录单次请求"""
        self.current_metrics.total_requests += 1
        
        if success:
            self.current_metrics.successful_requests += 1
        else:
            self.current_metrics.failed_requests += 1
            if error_msg:
                self.current_metrics.errors.append(error_msg)
        
        # 更新成功率
        if self.current_metrics.total_requests > 0:
            self.current_metrics.success_rate = (
                self.current_metrics.successful_requests / self.current_metrics.total_requests
            )
        
        # 更新平均响应时间
        total_time = (
            self.current_metrics.avg_response_time * (self.current_metrics.total_requests - 1)
        ) + response_time
        self.current_metrics.avg_response_time = total_time / self.current_metrics.total_requests
    
    def get_current_metrics(self) -> FetchMetrics:
        """获取当前指标"""
        return self.current_metrics
    
    def save_metrics(self):
        """保存指标到文件"""
        try:
            # 读取现有数据
            existing_data = []
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            
            # 添加当前指标
            existing_data.append(asdict(self.current_metrics))
            
            # 限制历史记录数量，保留最近30天
            if len(existing_data) > 30:
                existing_data = existing_data[-30:]
            
            # 保存到文件
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"已保存监控指标到 {self.metrics_file}")
        except Exception as e:
            logger.error(f"保存监控指标失败: {e}")
    
    def load_metrics(self) -> List[FetchMetrics]:
        """从文件加载指标"""
        try:
            if not self.metrics_file.exists():
                return []
            
            with open(self.metrics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metrics_list = []
            for item in data:
                metrics = FetchMetrics(
                    date=item['date'],
                    total_requests=item['total_requests'],
                    successful_requests=item['successful_requests'],
                    failed_requests=item['failed_requests'],
                    success_rate=item['success_rate'],
                    avg_response_time=item['avg_response_time'],
                    errors=item['errors']
                )
                metrics_list.append(metrics)
            
            return metrics_list
        except Exception as e:
            logger.error(f"加载监控指标失败: {e}")
            return []
    
    def get_success_rate_trend(self, days: int = 7) -> List[Dict]:
        """获取成功率趋势"""
        metrics_list = self.load_metrics()
        recent_metrics = metrics_list[-days:] if len(metrics_list) >= days else metrics_list
        
        trend = []
        for metric in recent_metrics:
            trend.append({
                'date': metric.date,
                'success_rate': metric.success_rate,
                'total_requests': metric.total_requests
            })
        
        return trend
    
    def get_summary(self) -> Dict:
        """获取监控摘要"""
        metrics_list = self.load_metrics()
        if not metrics_list:
            return {
                'total_requests': 0,
                'successful_requests': 0,
                'failed_requests': 0,
                'overall_success_rate': 0.0,
                'avg_response_time': 0.0
            }
        
        total_requests = sum(m.total_requests for m in metrics_list)
        total_successful = sum(m.successful_requests for m in metrics_list)
        total_failed = sum(m.failed_requests for m in metrics_list)
        avg_response_time = sum(m.avg_response_time for m in metrics_list) / len(metrics_list) if metrics_list else 0.0
        
        overall_success_rate = total_successful / total_requests if total_requests > 0 else 0.0
        
        return {
            'total_requests': total_requests,
            'successful_requests': total_successful,
            'failed_requests': total_failed,
            'overall_success_rate': overall_success_rate,
            'avg_response_time': avg_response_time
        }


# 全局监控管理器实例
monitoring_manager = MonitoringManager()