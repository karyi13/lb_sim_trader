"""
结构化日志和性能监控工具
"""
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any


class StructuredLogger:
    """结构化日志记录器"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # 如果还没有处理器，添加一个控制台处理器
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _log_structured(self, level: int, message: str, extra_data: Dict[str, Any] = None):
        """记录结构化日志"""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': logging.getLevelName(level),
            'message': message,
            'module': self.logger.name
        }
        
        if extra_data:
            log_entry.update(extra_data)
        
        # 记录JSON格式的日志
        self.logger.log(level, json.dumps(log_entry, ensure_ascii=False))
    
    def info(self, message: str, extra_data: Dict[str, Any] = None):
        self._log_structured(logging.INFO, message, extra_data)
    
    def warning(self, message: str, extra_data: Dict[str, Any] = None):
        self._log_structured(logging.WARNING, message, extra_data)
    
    def error(self, message: str, extra_data: Dict[str, Any] = None):
        self._log_structured(logging.ERROR, message, extra_data)
    
    def debug(self, message: str, extra_data: Dict[str, Any] = None):
        self._log_structured(logging.DEBUG, message, extra_data)


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.metrics = {}
    
    def start_timer(self, operation_name: str) -> str:
        """开始计时操作"""
        timer_id = f"{operation_name}_{time.time()}"
        self.metrics[timer_id] = {
            'operation': operation_name,
            'start_time': time.time(),
            'end_time': None,
            'duration': None
        }
        return timer_id
    
    def end_timer(self, timer_id: str, extra_data: Dict[str, Any] = None) -> float:
        """结束计时操作并返回持续时间"""
        if timer_id not in self.metrics:
            return 0
        
        end_time = time.time()
        duration = end_time - self.metrics[timer_id]['start_time']
        
        self.metrics[timer_id].update({
            'end_time': end_time,
            'duration': duration
        })
        
        # 返回持续时间用于日志记录
        return duration
    
    def get_metric(self, timer_id: str) -> Dict[str, Any]:
        """获取特定指标"""
        return self.metrics.get(timer_id, {})
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """获取所有指标"""
        return self.metrics.copy()


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()


def log_performance(operation_name: str, extra_data: Dict[str, Any] = None):
    """装饰器：记录函数执行性能"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            timer_id = performance_monitor.start_timer(operation_name)
            try:
                result = func(*args, **kwargs)
                duration = performance_monitor.end_timer(timer_id, extra_data)
                
                # 记录性能日志
                structured_logger = StructuredLogger(func.__module__)
                structured_logger.info(
                    f"Performance: {operation_name} completed",
                    {
                        'function': func.__name__,
                        'duration_seconds': round(duration, 4),
                        'status': 'success',
                        **(extra_data or {})
                    }
                )
                
                return result
            except Exception as e:
                duration = performance_monitor.end_timer(timer_id, extra_data)
                
                # 记录错误性能日志
                structured_logger = StructuredLogger(func.__module__)
                structured_logger.error(
                    f"Performance: {operation_name} failed",
                    {
                        'function': func.__name__,
                        'duration_seconds': round(duration, 4),
                        'status': 'failed',
                        'error': str(e),
                        **(extra_data or {})
                    }
                )
                
                raise
        return wrapper
    return decorator