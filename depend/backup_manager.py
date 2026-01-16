"""
数据备份和恢复模块
"""
import os
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional


logger = logging.getLogger(__name__)


class DataBackupManager:
    """数据备份管理器"""
    
    def __init__(self, backup_dir: str = "data/backup"):
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    def create_backup(self, source_file: str, backup_name: Optional[str] = None) -> str:
        """
        创建数据文件的备份
        
        Args:
            source_file (str): 源文件路径
            backup_name (str, optional): 备份文件名，如果为None则使用时间戳
            
        Returns:
            str: 备份文件路径
        """
        source_path = Path(source_file)
        if not source_path.exists():
            logger.warning(f"源文件不存在: {source_file}")
            return ""
        
        if backup_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{source_path.stem}_{timestamp}{source_path.suffix}"
        
        backup_path = self.backup_dir / backup_name
        try:
            shutil.copy2(source_path, backup_path)
            logger.info(f"已创建备份: {source_file} -> {backup_path}")
            return str(backup_path)
        except Exception as e:
            logger.error(f"备份失败 {source_file}: {e}")
            return ""
    
    def restore_from_backup(self, backup_file: str, target_file: str) -> bool:
        """
        从备份恢复数据文件
        
        Args:
            backup_file (str): 备份文件路径
            target_file (str): 目标文件路径
            
        Returns:
            bool: 恢复成功返回True，失败返回False
        """
        backup_path = Path(backup_file)
        target_path = Path(target_file)
        
        if not backup_path.exists():
            logger.error(f"备份文件不存在: {backup_file}")
            return False
        
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(backup_path, target_path)
            logger.info(f"已从备份恢复: {backup_file} -> {target_file}")
            return True
        except Exception as e:
            logger.error(f"恢复失败 {backup_file} -> {target_file}: {e}")
            return False
    
    def list_backups(self, pattern: str = "*") -> list:
        """
        列出备份文件
        
        Args:
            pattern (str): 文件名匹配模式
            
        Returns:
            list: 匹配的备份文件列表
        """
        return [str(f) for f in self.backup_dir.glob(pattern)]
    
    def cleanup_old_backups(self, keep_count: int = 5) -> bool:
        """
        清理旧的备份文件，保留最新的几个
        
        Args:
            keep_count (int): 保留的备份文件数量
            
        Returns:
            bool: 清理成功返回True，失败返回False
        """
        try:
            all_backups = list(self.backup_dir.glob("*"))
            # 按修改时间排序，保留最新的
            all_backups.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            for old_backup in all_backups[keep_count:]:
                old_backup.unlink()
                logger.info(f"已删除旧备份: {old_backup}")
            
            return True
        except Exception as e:
            logger.error(f"清理旧备份失败: {e}")
            return False


# 全局备份管理器实例
backup_manager = DataBackupManager()