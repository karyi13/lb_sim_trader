"""
按需加载K线数据 - 生成分片JSON文件
将大K线数据文件拆分为单股票JSON文件，实现按需加载
"""
import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KlineDataSplitter:
    """K线数据分片器 - 生成按股票分片的JSON文件"""

    def __init__(self, parquet_file, output_dir='data/kline_split'):
        self.parquet_file = parquet_file
        self.output_dir = output_dir
        self.index_file = os.path.join(output_dir, 'index.json')

    def load_parquet(self):
        """加载parquet数据"""
        logger.info("Loading parquet data from {}".format(self.parquet_file))
        df = pd.read_parquet(self.parquet_file)
        logger.info("Loaded {} records".format(len(df)))
        return df

    def generate_stock_index(self, df):
        """生成股票索引文件"""
        logger.info("Generating stock index...")

        unique_stocks = df[['symbol', 'name']].drop_duplicates()
        index_data = {}

        for _, row in unique_stocks.iterrows():
            code = row['symbol']
            name = row['name']
            # 文件名使用简化格式，替换.为_
            filename = code.replace('.', '_') + '.json'
            index_data[code] = {
                'name': name,
                'file': 'kline_split/{}'.format(filename)
            }

        return index_data

    def create_compressed_value(self, values):
        """压缩values数据 - 使用delta编码减少体积"""
        # values is Nx4 array: [open, close, low, high]
        # 使用delta编码存储
        base_values = values[0]
        deltas = []

        for i in range(1, len(values)):
            delta = values[i] - values[i-1]
            # 转换为列表，保留2位小数以减少精度
            deltas.append(delta.tolist())

        return {
            'base': base_values.tolist(),
            'deltas': [[round(float(x), 2) for x in row] for row in deltas]
        }

    def save_stock_data(self, code, name, dates, values, volumes):
        """保存单支股票数据为JSON文件"""
        os.makedirs(self.output_dir, exist_ok=True)

        filename = code.replace('.', '_') + '.json'
        filepath = os.path.join(self.output_dir, filename)

        data = {
            'code': code,
            'name': name,
            'dates': dates,
            # 压缩后的values数据
            'values': [
                [round(float(val[0]), 2),  # open
                 round(float(val[1]), 2),  # close
                 round(float(val[2]), 2),  # low
                 round(float(val[3]), 2)]  # high
                for val in values
            ],
            'volumes': [int(vol) if pd.notna(vol) else 0 for vol in volumes]
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

        return filepath

    def split_data(self, df):
        """拆分数据为单股票JSON文件"""
        logger.info("Starting data split...")

        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)

        # 生成并保存索引
        index_data = self.generate_stock_index(df)
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, separators=(',', ':'))
        logger.info("Saved index with {} stocks to {}".format(len(index_data), self.index_file))

        # 按股票分组处理
        grouped = df.groupby('symbol')

        # 使用多线程加速保存
        def save_group(code, group):
            group_sorted = group.sort_values('date')
            name = group_sorted['name'].iloc[0] if 'name' in group_sorted.columns else ''

            # 格式化日期
            dates = pd.to_datetime(
                group_sorted['date'].astype(str).str.replace(r'[-/.]', '', regex=True),
                format='%Y%m%d',
                errors='coerce'
            ).dt.strftime('%Y-%m-%d').tolist()

            values = group_sorted[['open', 'close', 'low', 'high']].values
            volumes = group_sorted['volume'].tolist()

            return self.save_stock_data(code, name, dates, values, volumes)

        total_stocks = len(grouped)
        saved_count = 0

        # 使用多线程
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(save_group, code, group): code
                      for code, group in grouped}

            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                    saved_count += 1
                    if (i + 1) % 50 == 0:
                        logger.info("Progress: {}/{} stocks saved".format(i + 1, total_stocks))
                except Exception as e:
                    code = futures[future]
                    logger.error("Error saving stock {}: {}".format(code, e))

        logger.info("Split complete! {} stock files saved to {}".format(saved_count, self.output_dir))

        # 计算存储节省
        self.calculate_savings(df)

    def calculate_savings(self, df):
        """计算存储节省"""
        # 估算原始数据大小
        df['date_formatted'] = pd.to_datetime(
            df['date'].astype(str).str.replace(r'[-/.]', '', regex=True),
            format='%Y%m%d',
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')

        # 计算各股票K线条数
        bar_counts = df.groupby('symbol').size()

        # 假设原始数据未压缩
        original_size = sum(bar_counts * 100)  # 每条K线约100字节（估算）
        original_size_mb = original_size / (1024 * 1024)

        # 计算实际文件大小
        actual_size = 0
        for file in Path(self.output_dir).glob('*.json'):
            actual_size += file.stat().st_size
        actual_size_mb = actual_size / (1024 * 1024)

        savings = original_size_mb - actual_size_mb
        savings_pct = (savings / original_size_mb * 100) if original_size_mb > 0 else 0

        logger.info("Storage analysis:")
        logger.info("  Estimated original size: {:.2f} MB".format(original_size_mb))
        logger.info("  Actual file size: {:.2f} MB".format(actual_size_mb))
        logger.info("  Savings: {:.2f} MB ({:.1f}%)".format(savings, savings_pct))


def main():
    import sys

    parquet_file = 'data/stock_daily_latest.parquet'
    output_dir = 'data/kline_split'

    if len(sys.argv) > 1:
        parquet_file = sys.argv[1]

    if len(sys.argv) > 2:
        output_dir = sys.argv[2]

    # 检查输入文件
    if not os.path.exists(parquet_file):
        logger.error("Parquet file not found: {}".format(parquet_file))
        logger.info("Run 'python main.py fetch' first to download data")
        sys.exit(1)

    # 执行拆分
    splitter = KlineDataSplitter(parquet_file, output_dir)
    df = splitter.load_parquet()
    splitter.split_data(df)

    logger.info("\nNext steps:")
    logger.info("1. K-line split files are in: {}/".format(output_dir))
    logger.info("2. Index file: {}/index.json".format(output_dir))
    logger.info("3. Update HTML to use kline_loader.js for on-demand loading")


if __name__ == '__main__':
    main()
