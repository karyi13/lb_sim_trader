import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_kline_data(input_file='stock_daily_latest.parquet', output_file='kline_data.js'):
    """
    生成 K 线数据文件，用于 HTML 可视化
    使用向量化操作提高性能
    """
    logger.info(f"Loading data from {input_file}...")
    df = pd.read_parquet(input_file)
    
    # 获取所有股票的代码
    unique_symbols = df['symbol'].unique()
    
    logger.info(f"Generating K-line data for {len(unique_symbols)} stocks...")
    
    # 使用 groupby 批量处理
    df_sorted = df.sort_values(['symbol', 'date'])
    
    # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
    df_sorted['date_formatted'] = pd.to_datetime(df_sorted['date']).dt.strftime('%Y-%m-%d')
    
    # 按股票分组
    grouped = df_sorted.groupby('symbol')
    
    kline_data = {}
    
    for i, (symbol, group) in enumerate(grouped):
        kline_data[symbol] = {
            'name': group['name'].iloc[0],
            'dates': group['date_formatted'].tolist(),
            'values': group[['open', 'close', 'low', 'high']].values.tolist(),
            'volumes': group['volume'].tolist()
        }
        
        if (i + 1) % 1000 == 0:
            logger.info(f"Processed {i + 1}/{len(unique_symbols)} stocks...")
    
    # 保存为 JS 文件
    js_content = f"window.KLINE_DATA_GLOBAL = {json.dumps(kline_data, ensure_ascii=False)};"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    logger.info(f"Saved K-line data to {output_file}")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

if __name__ == "__main__":
    generate_kline_data()
