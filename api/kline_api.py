"""
K线数据API - 按需加载单个股票的K线数据
"""
import os
import json
import pandas as pd
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)

# Configuration
PARQUET_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'stock_daily_latest.parquet')
LADDER_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'limit_up_ladder.parquet')

# Cache for loaded data to avoid repeated disk reads
def load_parquet_file(file_path):
    """Load parquet file with caching."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()
    return pd.read_parquet(file_path)

# Load data once on startup
_df_global = None
_ladder_global = None

def get_df():
    global _df_global
    if _df_global is None:
        logger.info(f"Loading parquet data from {PARQUET_FILE}")
        _df_global = load_parquet_file(PARQUET_FILE)
        logger.info(f"Loaded {len(_df_global)} records")
    return _df_global

def get_ladder_df():
    global _ladder_global
    if _ladder_global is None:
        logger.info(f"Loading ladder data from {LADDER_FILE}")
        _ladder_global = load_parquet_file(LADDER_FILE)
        logger.info(f"Loaded {len(_ladder_global)} ladder records")
    return _ladder_global


@app.route('/api/health')
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'records': len(get_df()),
        'ladder_records': len(get_ladder_df())
    })


@app.route('/api/kline/<code>')
def get_kline_data(code):
    """Get K-line data for a specific stock code.

    Args:
        code: Stock code (e.g., "000001.SZ")

    Returns:
        JSON with K-line data: {dates: [], values: [], volumes: [], name: ""}
    """
    try:
        code = code.upper()

        # Get stock data
        df = get_df()

        # Filter for this stock
        stock_df = df[df['symbol'] == code]

        if stock_df.empty:
            return jsonify({'error': f'Stock {code} not found'}), 404

        # Get stock name
        name = stock_df['name'].iloc[0] if 'name' in stock_df.columns else ''

        # Sort by date
        stock_df = stock_df.sort_values('date')

        # Format dates
        stock_df['date_formatted'] = pd.to_datetime(
            stock_df['date'].astype(str).str.replace(r'[-/.]', '', regex=True),
            format='%Y%m%d',
            errors='coerce'
        ).dt.strftime('%Y-%m-%d')

        # Extract K-line data
        result = {
            'code': code,
            'name': name,
            'dates': stock_df['date_formatted'].tolist(),
            'values': stock_df[['open', 'close', 'low', 'high']].values.tolist(),
            'volumes': stock_df['volume'].tolist()
        }

        logger.info(f"Returning K-line data for {code}: {len(result['dates'])} bars")
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting K-line data for {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/kline/batch', methods=['POST'])
def get_kline_batch():
    """Get K-line data for multiple stocks.

    Request body: {"codes": ["000001.SZ", "000002.SZ"]}

    Returns:
        JSON with K-line data for all requested stocks
    """
    try:
        data = request.get_json()
        codes = data.get('codes', [])

        if not codes:
            return jsonify({'error': 'No codes provided'}), 400

        if len(codes) > 50:
            return jsonify({'error': 'Maximum 50 codes allowed per request'}), 400

        df = get_df()

        # Filter for requested stocks
        stock_df = df[df['symbol'].isin(codes)]

        if stock_df.empty:
            return jsonify({}), 200

        result = {}
        for code in codes:
            code_data = stock_df[stock_df['symbol'] == code]
            if code_data.empty:
                continue

            name = code_data['name'].iloc[0] if 'name' in code_data.columns else ''
            code_df = code_data.sort_values('date')
            code_df['date_formatted'] = pd.to_datetime(
                code_df['date'].astype(str).str.replace(r'[-/.]', '', regex=True),
                format='%Y%m%d',
                errors='coerce'
            ).dt.strftime('%Y-%m-%d')

            result[code] = {
                'name': name,
                'dates': code_df['date_formatted'].tolist(),
                'values': code_df[['open', 'close', 'low', 'high']].values.tolist(),
                'volumes': code_df['volume'].tolist()
            }

        logger.info(f"Batch K-line data for {len(result)}/{len(codes)} stocks")
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in batch get K-line: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ladder')
def get_ladder_data():
    """Get ladder data for all dates.

    Returns:
        JSON with ladder data grouped by date
    """
    try:
        df = get_ladder_df()

        if df.empty:
            return jsonify({})

        # Calculate next day opening change (vectorized)
        df_sorted = df.sort_values(['symbol', 'date']).reset_index(drop=True)
        df_sorted['next_open'] = df_sorted.groupby('symbol')['open'].shift(-1)
        df_sorted['next_day_open_change_pct'] = (
            (df_sorted['next_open'] - df_sorted['close']) / df_sorted['close'] * 100
        ).round(2)

        # Handle cases where next_open is NaN (last trading day)
        df_sorted['next_day_open_change_pct'] = df_sorted['next_day_open_change_pct'].fillna(0)

        # Group by date and consecutive days
        all_dates_data = {}

        unique_dates = sorted(df_sorted['date'].unique(), reverse=True)

        for date in unique_dates:
            date_data = df_sorted[df_sorted['date'] == date].copy()

            grouped = date_data.groupby('consecutive_limit_up_days').apply(
                lambda x: x[['symbol', 'name', 'close', 'consecutive_limit_up_days', 'concept_themes', 'next_day_open_change_pct']].to_dict('records')
            ).to_dict()

            formatted_data = {}
            for level, stocks in grouped.items():
                formatted_data[str(level)] = []
                for stock in stocks:
                    concepts = stock['concept_themes']
                    if hasattr(concepts, 'tolist'):
                        concepts = concepts.tolist()
                    elif not isinstance(concepts, list):
                        concepts = list(concepts) if concepts else []

                    formatted_data[str(level)].append({
                        'code': stock['symbol'],
                        'name': stock['name'],
                        'price': float(stock['close']),
                        'limitUpDays': int(stock['consecutive_limit_up_days']),
                        'conceptThemes': concepts,
                        'nextDayOpenChangePct': float(stock.get('next_day_open_change_pct', 0))
                    })

            all_dates_data[date] = formatted_data

        return jsonify(all_dates_data)

    except Exception as e:
        logger.error(f"Error getting ladder data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ladder/<date>')
def get_ladder_by_date(date):
    """Get ladder data for a specific date.

    Args:
        date: Date in YYYYMMDD format

    Returns:
        JSON with ladder data for the specified date
    """
    try:
        all_ladder = get_ladder_data()
        ladder_json = all_ladder.get_json()

        if date not in ladder_json:
            return jsonify({'error': f'Date {date} not found'}), 404

        return jsonify({date: ladder_json[date]})

    except Exception as e:
        logger.error(f"Error getting ladder data for {date}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stocks')
def get_stock_list():
    """Get list of all stocks.
    Returns:
        JSON with stock codes and names from ladder data
    """
    try:
        df = get_ladder_df()

        if df.empty:
            return jsonify([])

        # Get unique stocks
        stocks = df[['symbol', 'name']].drop_duplicates().sort_values('symbol')

        result = []
        for _, row in stocks.iterrows():
            result.append({
                'code': row['symbol'],
                'name': row['name']
            })

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting stock list: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Pre-load data
    get_df()
    get_ladder_df()

    # Start server
    logger.info("Starting K-line API server on http://localhost:5000")
    app.run(host='127.0.0.1', port=5000, debug=True)
