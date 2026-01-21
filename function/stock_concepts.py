
import requests
import json
import time
import random
import re
from functools import wraps
from typing import List, Dict, Optional

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=10, backoff_factor=2):
    """重试装饰器，带指数退避"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    if isinstance(result, dict) and 'error' in result:
                        retries += 1
                        if retries >= max_retries:
                            return result
                        delay = min(base_delay * (backoff_factor ** retries) + random.uniform(0, 1), max_delay)
                        time.sleep(delay)
                        continue
                    return result
                except requests.exceptions.RequestException:
                    retries += 1
                    if retries >= max_retries:
                        return {'error': '网络请求异常'}
                    delay = min(base_delay * (backoff_factor ** retries) + random.uniform(0, 1), max_delay)
                    time.sleep(delay)
                except Exception as e:
                    return {'error': f'异常: {str(e)}'}
            return {'error': f'达到最大重试次数 {max_retries}'}
        return wrapper
    return decorator


@retry_with_backoff(max_retries=3, base_delay=1, max_delay=10)
def get_stock_concepts_ths(code: str) -> Dict:
    """
    使用同花顺API获取股票的概念题材、行业和地域信息。

    Args:
        code (str): 股票代码，格式可以是 '300059', '900059'

    Returns:
        dict: 包含概念、行业、地域的字典
    """
    # 清理代码，获取纯数字
    clean_code = re.sub(r'\D', '', code)
    if not clean_code or len(clean_code) != 6:
        return {'error': f'无效的股票代码: {code}'}

    # 同花顺概念API
    url = f"https://basic.10jqka.com.cn/api/{clean_code}/concept"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": f"https://basic.10jqka.com.cn/{clean_code}/",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10, proxies={})
        if res.status_code == 200:
            data = res.json()

            # 同花顺返回格式示例:
            # {
            #   "data": {
            #     "concept": [{"name": "新能源", "code": "BK0485"}, ...],
            #     "industry": {"name": "光伏设备", "code": "BK0030"},
            #     "area": {"name": "江苏"}
            #   }
            # }

            if data and 'data' in data:
                result = {
                    'code': clean_code,
                    'name': '',
                    'industry': '',
                    'area': '',
                    'concepts': []
                }

                stock_data = data['data']

                # 概念板块
                if 'concept' in stock_data and stock_data['concept']:
                    concepts = [c.get('name', '') for c in stock_data['concept'] if c.get('name')]
                    result['concepts'] = concepts

                # 行业
                if 'industry' in stock_data and stock_data['industry']:
                    result['industry'] = stock_data['industry'].get('name', '')

                # 地域
                if 'area' in stock_data and stock_data['area']:
                    result['area'] = stock_data['area'].get('name', '')

                # 如果没有名称，尝试从行业或概念推断
                if not result['name'] and result['industry']:
                    result['name'] = clean_code

                return result
            else:
                return {'error': '未找到数据'}
        else:
            return {'error': f'请求失败: {res.status_code}'}

    except requests.exceptions.Timeout:
        return {'error': '请求超时'}
    except requests.exceptions.ConnectionError:
        return {'error': '连接错误'}
    except Exception as e:
        return {'error': f'异常: {str(e)}'}


@retry_with_backoff(max_retries=3, base_delay=1, max_delay=10)
def get_stock_concepts_ths_html(code: str) -> Dict:
    """
    使用同花顺HTML页面解析获取概念信息（备用方案）

    Args:
        code (str): 股票代码

    Returns:
        dict: 包含概念、行业、地域的字典
    """
    clean_code = re.sub(r'\D', '', code)
    if not clean_code or len(clean_code) != 6:
        return {'error': f'无效的股票代码: {code}'}

    url = f"https://basic.10jqka.com.cn/{clean_code}/concept.html"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10, proxies={})
        if res.status_code == 200:
            html = res.text

            # 解析概念
            concepts = []
            concept_pattern = r'<a[^>]*concept/([^/]+)"[^>]*>([^<]+)</a>'
            matches = re.findall(concept_pattern, html)
            if matches:
                concepts = [name for code, name in matches if name]

            # 解析行业
            industry = ''
            industry_pattern = r'<a[^>]*industry/([^/]+)"[^>]*>([^<]+)</a>'
            industry_matches = re.findall(industry_pattern, html)
            if industry_matches:
                industry = industry_matches[0][1] if industry_matches[0][1] else ''

            # 解析地域
            area = ''
            area_pattern = r'<span[^>]*class="(area|region)"[^>]*>([^<]+)</span>'
            area_match = re.search(area_pattern, html)
            if area_match:
                area = area_match.group(2)

            return {
                'code': clean_code,
                'name': '',
                'industry': industry,
                'area': area,
                'concepts': concepts
            }
        else:
            return {'error': f'请求失败: {res.status_code}'}

    except Exception as e:
        return {'error': f'异常: {str(e)}'}


@retry_with_backoff(max_retries=3, base_delay=1, max_delay=10)
def get_stock_concepts_em(code: str) -> Dict:
    """
    使用东方财富API获取概念信息（备用方案）

    Args:
        code (str): 股票代码

    Returns:
        dict: 包含概念、行业、地域的字典
    """
    clean_code = re.sub(r'\D', '', code)

    if clean_code.startswith('6') or clean_code.startswith('9'):
        secid = f"1.{clean_code}"
    elif clean_code.startswith('8') or clean_code.startswith('4'):
        secid = f"0.{clean_code}"
    else:
        secid = f"0.{clean_code}"

    url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f58,f127,f128,f129"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=10, proxies={})
        if res.status_code == 200:
            data = res.json()
            if data and 'data' in data and data['data']:
                stock_data = data['data']
                concepts_str = stock_data.get('f129', '')
                concepts = concepts_str.split(',') if concepts_str else []
                return {
                    'code': clean_code,
                    'name': stock_data.get('f58', ''),
                    'industry': stock_data.get('f127', ''),
                    'area': stock_data.get('f128', ''),
                    'concepts': concepts
                }
            else:
                return {'error': '未找到数据'}
        else:
            return {'error': f'请求失败: {res.status_code}'}
    except Exception as e:
        return {'error': f'异常: {str(e)}'}


def get_stock_concepts(code: str) -> Dict:
    """
    获取股票的概念题材、行业和地域信息。
    优先使用同花顺API，失败则回退到东方财富API。

    Args:
        code (str): 股票代码，格式可以是 '300059', 'sz.300059', 'sz300059' 等

    Returns:
        dict: 包含概念、行业、地域的字典
    """
    clean_code = re.sub(r'\D', '', code)

    # 优先尝试同花顺API
    result = get_stock_concepts_ths(clean_code)

    # 如果失败，回退到东方财富
    if 'error' in result:
        result = get_stock_concepts_em(clean_code)

    return result


def get_concept_stocks(concept_name: str) -> Optional[List[Dict]]:
    """
    获取指定概念板块的所有成分股

    Args:
        concept_name (str): 概念名称，如 "新能源"

    Returns:
        list: 成分股列表 [{"code": "300059", "name": "东方财富"}, ...]
    """
    # 同花顺概念板块API
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    # 概念代码映射 (部分常用概念)
    concept_map = {
        "新能源": "BK0485",
        "人工智能": "BK0505",
        "芯片": "BK0453",
        "半导体": "BK0453",
        "医药": "BK0042",
        "医疗器械": "BK0364",
        "军工": "BK0475",
        "新能源汽车": "BK0947",
        "锂电池": "BK0523",
        "光伏": "BK0029",
        "风电": "BK0495",
        "储能": "BK0907",
        "工业母机": "BK1000",
        "机器人": "BK0893",
        "低空经济": "BK1047",
        "算力": "BK1139",
    }

    concept_code = concept_map.get(concept_name)
    if not concept_code:
        return None

    params = {
        "sortColumns": "CHANGE_RATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPTA_APP_BOARD_CONCEPT",
        "columns": "ALL",
        "filter": f'(BOARD_CODE="{concept_code}")',
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=10, proxies={})
        if res.status_code == 200:
            data = res.json()
            if data and 'result' in data and 'data' in data['result']:
                stocks = []
                for item in data['result']['data']:
                    stocks.append({
                        'code': item.get('SECURITY_CODE', ''),
                        'name': item.get('SECURITY_NAME_ABBR', ''),
                        'price': item.get('NEW_PRICE', 0),
                        'change_pct': item.get('CHANGE_RATE', 0)
                    })
                return stocks
    except Exception as e:
        print(f"获取概念股票失败: {e}")

    return None


if __name__ == "__main__":
    # 测试
    print("=== 测试同花顺API ===")
    print("测试股票 300059 (东方财富):")
    result = get_stock_concepts("300059")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("\n测试股票 600519 (贵州茅台):")
    result = get_stock_concepts("600519")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("\n测试股票 000651 (格力电器):")
    result = get_stock_concepts("000651")
    print(json.dumps(result, ensure_ascii=False, indent=2))
