
import requests
import json
import time
import random
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=10, backoff_factor=2):
    """
    重试装饰器，带指数退避
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    # 如果返回结果包含错误信息，也进行重试
                    if isinstance(result, dict) and 'error' in result:
                        retries += 1
                        if retries >= max_retries:
                            return result
                        # 指数退避延迟
                        delay = min(base_delay * (backoff_factor ** retries) + random.uniform(0, 1), max_delay)
                        time.sleep(delay)
                        continue
                    return result
                except requests.exceptions.RequestException as e:
                    retries += 1
                    if retries >= max_retries:
                        return {'error': f'网络请求异常: {str(e)}'}
                    # 指数退避延迟
                    delay = min(base_delay * (backoff_factor ** retries) + random.uniform(0, 1), max_delay)
                    time.sleep(delay)
                except Exception as e:
                    # 对于非请求异常，不重试
                    return {'error': f'异常: {str(e)}'}
            return {'error': f'达到最大重试次数 {max_retries}'}
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3, base_delay=1, max_delay=10)
def get_stock_concepts(code):
    """
    获取股票的概念题材、行业和地域信息。

    Args:
        code (str): 股票代码，格式可以是 '300059', 'sz.300059', 'sz300059' 等

    Returns:
        dict: 包含概念、行业、地域的字典，如果失败返回错误信息
    """
    # 1. 格式化代码
    # 移除所有非数字字符，保留纯数字代码
    import re
    clean_code = re.sub(r'\D', '', code)

    # 2. 推断市场 (secid)
    # 0: 深证 (300, 000, 002, 200), 1: 上证 (600, 601, 603, 688, 900)
    # 北交所 (8xx, 4xx) 通常也是 0 (东财接口规则)
    if clean_code.startswith('6') or clean_code.startswith('9'):
        secid = f"1.{clean_code}"
    elif clean_code.startswith('8') or clean_code.startswith('4'):
        secid = f"0.{clean_code}" # 北交所通常在东财是 0
    else:
        secid = f"0.{clean_code}"

    url = "http://push2.eastmoney.com/api/qt/stock/get"
    # f58: 名称
    # f127: 行业
    # f128: 地域
    # f129: 概念板块 (逗号分隔)
    params = {
        "secid": secid,
        "fields": "f58,f127,f128,f129"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # 设置 proxies 为 None 或空字典以避免使用系统代理
        res = requests.get(url, params=params, headers=headers, timeout=10, proxies={})
        if res.status_code == 200:
            data = res.json()
            if data and 'data' in data and data['data']:
                stock_data = data['data']

                # 解析概念
                concepts_str = stock_data.get('f129', '')
                concepts = concepts_str.split(',') if concepts_str else []
                # 过滤掉一些无意义的标签 (如 "HS300_", "深股通" 等，可视情况保留)
                # 这里暂时全部保留

                return {
                    'code': clean_code,
                    'name': stock_data.get('f58', 'Unknown'),
                    'industry': stock_data.get('f127', ''),
                    'area': stock_data.get('f128', ''),
                    'concepts': concepts
                }
            else:
                return {'error': '未找到数据'}
        else:
            return {'error': f'请求失败: {res.status_code}'}

    except requests.exceptions.Timeout:
        return {'error': '请求超时'}
    except requests.exceptions.ConnectionError:
        return {'error': '连接错误'}
    except requests.exceptions.RequestException as e:
        return {'error': f'请求异常: {str(e)}'}
    except ValueError:  # JSON decode error
        return {'error': '响应数据格式错误'}
    except Exception as e:
        return {'error': f'异常: {str(e)}'}

if __name__ == "__main__":
    # 测试
    print(get_stock_concepts("300059"))
    print(get_stock_concepts("sz.300750"))
