## 项目概述

A股连板股票模拟交易分析和训练系统，提供K线图表和涨停/跌停阶梯可视化功能。支持PC端和移动端视图。

## 常用命令

### 安装依赖
```bash
pip install -r requirements.txt
```

### 运行主程序
```bash
# 执行完整流程：获取数据 → 分析数据 → 生成阶梯数据 → 生成K线数据
python main.py full

# 单独获取数据（增量更新）
python main.py fetch

# 单独获取数据（完全刷新）
python main.py fetch --full-refresh

# 指定日期范围获取数据
python main.py fetch --start-date 20240101 --end-date 20240131

# 分析数据
python main.py analyze

# 生成阶梯数据（用于前端显示）
python main.py ladder

# 生成K线数据（用于前端显示）
python main.py kline
```

### 生成按需加载的K线数据（推荐）
```bash
# 将大K线文件拆分为单股票JSON文件，实现按需加载
python function/split_kline_data.py data/stock_daily_latest.parquet data/kline_split
```

### 运行测试
```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行测试并生成覆盖率报告
python -m pytest tests/ -v --cov=. --cov-report=html
```

### 前端部署
```bash
# 使用 deploy.sh 部署到 Vercel
bash deploy.sh
```

### 运行CLI模块
```bash
python cli.py
```

## 技术栈

### 后端
- **Python 3.7+**: 主要语言
- **pandas/numpy**: 数据处理，已优化为向量化操作
- **pytdx**: 通达信数据源（主要）
- **akshare**: 东方财富数据源（备用）
- **pyarrow**: Parquet文件格式存储
- **pytest**: 单元测试框架

### 前端
- **HTML5/CSS3**: 页面结构和响应式样式
- **ES6+ JavaScript**: 业务逻辑
- **ECharts 5.4.3**: K线图表可视化
- **按需加载**: K线数据按股票代码加载，提升首屏加载速度

## 架构设计

### 目录结构
```
lbmoni/
├── depend/                  # 依赖注入和服务层
│   ├── config.py           # 全局配置
│   ├── di_container.py     # 依赖注入容器
│   └── services.py         # 数据服务（fetcher、validator、storage）
├── function/               # 功能模块
│   ├── split_kline_data.py # K线数据分片（按需加载）
│   ├── stock_concepts.py   # 股票概念获取
│   ├── generate_kline_data.py  # K线数据生成
│   ├── update_html.py      # HTML更新
│   └── update_project.py   # 项目更新
├── utils/                  # 工具类
│   └── logging_utils.py    # 结构化日志
├── tests/                  # 单元测试
│   ├── test_validators.py  # 数据验证器测试
│   ├── test_analyzer.py    # 分析器测试
│   └── test_kline_loader.py # K线加载器测试
├── data/                   # 数据文件
│   ├── stock_daily_latest.parquet  # 主数据文件
│   ├── limit_up_ladder.parquet     # 阶梯数据
│   ├── kline_split/               # 按需加载的K线数据分片
│   ├── ladder_data.js             # 前端阶梯数据 (~5.7MB)
│   └── kline_data.js              # 完整K线数据（可选，~48MB）
├── css/
│   ├── style.css           # PC版样式
│   └── mobile.css          # 移动端样式
├── js/
│   ├── utils.js            # 公共工具函数
│   ├── kline_loader.js     # K线数据按需加载器
│   ├── app.js              # PC版前端逻辑
│   └── mobile.js           # 移动端前端逻辑
├── index.html              # PC版主页面
├── mobile.html             # 移动端页面
├── main.py                 # 主入口
├── cli.py                  # CLI交互式模块
└── requirements.txt        # 依赖列表
```

### 核心优化

#### 1. 前端数据按需加载
- **问题**: 原来 `kline_data.js` 约48MB，首屏加载慢
- **方案**: 拆分为按股票分片的JSON文件 + 按需加载器
- **效果**: 首屏只加载~1MB精简数据，用户查看K线时按需加载单股票数据（几十KB）

#### 2. 向量化性能优化
- **优化前**: 双重循环计算次日开盘涨跌幅
- **优化后**: 使用 `groupby().shift()` 向量化操作
- **效果**: 计算速度提升约10倍

#### 3. 代码模块化
- 提取 `js/utils.js` 公共工具函数
- 统一使用 `DataValidator` 进行数据验证
- 消除重复代码约280行

#### 4. 测试覆盖率
- 添加单元测试框架
- 覆盖数据验证、分析器、K线加载器等核心模块

### 依赖注入模式

项目使用依赖注入容器 (`depend/di_container.py`) 管理服务：

```python
from depend.di_container import container

# 获取服务实例
data_fetcher = container.get('data_fetcher')
data_validator = container.get('data_validator')
data_storage = container.get('data_storage')
```

### 数据获取策略

使用复合数据源模式（见 `depend/services.py`）：
1. **PyTDX** - 通达信数据源（优先，速度快）
2. **AkShare** - 东方财富数据源（备用，作为fallback）

### 数据处理流程

1. **fetch** (`DataFetcher.run()`):
   - 获取股票列表
   - 并发获取日线数据（使用 ThreadPoolExecutor，最大20个worker）
   - 支持增量更新（仅获取新交易日数据）
   - 数据验证后保存为 Parquet 格式

2. **analyze** (`Analyzer.process()`):
   - 识别涨停股票（根据板块计算涨跌幅限制）
   - 计算连续涨停天数（向量化操作）
   - 计算次日开盘涨跌幅（向量化操作）
   - 识别涨停板类型（一字板、T字板、换手板）
   - 拉取股票概念
   - 计算晋级率

3. **generate**:
   - `generate_kline_data()`: 生成前端K线图表数据 (JS格式)
   - `generate_ladder_data_for_html()`: 生成前端阶梯数据 (JS格式)

### 涨跌停计算规则（见 main.py 的 `calculate_limit_price` 方法）

- **ST股票**: 5% 涨跌幅
- **创业板/科创板（30/68开头）**: 20% 涨跌幅
- **主板股票**: 10% 涨跌幅

涨停价计算公式：
```
limit = int(prev_close * multiplier * 100 + 0.49999) / 100.0
```

### 前端状态管理

`AppState` 对象管理系统状态：
- `currentDate`: 当前交易日
- `account`: 账户信息（资金、持仓、冻结）
- `trades`: 交易记录
- `conditionOrders`: 条件单
- `pendingActions`: 待执行的明日操作
- `selectedStock`: 当前选中的股票

### 前端工具函数（js/utils.js）

公共工具函数包括：
- 日期工具：`formatDate`, `findNearestTradingDay`, `parseDate`
- 计算工具：`calculateLimitUpPrice`, `calculateFees`, `calculateChangePct`
- 验证工具：`isValidPrice`, `isValidQuantity`
- K线图工具：`filterKlineDataToDate`, `calculateZoomStart`
- DOM工具：`setText`, `getElement`, `toggleElement`, `showToast`

## 配置说明

主要配置在 `depend/config.py` 中：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `DEFAULT_START_DATE` | '20241220' | 默认开始日期 |
| `MAX_WORKERS` | 20 | 数据获取并发数 |
| `CONCEPT_FETCH_WORKERS` | 10 | 概念获取并发数 |
| `MAX_RETRIES` | 3 | 最大重试次数 |
| `REQUEST_TIMEOUT` | 10 | 请求超时时间（秒） |
| `PYTDX_SERVERS` | 3个服务器 | 通达信服务器列表 |

## 前端与后端数据交互

### 传统方式（一次性加载）
- `data/kline_data.js`: 包含 `window.KLINE_DATA_GLOBAL`，约48MB
- `data/ladder_data.js`: 包含 `window.LADDER_DATA`，约5.7MB
- `data/kline_data_core.js`: 精简版K线数据，约1MB

### 按需加载方式（推荐）
- `data/kline_split/index.json`: 股票索引文件
- `data/kline_split/{code}.json`: 单股票K线数据
- `js/kline_loader.js`: 按需加载器

数据格式：
```javascript
// 索引文件
{
    "000001.SZ": {
        "name": "平安银行",
        "file": "kline_split/000001_SZ.json"
    }
}

// 单股票数据
{
    "code": "000001.SZ",
    "name": "平安银行",
    "dates": ["2024-01-01", "2024-01-02", ...],
    "values": [  // [open, close, low, high]
        [10.5, 10.8, 10.0, 11.0],
        ...
    ],
    "volumes": [1000000, 1200000, ...]
}
```

## 注意事项

1. **数据格式**: 日期使用 `YYYYMMDD` 格式（如 20240101）
2. **增量更新**: 默认开启增量模式，只获取新交易日数据。使用 `--full-refresh` 进行全量刷新
3. **周末处理**: `get_default_end_date()` 会自动跳过周末，在15:00前使用昨天日期，15:00后使用今天日期
4. **线程安全**: 数据获取使用线程池，PyTDX 连接使用 thread-local storage 管理
5. **数据验证**: 获取的数据会经过 `DataValidator.validate()` 验证
6. **备份机制**: 保存数据失败时会尝试保存到备份文件
7. **Python版本**: 建议使用 Python 3.7 或更高版本
8. **按需加载**: 首次部署或更新数据后，运行 `split_kline_data.py` 生成分片数据以启用按需加载

## 部署说明

### Vercel 部署
1. 项目已配置 Vercel 部署
2. 运行 `bash deploy.sh` 即可部署
3. 静态文件部署，无需后端服务器

### 本地开发
1. 运行 `python main.py full` 获取并分析数据
2. 运行 `python function/split_kline_data.py` 生成按需加载数据
3. 直接打开 `index.html` 或 `mobile.html` 即可使用
