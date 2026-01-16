/**
 * 连板股票模拟交易训练系统 - 核心逻辑
 */

// ==================== K线数据获取辅助函数 ====================
// 优先使用精简版数据，如果找不到则尝试完整数据（加载后可用）
function getKlineData(code) {
    // 先查精简版
    const coreData = window.KLINE_DATA_CORE?.[code];
    if (coreData) return coreData;
    // 再查完整版
    const globalData = window.KLINE_DATA_GLOBAL?.[code];
    return globalData || null;
}

// 获取所有K线数据（用于检查）
function getAllKlineData() {
    const core = window.KLINE_DATA_CORE || {};
    const global = window.KLINE_DATA_GLOBAL || {};
    // 合并数据
    return { ...global, ...core }; // global 后加载，优先级更高
}

// 检查股票数据是否存在
function hasKlineData(code) {
    code = code.toUpperCase();
    return getKlineData(code) !== null;
}

// ==================== 全局状态管理 ====================
const AppState = {
    // 当前交易日
    currentDate: null,
    availableDates: [],

    // 用户账户状态
    account: {
        initialFund: 100000,      // 初始资金
        available: 100000,        // 可用资金
        positions: {},            // 持仓 {code: {quantity, cost, buyDate}}
        frozen: 0,                // 冻结资金
    },

    // 交易记录
    trades: [],

    // 条件单
    conditionOrders: [],

    // 待执行的明日操作
    pendingActions: [],

    // 当前选中的股票
    selectedStock: null,

    // K线图实例
    klineChart: null,

    // K线图缩放状态
    klineZoom: null,

    // 是否处于模拟的"明天"
    isNextDay: false,
};

// ==================== 常量配置 ====================
const CONFIG = {
    // 交易费率
    FEES: {
        STAMP_TAX: 0.001,      // 印花税 (卖出时收取)
        COMMISSION: 0.0003,    // 佣金 (双向收取, 最低5元)
        MIN_COMMISSION: 5,     // 最低佣金
    },

    // 涨跌停幅度
    LIMIT_UP: 0.10,           // 普通股票涨停
    LIMIT_DOWN: 0.10,         // 普通股票跌停

    // ST股票涨跌停
    ST_LIMIT: 0.05,

    // 交易单位
    TRADING_UNIT: 100,
};

// ==================== 初始化 ====================
document.addEventListener('DOMContentLoaded', () => {
    initApp();
});

async function initApp() {
    try {
        // 初始化主题
        initTheme();

        // 等待数据加载
        await waitForData();

        // 初始化日期选择器
        initDateSelector();

        // 加载保存的数据（这可能会覆盖日期选择器中设置的日期）
        loadSavedData();

        // 渲染连板列表（使用最终确定的日期）
        renderLadderList();

        // 初始化K线图
        initKlineChart();

        // 初始化事件监听
        initEventListeners();

        // 初始化标签页
        initTabs();

        // 更新账户信息
        updateAccountInfo();
        renderMyPositions();

        console.log('模拟交易系统初始化完成');
    } catch (error) {
        console.error('初始化失败:', error);
        showToast('系统初始化失败: ' + error.message, 'error');
    }
}

// 等待数据加载
function waitForData() {
    return new Promise((resolve, reject) => {
        const checkData = () => {
            if (window.LADDER_DATA && window.KLINE_DATA_CORE) {
                resolve();
            } else {
                setTimeout(checkData, 100);
            }
        };
        // 超时检测
        setTimeout(() => reject(new Error('数据加载超时')), 10000);
        checkData();
    });
}

// ==================== 日期管理 ====================
// 全局函数：导航到上一日
window.goToPrevDay = function() {
    const idx = AppState.availableDates.indexOf(AppState.currentDate);
    if (idx > 0) {
        switchDate(AppState.availableDates[idx - 1]);
    }
};

// 全局函数：导航到下一日
window.goToNextDay = function() {
    const idx = AppState.availableDates.indexOf(AppState.currentDate);
    if (idx < AppState.availableDates.length - 1) {
        switchDate(AppState.availableDates[idx + 1]);
    }
};

function initDateSelector() {
    const dates = Object.keys(window.LADDER_DATA).sort();
    AppState.availableDates = dates;

    // 默认选择最接近今天的真实交易日
    // 如果今天是交易日且数据存在，则选择今天；否则选择最近的交易日
    AppState.currentDate = findNearestTradingDay(dates);

    const select = document.getElementById('currentDate');
    dates.forEach(date => {
        const option = document.createElement('option');
        option.value = date;
        option.textContent = formatDate(date);
        select.appendChild(option);
    });

    select.value = AppState.currentDate;

    // 日期切换事件
    select.addEventListener('change', (e) => {
        switchDate(e.target.value);
    });

    // 上一日/下一日按钮 - 直接调用全局函数
    const prevBtn = document.getElementById('prevDay');
    const nextBtn = document.getElementById('nextDay');

    if (prevBtn) {
        prevBtn.onclick = window.goToPrevDay;
    }
    if (nextBtn) {
        nextBtn.onclick = window.goToNextDay;
    }
}

// 查找最接近今天的交易日
function findNearestTradingDay(availableDates) {
    // 获取今天的日期（格式：YYYYMMDD）
    const today = new Date();
    const todayStr = formatDateForComparison(today);

    // 检查今天是否在可用数据中
    if (availableDates.includes(todayStr)) {
        return todayStr;
    }

    // 如果今天不在数据中，查找最接近的日期
    // 优先查找今天之前的日期
    for (let i = availableDates.length - 1; i >= 0; i--) {
        if (availableDates[i] <= todayStr) {
            return availableDates[i];
        }
    }

    // 如果没有今天之前的日期，返回最早的一天
    return availableDates[0];
}

// 将Date对象格式化为YYYYMMDD字符串
function formatDateForComparison(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
}

function switchDate(newDate) {
    // 执行待执行的条件单和操作
    executePendingOrders(newDate);

    AppState.currentDate = newDate;
    document.getElementById('currentDate').value = newDate;
    document.getElementById('currentDateBottom').textContent = formatDate(newDate);

    // 渲染连板列表
    renderLadderList();

    // 如果有选中股票，更新K线图和交易面板
    if (AppState.selectedStock) {
        updateStockPanel(AppState.selectedStock);
    }

    // 更新持仓信息
    updatePositionsInfo();

    // 更新明日操作预览
    renderNextDayActions();

    // 更新右侧持仓列表
    renderMyPositions();

    // 更新账户信息
    updateAccountInfo();

    // 更新交易记录（重要！订单执行后需要刷新显示）
    renderTradeRecords();

    // 更新底部当前持仓
    renderPositions();

    // 更新条件单列表
    renderConditionOrders();

    saveData();
}

function formatDate(dateStr) {
    const year = dateStr.slice(0, 4);
    const month = dateStr.slice(4, 6);
    const day = dateStr.slice(6, 8);
    return `${year}-${month}-${day}`;
}

// ==================== 连板列表渲染 ====================
function renderLadderList() {
    const container = document.getElementById('ladderList');
    const filter = parseInt(document.getElementById('limitDaysFilter').value) || 0;
    const dateData = window.LADDER_DATA[AppState.currentDate];

    if (!dateData) {
        container.innerHTML = '<p class="empty-tip">暂无连板数据</p>';
        return;
    }

    // 合并所有连板天数的数据
    const allStocks = [];
    Object.keys(dateData).forEach(key => {
        const stocksByDays = dateData[key] || [];
        allStocks.push(...stocksByDays);
    });

    // 根据连板天数过滤
    let filteredStocks = filter === 0
        ? allStocks
        : allStocks.filter(s => {
            if (filter === 5) return s.limitUpDays >= 5;
            return s.limitUpDays === filter;
        });

    // 按连板天数降序排序（最高连板在上）
    filteredStocks.sort((a, b) => b.limitUpDays - a.limitUpDays);

    if (filteredStocks.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无符合条件的股票</p>';
        return;
    }

    container.innerHTML = filteredStocks.map((stock, idx) => {
        const limitClass = stock.limitUpDays >= 5 ? 'limit-up-5' :
            stock.limitUpDays >= 3 ? 'limit-up-4' :
            stock.limitUpDays >= 2 ? 'limit-up-3' : 'limit-up-2';

        const selected = AppState.selectedStock?.code === stock.code ? 'selected' : '';

        return `
            <div class="ladder-item ${limitClass} ${selected}"
                 data-code="${stock.code}"
                 data-stock='${JSON.stringify(stock).replace(/'/g, "&#39;")}'>
                <div class="stock-main">
                    <span class="stock-code">${stock.code.split('.')[0]}</span>
                    <span class="limit-days ${stock.limitUpDays >= 5 ? 'several' : ''}">
                        ${stock.limitUpDays}连板
                    </span>
                </div>
                <div class="stock-price-info">
                    <span class="stock-name">${stock.name}</span>
                    <span class="price">¥${stock.price.toFixed(2)}</span>
                </div>
                <div class="concepts">${(stock.conceptThemes || []).slice(0, 3).join(' | ')}</div>
            </div>
        `;
    }).join('');
}

// 选择股票
function selectStock(code) {
    console.log('selectStock called with code:', code);
    const stockData = getStockData(code);
    console.log('stockData:', stockData ? 'found' : 'not found');
    if (!stockData) {
        showToast('股票数据不存在', 'error');
        return;
    }

    AppState.selectedStock = stockData;

    // 更新UI选中状态
    document.querySelectorAll('.ladder-item').forEach(item => {
        item.classList.remove('selected');
    });
    document.querySelector(`.ladder-item[data-code="${code}"]`)?.classList.add('selected');

    // 更新K线图和交易面板
    updateStockPanel(stockData);
}

function getStockData(code) {
    // 从连板数据获取
    const dateData = window.LADDER_DATA[AppState.currentDate];
    if (dateData) {
        for (const level in dateData) {
            const stock = dateData[level].find(s => s.code === code);
            if (stock) return stock;
        }
    }
    return null;
}

// ==================== K线图 ====================
function initKlineChart() {
    const chartDom = document.getElementById('klineChart');
    // 设置明确的样式
    chartDom.style.width = '100%';
    chartDom.style.height = '100%';

    AppState.klineChart = echarts.init(chartDom);

    // 初始空状态用 setOption 设置，而不是 innerHTML
    AppState.klineChart.setOption({
        title: {
            text: '请从左侧选择一只股票查看K线',
            left: 'center',
            top: 'center',
            textStyle: { color: '#999', fontSize: 14 }
        },
        grid: { hidden: true },
        xAxis: { hidden: true },
        yAxis: { hidden: true }
    });

    // 窗口大小变化时自适应
    window.addEventListener('resize', () => {
        AppState.klineChart?.resize();
    });
}

// 计算K线图缩放起始位置
function calculateZoomStart(totalCount, defaultCount) {
    // 如果有保存的缩放状态且在有效范围内，使用保存的值
    if (AppState.klineZoom && AppState.klineZoom.startValue !== undefined) {
        const savedStart = AppState.klineZoom.startValue;
        const savedEnd = AppState.klineZoom.endValue;
        const savedCount = savedEnd - savedStart;

        // 如果保存的范围在当前数据范围内，使用保存的值
        if (savedStart >= 0 && savedEnd < totalCount) {
            return savedStart;
        }
    }
    // 默认显示最后 defaultCount 根K线
    return Math.max(0, totalCount - defaultCount);
}

function updateKlineChart(code) {
    console.log('updateKlineChart called with code:', code);
    console.log('ECharts available:', typeof echarts !== 'undefined');
    console.log('KLINE_DATA keys:', Object.keys(getAllKlineData()).slice(0, 5));

    let klineData = getKlineData(code);
    console.log('klineData for', code, ':', klineData ? 'found' : 'not found');

    // 如果数据未加载，尝试异步加载
    if (!klineData) {
        console.log('K-line data not loaded, attempting to load from split files...');

        // 显示加载提示
        if (AppState.klineChart) {
            AppState.klineChart.setOption({
                title: {
                    text: '正在加载K线数据...',
                    left: 'center',
                    top: 'center',
                    textStyle: { color: '#999', fontSize: 14 }
                }
            });
        }

        // 异步加载
        if (window.loadKlineAsync) {
            window.loadKlineAsync(code).then(data => {
                if (data) {
                    console.log('K-line data loaded successfully');
                    _renderKlineChart(code, data);
                } else {
                    console.error('Failed to load K-line data');
                    showKlineLoadError(code);
                }
            }).catch(err => {
                console.error('Error loading K-line data:', err);
                showKlineLoadError(code);
            });
        } else {
            console.error('loadKlineAsync not available');
            showKlineLoadError(code);
        }
        return;
    }

    const dates = klineData.dates;
    const values = klineData.values;
    console.log('dates count:', dates?.length, 'values count:', values?.length);

    // 将当前日期转换为与K线数据相同的格式（YYYY-MM-DD）
    const currentDateFormatted = AppState.currentDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');
    console.log('currentDate:', AppState.currentDate, '-> formatted:', currentDateFormatted);

    // 过滤当前日期及之前的数据
    let currentDateIdx = -1;
    for (let i = 0; i < dates.length; i++) {
        if (dates[i] <= currentDateFormatted) {
            currentDateIdx = i;
        } else {
            break;
        }
    }
    console.log('currentDateIdx:', currentDateIdx);

    const validDates = currentDateIdx >= 0 ? dates.slice(0, currentDateIdx + 1) : dates;
    const validValues = currentDateIdx >= 0 ? values.slice(0, currentDateIdx + 1) : values;
    console.log('validDates count:', validDates?.length);

    if (validDates.length === 0) {
        showToast('没有可用日期数据', 'error');
        return;
    }

    // 计算K线数据
    const klineDataFormatted = validValues.map(v => ({
        open: v[0],
        close: v[1],
        low: v[2],
        high: v[3],
    }));

    // ECharts candlestick格式: [open, close, low, high]
    const candlestickData = validValues.map(v => [v[0], v[1], v[2], v[3]]);
    console.log('candlestickData sample:', candlestickData.slice(0, 3));

    // 获取成交量数据
    const volumes = klineData.volumes || [];
    const validVolumes = currentDateIdx >= 0 ? volumes.slice(0, currentDateIdx + 1) : volumes;

    // 计算成交量颜色（上涨为红色，下跌为绿色）
    const volumeColors = validValues.map((v, i) => {
        if (i === 0) return v[1] >= v[0] ? '#ff0000' : '#00aa00';
        return v[1] >= v[0] ? '#ff0000' : '#00aa00';
    });

    // 过滤有效日期对应的成交量
    const validVolumeColors = currentDateIdx >= 0 ? volumeColors.slice(0, currentDateIdx + 1) : volumeColors;

    const option = {
        tooltip: {
            trigger: 'item',
            axisPointer: { type: 'cross' },
            formatter: function(params) {
                if (!params || !params.data) {
                    return '';
                }
                const data = params.data;
                const dateIndex = params.dataIndex;

                // 获取成交量（所有系列共享同一个dataIndex）
                const volume = validVolumes[dateIndex] || 0;

                // 检查数据格式：[dateIndex, open, close, low, high]
                let open, close, low, high;

                if (Array.isArray(data) && data.length >= 5) {
                    // ECharts candlestick with dataIndex
                    open = data[1];
                    close = data[2];
                    low = data[3];
                    high = data[4];
                } else if (Array.isArray(data) && data.length === 4) {
                    // 标准 [open, close, low, high] 格式
                    open = data[0];
                    close = data[1];
                    low = data[2];
                    high = data[3];
                } else {
                    return '数据格式错误';
                }

                // 获取上一根K线的收盘价（第一根K线或prevClose为0时，相对于自己）
                let prevClose = 0;
                if (dateIndex > 0 && validValues[dateIndex - 1]) {
                    prevClose = validValues[dateIndex - 1][1];
                }
                if (!prevClose || prevClose === 0) {
                    prevClose = close;
                }

                // 计算相对于上一根K线收盘价的涨跌幅
                const formatChange = (price) => {
                    if (prevClose === 0) {
                        return '<span style="color:#999">(--%)</span>';
                    }
                    const change = ((price - prevClose) / prevClose * 100).toFixed(2);
                    const color = change >= 0 ? '#ff0000' : '#00aa00';
                    return `<span style="color: ${color}">(${change >= 0 ? '+' : ''}${change}%)</span>`;
                };

                return `
                    <div style="padding: 5px;">
                        <div>${params.name}</div>
                        <div>开盘: ${open.toFixed(2)} ${formatChange(open)}</div>
                        <div>收盘: ${close.toFixed(2)} ${formatChange(close)}</div>
                        <div>最高: ${high.toFixed(2)} ${formatChange(high)}</div>
                        <div>最低: ${low.toFixed(2)} ${formatChange(low)}</div>
                        <div>成交量: ${(volume / 10000).toFixed(2)}万手</div>
                    </div>
                `;
            }
        },
        grid: [
            {
                left: '10%',
                right: '10%',
                top: '10%',
                height: '50%'
            },
            {
                left: '10%',
                right: '10%',
                top: '65%',
                height: '20%'
            }
        ],
        xAxis: [
            {
                type: 'category',
                data: validDates,
                boundaryGap: false,
                axisLine: { onZero: false },
                axisLabel: { show: true },
                min: 'dataMin',
                max: 'dataMax'
            },
            {
                type: 'category',
                data: validDates,
                boundaryGap: false,
                axisLine: { onZero: false },
                axisLabel: { show: false },
                gridIndex: 1,
                min: 'dataMin',
                max: 'dataMax'
            }
        ],
        yAxis: [
            {
                scale: true,
                splitArea: { show: true },
                axisLabel: { show: true }
            },
            {
                scale: true,
                splitNumber: 2,
                axisLabel: { show: true },
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { show: false },
                gridIndex: 1,
                valueFormatter: function(value) {
                    return (value / 10000).toFixed(0) + '万';
                }
            }
        ],
        dataZoom: [
            {
                type: 'inside',
                startValue: calculateZoomStart(validDates.length, 30),
                endValue: validDates.length - 1,
                xAxisIndex: [0, 1]
            },
            {
                type: 'slider',
                bottom: 10,
                startValue: calculateZoomStart(validDates.length, 30),
                endValue: validDates.length - 1,
                xAxisIndex: [0, 1]
            }
        ],
        series: [
            {
                name: 'K线',
                type: 'candlestick',
                data: candlestickData,
                itemStyle: {
                    color: '#ff0000',      // 阳线（上涨）红色
                    color0: '#00aa00',     // 阴线（下跌）绿色
                    borderColor: '#ff0000',
                    borderColor0: '#00aa00'
                }
            },
            {
                name: '成交量',
                type: 'bar',
                data: validVolumes,
                itemStyle: {
                    color: function(params) {
                        const idx = params.dataIndex;
                        if (idx >= validValues.length) return '#00aa00';
                        return validValues[idx][1] >= validValues[idx][0] ? '#ff0000' : '#00aa00';
                    }
                },
                xAxisIndex: 1,
                yAxisIndex: 1
            }
        ]
    };

    console.log('Setting chart option with', candlestickData.length, 'data points');
    AppState.klineChart.setOption(option);  // 正常更新，不使用 notMerge

    // 监听缩放事件，保存用户调整的状态
    AppState.klineChart.off('datazoom');  // 移除旧事件
    AppState.klineChart.on('datazoom', function(params) {
        const option = AppState.klineChart.getOption();
        const zoom = option.dataZoom[0];
        if (zoom.startValue !== undefined && zoom.endValue !== undefined) {
            AppState.klineZoom = {
                startValue: zoom.startValue,
                endValue: zoom.endValue
            };
        }
    });
}

// 内部渲染K线图的函数（统一渲染逻辑）
function _renderKlineChart(code, klineData) {
    const dates = klineData.dates;
    const values = klineData.values;
    console.log('Rendering K-line for', code, '- bars:', dates?.length);

    // 将当前日期转换为与K线数据相同的格式（YYYY-MM-DD）
    const currentDateFormatted = AppState.currentDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');

    // 过滤当前日期及之前的数据
    let currentDateIdx = -1;
    for (let i = 0; i < dates.length; i++) {
        if (dates[i] <= currentDateFormatted) {
            currentDateIdx = i;
        } else {
            break;
        }
    }

    const validDates = currentDateIdx >= 0 ? dates.slice(0, currentDateIdx + 1) : dates;
    const validValues = currentDateIdx >= 0 ? values.slice(0, currentDateIdx + 1) : values;

    if (validDates.length === 0) {
        showToast('没有可用日期数据', 'error');
        return;
    }

    // ECharts candlestick格式: [open, close, low, high]
    const candlestickData = validValues.map(v => [v[0], v[1], v[2], v[3]]);

    // 获取成交量数据
    const volumes = klineData.volumes || [];
    const validVolumes = currentDateIdx >= 0 ? volumes.slice(0, currentDateIdx + 1) : volumes;

    const option = {
        tooltip: {
            trigger: 'item',
            axisPointer: { type: 'cross' },
            formatter: function(params) {
                if (!params || !params.data) return '';
                const data = params.data;
                const dateIndex = params.dataIndex;
                const volume = validVolumes[dateIndex] || 0;

                let open, close, low, high;
                if (Array.isArray(data) && data.length === 4) {
                    open = data[0];
                    close = data[1];
                    low = data[2];
                    high = data[3];
                } else {
                    return '数据格式错误';
                }

                let prevClose = 0;
                if (dateIndex > 0 && validValues[dateIndex - 1]) {
                    prevClose = validValues[dateIndex - 1][1];
                }
                if (!prevClose || prevClose === 0) prevClose = close;

                const formatChange = (price) => {
                    if (prevClose === 0) return '<span style="color:#999">(--%)</span>';
                    const change = ((price - prevClose) / prevClose * 100).toFixed(2);
                    const color = change >= 0 ? '#ff0000' : '#00aa00';
                    return `<span style="color: ${color}">(${change >= 0 ? '+' : ''}${change}%)</span>`;
                };

                return `
                    <div style="padding: 5px;">
                        <div>${params.name}</div>
                        <div>开盘: ${open.toFixed(2)} ${formatChange(open)}</div>
                        <div>收盘: ${close.toFixed(2)} ${formatChange(close)}</div>
                        <div>最高: ${high.toFixed(2)} ${formatChange(high)}</div>
                        <div>最低: ${low.toFixed(2)} ${formatChange(low)}</div>
                        <div>成交量: ${(volume / 10000).toFixed(2)}万手</div>
                    </div>
                `;
            }
        },
        grid: [
            { left: '10%', right: '10%', top: '10%', height: '50%' },
            { left: '10%', right: '10%', top: '65%', height: '20%' }
        ],
        xAxis: [
            {
                type: 'category',
                data: validDates,
                boundaryGap: false,
                axisLine: { onZero: false },
                axisLabel: { show: true },
                min: 'dataMin',
                max: 'dataMax'
            },
            {
                type: 'category',
                data: validDates,
                boundaryGap: false,
                axisLine: { onZero: false },
                axisLabel: { show: false },
                gridIndex: 1,
                min: 'dataMin',
                max: 'dataMax'
            }
        ],
        yAxis: [
            {
                scale: true,
                splitArea: { show: true },
                axisLabel: { show: true }
            },
            {
                scale: true,
                splitNumber: 2,
                axisLabel: { show: true },
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { show: false },
                gridIndex: 1,
                valueFormatter: function(value) {
                    return (value / 10000).toFixed(0) + '万';
                }
            }
        ],
        dataZoom: [
            {
                type: 'inside',
                startValue: calculateZoomStart(validDates.length, 30),
                endValue: validDates.length - 1,
                xAxisIndex: [0, 1]
            },
            {
                type: 'slider',
                bottom: 10,
                startValue: calculateZoomStart(validDates.length, 30),
                endValue: validDates.length - 1,
                xAxisIndex: [0, 1]
            }
        ],
        series: [
            {
                name: 'K线',
                type: 'candlestick',
                data: candlestickData,
                itemStyle: {
                    color: '#ff0000',
                    color0: '#00aa00',
                    borderColor: '#ff0000',
                    borderColor0: '#00aa00'
                }
            },
            {
                name: '成交量',
                type: 'bar',
                data: validVolumes,
                itemStyle: {
                    color: function(params) {
                        const idx = params.dataIndex;
                        if (idx >= validValues.length) return '#00aa00';
                        return validValues[idx][1] >= validValues[idx][0] ? '#ff0000' : '#00aa00';
                    }
                },
                xAxisIndex: 1,
                yAxisIndex: 1
            }
        ]
    };

    AppState.klineChart.setOption(option);

    // 保存加载的数据到缓存
    const cacheData = {
        ...klineData,
        dates: validDates,
        values: validValues,
        volumes: validVolumes
    };
    window.KLINE_DATA_CORE = window.KLINE_DATA_CORE || {};
    window.KLINE_DATA_CORE[code] = cacheData;

    // 缓存到加载器
    if (window.klineLoader) {
        window.klineLoader.cache.set(code, cacheData);
    }

    console.log('K-line chart rendered for', code);
}

// 显示K线加载错误
function showKlineLoadError(code) {
    if (AppState.klineChart) {
        AppState.klineChart.setOption({
            title: {
                text: `${code} K线数据不可用`,
                left: 'center',
                top: 'center',
                textStyle: { color: '#999', fontSize: 14 }
            },
            xAxis: { show: false },
            yAxis: { show: false },
            series: []
        });
    }
    showToast(`无法加载 ${code} 的K线数据`, 'warning');
}

// ==================== 交易面板更新 ====================
function updateStockPanel(stock) {
    const klineData = getKlineData(stock.code);

    // 更新股票信息
    document.getElementById('stockName').textContent = `${stock.name} (${stock.code})`;
    document.getElementById('stockCode').textContent = stock.code;
    document.getElementById('currentPrice').textContent = `¥${stock.price.toFixed(2)}`;
    document.getElementById('limitUpDays').textContent = `${stock.limitUpDays}连板`;

    // 更新涨停价
    const isST = stock.name.includes('ST');
    const limitRate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_UP;
    const limitUpPrice = Math.round(stock.price * (1 + limitRate) * 100) / 100;
    document.getElementById('limitUpPrice').value = limitUpPrice;

    // 更新次日开盘信息：从K线数据获取当前日期的收盘价和下一日的开盘价
    let nextDayOpenChangePct = 0;
    let nextDayOpenPrice = stock.price;

    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const currentDateIdx = klineData.dates.indexOf(targetDate);

        // 如果找到当前日期，且存在下一日数据
        if (currentDateIdx >= 0 && currentDateIdx + 1 < klineData.dates.length) {
            const currentClosePrice = klineData.values[currentDateIdx][1]; // 当前日收盘价
            const nextDayOpenPriceVal = klineData.values[currentDateIdx + 1][0]; // 下一日开盘价

            nextDayOpenChangePct = ((nextDayOpenPriceVal - currentClosePrice) / currentClosePrice) * 100;
            nextDayOpenPrice = nextDayOpenPriceVal;
        }
    }

    const openChangeEl = document.getElementById('nextDayOpenChange');
    openChangeEl.textContent = `${nextDayOpenChangePct >= 0 ? '+' : ''}${nextDayOpenChangePct.toFixed(2)}%`;
    openChangeEl.className = nextDayOpenChangePct >= 0 ? 'positive' : 'negative';
    document.getElementById('nextDayOpenPrice').textContent = `¥${nextDayOpenPrice.toFixed(2)}`;

    // 更新条件单默认价格
    document.getElementById('limitUpPrice').value = limitUpPrice.toFixed(2);

    // 更新加仓价格默认值为开盘价
    document.getElementById('addPrice').value = nextDayOpenPrice.toFixed(2);

    // 更新持仓信息
    updatePositionInfo(stock.code);

    // 更新K线图
    updateKlineChart(stock.code);

    // 更新次日操作预览
    renderNextDayActions();
}

function updatePositionInfo(code) {
    const position = AppState.account.positions[code];
    const klineData = getKlineData(code);

    // 从K线数据中获取当前日期的价格
    let currentPrice = position ? position.cost : 0;
    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const dateIdx = klineData.dates.indexOf(targetDate);
        if (dateIdx >= 0 && klineData.values[dateIdx]) {
            currentPrice = klineData.values[dateIdx][1]; // 收盘价
        }
    }

    const quantityEl = document.getElementById('holdQuantity');
    const costEl = document.getElementById('holdCost');
    const profitEl = document.getElementById('holdProfit');
    const sellTip = document.getElementById('sellTip');

    if (position) {
        quantityEl.textContent = position.quantity;
        costEl.textContent = position.cost.toFixed(2);

        const profit = (currentPrice - position.cost) * position.quantity;
        profitEl.textContent = `${profit >= 0 ? '+' : ''}${profit.toFixed(2)}`;
        profitEl.className = profit >= 0 ? 'loss' : 'profit';

        // 检查是否可卖出（T+1）
        const buyDateIdx = AppState.availableDates.indexOf(position.buyDate);
        const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);
        const canSell = currentDateIdx > buyDateIdx;

        if (canSell) {
            sellTip.textContent = '';
            document.getElementById('sellPrice').value = currentPrice.toFixed(2);
            document.getElementById('sellQuantity').value = position.quantity;
        } else {
            const daysWait = buyDateIdx + 1 - currentDateIdx;
            sellTip.textContent = `T+1交易，还需等待${daysWait}个交易日`;
            sellTip.className = 'tip warning';
        }

        document.getElementById('sellSection').style.display = 'block';
    } else {
        document.getElementById('sellSection').style.display = 'none';
    }
}

function updatePositionsInfo() {
    Object.keys(AppState.account.positions).forEach(code => {
        updatePositionInfo(code);
    });
}

// ==================== 股票搜索 ====================

// 股票搜索功能
function searchStock() {
    const keyword = document.getElementById('stockSearch').value.trim().toUpperCase();
    if (!keyword) {
        document.getElementById('searchResults').innerHTML = '<p class="empty-tip">请输入股票代码或名称</p>';
        return;
    }

    const results = [];
    const klineData = getAllKlineData();

    // 搜索K线数据中的股票
    for (const [code, data] of Object.entries(klineData)) {
        const codeMatch = code.toUpperCase().includes(keyword);
        const nameMatch = data.name && data.name.toUpperCase().includes(keyword);

        if (codeMatch || nameMatch) {
            // 获取当前价格
            const values = data.values || [];
            const currentPrice = values.length > 0 ? values[values.length - 1][1] : 0;

            results.push({
                code: code,
                name: data.name || '未知',
                price: currentPrice
            });
        }
    }

    renderSearchResults(results);
}

// 渲染搜索结果
function renderSearchResults(results) {
    const container = document.getElementById('searchResults');

    if (results.length === 0) {
        container.innerHTML = '<p class="empty-tip">未找到相关股票</p>';
        return;
    }

    // 限制显示数量
    const displayResults = results.slice(0, 20);

    container.innerHTML = displayResults.map(stock => `
        <div class="search-result-item" onclick="selectStockByCode('${stock.code}')">
            <div class="item-header">
                <span class="code">${stock.code}</span>
                <span class="price">¥${stock.price.toFixed(2)}</span>
            </div>
            <div class="name">${stock.name}</div>
        </div>
    `).join('');
}

// 根据代码选择股票
function selectStockByCode(code) {
    const klineData = getKlineData(code);
    if (!klineData) {
        showToast('股票数据不存在', 'error');
        return;
    }

    // 创建一个虚拟的股票对象用于显示
    const stock = {
        code: code,
        name: klineData.name || '未知',
        price: klineData.values?.length > 0 ? klineData.values[klineData.values.length - 1][1] : 0,
        limitUpDays: 0,
        conceptThemes: [],
        nextDayOpenChangePct: 0
    };

    AppState.selectedStock = stock;

    // 清空搜索结果
    document.getElementById('searchResults').innerHTML = '';
    document.getElementById('stockSearch').value = '';

    // 更新K线图和交易面板
    updateStockPanel(stock);

    showToast(`已选择: ${stock.name}`, 'success');
}

// ==================== 我的持仓列表 ====================

// 渲染我的持仓列表
function renderMyPositions() {
    const container = document.getElementById('myPositionsList');
    const positions = AppState.account.positions;

    if (Object.keys(positions).length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无持仓</p>';
        return;
    }

    const positionItems = Object.entries(positions).map(([code, pos]) => {
        const klineData = getKlineData(code);
        const stockName = klineData?.name || '未知';

        // 从K线数据中获取当前日期的价格
        let currentPrice = pos.cost; // 默认使用成本价
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate); // 转换为 YYYY-MM-DD 格式
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1]; // 收盘价
            }
        }

        const profit = (currentPrice - pos.cost) * pos.quantity;
        const profitRate = ((currentPrice - pos.cost) / pos.cost * 100);
        const profitClass = profit >= 0 ? 'profit' : 'loss';

        return `
            <div class="my-position-item">
                <div class="item-header">
                    <span class="code">${code}</span>
                    <span class="quantity">${pos.quantity}股</span>
                </div>
                <div class="price-info">
                    <span>成本: ¥${pos.cost.toFixed(2)}</span>
                    <span class="${profitClass}">${profit >= 0 ? '+' : ''}${profit.toFixed(2)} (${profitRate.toFixed(1)}%)</span>
                </div>
                <button class="btn-view" onclick="viewPositionChart('${code}')">查看K线</button>
            </div>
        `;
    }).join('');

    container.innerHTML = positionItems;
}

// 查看持仓股票的K线图
function viewPositionChart(code) {
    const klineData = getKlineData(code);
    if (!klineData) {
        showToast('K线数据不存在', 'error');
        return;
    }

    const stock = {
        code: code,
        name: klineData.name || '未知',
        price: klineData.values?.length > 0 ? klineData.values[klineData.values.length - 1][1] : 0,
        limitUpDays: 0,
        conceptThemes: [],
        nextDayOpenChangePct: 0
    };

    AppState.selectedStock = stock;
    updateStockPanel(stock);

    // 滚动到K线图位置
    document.querySelector('.chart-panel').scrollIntoView({ behavior: 'smooth' });
}

// ==================== 交易逻辑 ====================

// 计算涨停价
function calculateLimitUpPrice(price, isST = false) {
    const rate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_UP;
    return Math.round(price * (1 + rate) * 100) / 100;
}

// 计算跌停价
function calculateLimitDownPrice(price, isST = false) {
    const rate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_DOWN;
    return Math.round(price * (1 - rate) * 100) / 100;
}

// 验证价格是否在涨跌停范围内
function validatePrice(code, price) {
    if (!price || price <= 0) {
        return { valid: false, message: '价格必须大于0' };
    }

    // 获取股票的当前价格（收盘价）
    const klineData = getKlineData(code);
    if (klineData) {
        const klineValues = klineData.values || [];
        const klineDates = klineData.dates || [];

        // 找到当前日期对应的收盘价
        const currentDateFormatted = AppState.currentDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');
        const currentIndex = klineDates.indexOf(currentDateFormatted);

        if (currentIndex >= 0) {
            const closePrice = klineValues[currentIndex][1]; // close
            const name = klineData.name || '';
            const isST = name.includes('ST');

            const limitUp = calculateLimitUpPrice(closePrice, isST);
            const limitDown = calculateLimitDownPrice(closePrice, isST);

            // 买入价格不能高于涨停价，卖出价格不能低于跌停价
            if (price > limitUp) {
                return { valid: false, message: `买入价格不能高于涨停价 ¥${limitUp.toFixed(2)}` };
            }
            if (price < limitDown) {
                return { valid: false, message: `价格不能低于跌停价 ¥${limitDown.toFixed(2)}` };
            }
        }
    }

    return { valid: true };
}

// 计算交易费用
function calculateFees(amount, isSell = false) {
    let commission = Math.abs(amount) * CONFIG.FEES.COMMISSION;
    commission = Math.max(commission, CONFIG.FEES.MIN_COMMISSION);

    let stampTax = 0;
    if (isSell) {
        stampTax = Math.abs(amount) * CONFIG.FEES.STAMP_TAX;
    }

    return {
        commission: commission,
        stampTax: stampTax,
        total: commission + stampTax
    };
}

// 设置条件单（涨停买入）
function setConditionOrder() {
    const code = AppState.selectedStock?.code;
    if (!code) {
        showToast('请先选择股票', 'warning');
        return;
    }

    const price = parseFloat(document.getElementById('limitUpPrice').value);
    const quantity = parseInt(document.getElementById('conditionQuantity').value);

    if (!price || price <= 0) {
        showToast('请输入有效的涨停价格', 'warning');
        return;
    }

    if (!quantity || quantity % CONFIG.TRADING_UNIT !== 0) {
        showToast(`买入股数必须是${CONFIG.TRADING_UNIT}的整数倍`, 'warning');
        return;
    }

    const amount = price * quantity;
    const available = AppState.account.available + AppState.account.frozen;

    if (amount > available) {
        showToast('可用资金不足', 'warning');
        return;
    }

    const currentIdx = AppState.availableDates.indexOf(AppState.currentDate);
    const nextDate = AppState.availableDates[currentIdx + 1];

    if (!nextDate) {
        showToast('已是最后一个交易日', 'warning');
        return;
    }

    const order = {
        id: generateId(),
        date: nextDate,
        orderDate: AppState.currentDate,
        code: code,
        name: AppState.selectedStock.name,
        type: 'CONDITION_BUY',
        triggerPrice: price,
        quantity: quantity,
        amount: amount,
        status: 'PENDING'
    };

    AppState.conditionOrders.push(order);

    // 冻结资金
    AppState.account.frozen += amount;

    showToast(`条件单已设置，将于${formatDate(nextDate)}监控执行`, 'success');
    saveData();
    renderConditionOrders();
    renderNextDayActions();
}

// 开盘买入
function addPosition() {
    const code = AppState.selectedStock?.code;
    if (!code) {
        showToast('请先选择股票', 'warning');
        return;
    }

    const price = parseFloat(document.getElementById('addPrice').value);
    const quantity = parseInt(document.getElementById('addQuantity').value);

    if (!price || price <= 0) {
        showToast('请输入有效的买入价格', 'warning');
        return;
    }

    // 验证价格
    const priceValidation = validatePrice(code, price);
    if (!priceValidation.valid) {
        showToast(priceValidation.message, 'warning');
        return;
    }

    if (!quantity || quantity % CONFIG.TRADING_UNIT !== 0) {
        showToast(`买入股数必须是${CONFIG.TRADING_UNIT}的整数倍`, 'warning');
        return;
    }

    const amount = price * quantity;
    const available = AppState.account.available + AppState.account.frozen;

    if (amount > available) {
        showToast('可用资金不足', 'warning');
        return;
    }

    const currentIdx = AppState.availableDates.indexOf(AppState.currentDate);
    const nextDate = AppState.availableDates[currentIdx + 1];

    if (!nextDate) {
        showToast('已是最后一个交易日', 'warning');
        return;
    }

    // 计算交易费用
    const fees = calculateFees(amount, false);

    // 显示详细确认对话框
    const tradeDetail = {
        price: price,
        quantity: quantity,
        amount: amount,
        commission: fees.commission,
        stampTax: 0,
        total: amount + fees.total
    };

    showConfirmModal(
        `确认在${formatDate(nextDate)}以¥${price.toFixed(2)}买入${quantity}股${AppState.selectedStock.name}？`,
        () => {
            // 执行买入
            const tradeRecord = {
                id: generateId(),
                date: nextDate,
                orderDate: AppState.currentDate,
                code: code,
                name: AppState.selectedStock.name,
                type: 'ADD',
                price: price,
                quantity: quantity,
                amount: amount,
                fees: fees.total,
                status: 'PENDING'
            };

            AppState.trades.push(tradeRecord);
            AppState.account.frozen += amount;

            showToast(`买入委托已提交，将于${formatDate(nextDate)}执行`, 'success');
            saveData();
            renderTradeRecords();
            renderNextDayActions();
        },
        tradeDetail
    );
}

// 卖出持仓 - 创建待执行订单（T+1确认）
function sellStock() {
    const code = AppState.selectedStock?.code;
    if (!code) {
        showToast('请先选择股票', 'warning');
        return;
    }

    const position = AppState.account.positions[code];
    if (!position) {
        showToast('没有该股票持仓', 'warning');
        return;
    }

    const price = parseFloat(document.getElementById('sellPrice').value);
    const quantity = parseInt(document.getElementById('sellQuantity').value);

    if (!price || price <= 0) {
        showToast('请输入有效的卖出价格', 'warning');
        return;
    }

    // 验证价格
    const priceValidation = validatePrice(code, price);
    if (!priceValidation.valid) {
        showToast(priceValidation.message, 'warning');
        return;
    }

    if (!quantity || quantity <= 0 || quantity > position.quantity) {
        showToast('卖出股数不能超过持仓', 'warning');
        return;
    }

    // 检查是否可卖出（T+1）
    const buyDateIdx = AppState.availableDates.indexOf(position.buyDate);
    const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);

    if (currentDateIdx <= buyDateIdx) {
        showToast('T+1交易制度，今日买入不可卖出', 'warning');
        return;
    }

    const amount = price * quantity;
    const fees = calculateFees(amount, true);
    const netAmount = amount - fees.total;

    // 显示详细确认对话框
    const tradeDetail = {
        price: price,
        quantity: quantity,
        amount: amount,
        commission: fees.commission,
        stampTax: fees.stampTax,
        total: amount - fees.total
    };

    showConfirmModal(
        `确认以¥${price.toFixed(2)}卖出${quantity}股${AppState.selectedStock.name}？`,
        () => {
            const tradeRecord = {
                id: generateId(),
                date: AppState.currentDate,
                orderDate: AppState.currentDate,
                code: code,
                name: AppState.selectedStock.name,
                type: 'SELL',
                price: price,
                quantity: quantity,
                amount: amount,
                fees: fees.total,
                netAmount: netAmount,
                positionCost: position.cost * quantity,
                status: 'COMPLETED'
            };

            AppState.trades.push(tradeRecord);

            position.quantity -= quantity;
            if (position.quantity <= 0) {
                delete AppState.account.positions[code];
            }

            AppState.account.available += netAmount;

            showToast(`卖出成功，款项¥${netAmount.toFixed(2)}已到账`, 'success');
            saveData();
            renderTradeRecords();
            updateAccountInfo();
            updatePositionInfo(code);
        },
        tradeDetail
    );
}

// 执行待执行的订单
function executePendingOrders(executeDate) {
    // 执行待完成的交易记录
    AppState.trades.forEach(trade => {
        if (trade.status === 'PENDING' && trade.date === executeDate) {
            executeTrade(trade);
        }
    });

    // 执行条件单
    AppState.conditionOrders.forEach(order => {
        if (order.status === 'PENDING' && order.date === executeDate) {
            executeConditionOrder(order);
        }
    });
}

function executeTrade(trade) {
    const klineData = getKlineData(trade.code);
    if (!klineData) {
        trade.status = 'FAILED';
        showToast(`无法找到 ${trade.code} 的K线数据`, 'error');
        return;
    }

    // 将 YYYYMMDD 格式转换为 YYYY-MM-DD 格式来查找K线数据
    const targetDate = formatDate(trade.date);

    // 获取执行日的K线数据
    const dateIdx = klineData.dates.findIndex(d => d === targetDate);
    if (dateIdx < 0) {
        trade.status = 'FAILED';
        console.warn(`无法找到 ${trade.code} 在 ${targetDate} 的K线数据`, {
            tradeDate: trade.date,
            klineDates: klineData.dates.slice(-5)
        });
        showToast(`无法找到 ${trade.code} 在 ${targetDate} 的K线数据`, 'warning');
        return;
    }

    const dayData = klineData.values[dateIdx];
    const actualPrice = dayData[0]; // 开盘价作为实际成交价

    if (trade.type === 'BUY' || trade.type === 'ADD') {
        // 买入成交
        trade.actualPrice = actualPrice;
        trade.actualAmount = actualPrice * trade.quantity;
        trade.fees = calculateFees(trade.actualAmount).total;
        trade.status = 'COMPLETED';

        // 解冻资金：使用实际成交金额+手续费
        const totalDeduction = trade.actualAmount + trade.fees;
        AppState.account.frozen -= trade.amount;
        AppState.account.frozen += (trade.amount - totalDeduction); // 解冻剩余部分

        // 更新可用资金：扣除实际成交金额+手续费
        AppState.account.available -= totalDeduction;

        // 更新持仓
        const position = AppState.account.positions[trade.code];
        if (position) {
            const totalCost = position.cost * position.quantity + trade.actualAmount;
            const totalQuantity = position.quantity + trade.quantity;
            position.cost = totalCost / totalQuantity;
            position.quantity = totalQuantity;
        } else {
            AppState.account.positions[trade.code] = {
                quantity: trade.quantity,
                cost: actualPrice,
                buyDate: trade.date
            };
        }

        showToast(`${trade.name} 买入成交：${trade.quantity}股 @ ¥${actualPrice.toFixed(2)}`, 'success');

    } else if (trade.type === 'SELL') {
        // SELL 由 sellStock 直接处理，这里不需要重复处理
        // 卖出款项已在 sellStock 中冻结，在 switchDate 时解冻到可用资金
        return;
    }

    updateAccountInfo();
    renderMyPositions();
}

function executeConditionOrder(order) {
    const klineData = getKlineData(order.code);
    if (!klineData) {
        order.status = 'FAILED';
        // 解冻资金
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        showToast(`无法找到 ${order.code} 的K线数据`, 'error');
        return;
    }

    // 将 YYYYMMDD 格式转换为 YYYY-MM-DD 格式来查找K线数据
    const targetDate = formatDate(order.date);

    const dateIdx = klineData.dates.findIndex(d => d === targetDate);
    if (dateIdx < 0) {
        order.status = 'FAILED';
        // 解冻资金
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        console.warn(`无法找到 ${order.code} 在 ${targetDate} 的K线数据`, {
            orderDate: order.date,
            klineDates: klineData.dates.slice(-5)
        });
        showToast(`无法找到 ${order.code} 在 ${targetDate} 的K线数据`, 'warning');
        return;
    }

    const dayData = klineData.values[dateIdx];
    const high = dayData[3]; // 当日最高价

    // 检查是否涨停触发
    if (high >= order.triggerPrice) {
        // 以涨停价成交
        const actualPrice = order.triggerPrice;

        order.actualPrice = actualPrice;
        order.actualAmount = actualPrice * order.quantity;
        order.fees = calculateFees(order.actualAmount).total;
        order.status = 'EXECUTED';

        // 解冻资金：使用实际成交金额+手续费
        const totalDeduction = order.actualAmount + order.fees;
        AppState.account.frozen -= order.amount;
        AppState.account.frozen += (order.amount - totalDeduction); // 解冻剩余部分

        // 更新可用资金：扣除实际成交金额+手续费
        AppState.account.available -= totalDeduction;

        // 更新持仓
        const position = AppState.account.positions[order.code];
        if (position) {
            const totalCost = position.cost * position.quantity + order.actualAmount;
            const totalQuantity = position.quantity + order.quantity;
            position.cost = totalCost / totalQuantity;
            position.quantity = totalQuantity;
        } else {
            AppState.account.positions[order.code] = {
                quantity: order.quantity,
                cost: actualPrice,
                buyDate: order.date
            };
        }

        // 创建买入交易记录（用于收益统计）
        const tradeRecord = {
            id: generateId(),
            date: order.date,
            orderDate: order.orderDate,
            code: order.code,
            name: order.name,
            type: 'BUY',
            price: actualPrice,
            quantity: order.quantity,
            amount: actualPrice * order.quantity,
            fees: order.fees,
            status: 'COMPLETED'
        };
        AppState.trades.push(tradeRecord);

        showToast(`条件单触发：${order.name} 以涨停价 ¥${actualPrice.toFixed(2)} 买入 ${order.quantity}股`, 'success');
    } else {
        order.status = 'EXPIRED';
        // 解冻资金：全额解冻回可用资金
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        showToast(`条件单过期：${order.name} 未触及涨停价`, 'warning');
    }

    updateAccountInfo();
    renderMyPositions();
}

// 取消条件单
function cancelConditionOrder(orderId) {
    const order = AppState.conditionOrders.find(o => o.id === orderId);
    if (order && order.status === 'PENDING') {
        order.status = 'CANCELLED';
        // 解冻资金
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        showToast('条件单已取消', 'success');
        saveData();
        renderConditionOrders();
        renderNextDayActions();
        updateAccountInfo();
    }
}

// ==================== 账户信息更新 ====================
function updateAccountInfo() {
    // 计算持仓市值
    let positionValue = 0;
    Object.keys(AppState.account.positions).forEach(code => {
        const position = AppState.account.positions[code];
        const klineData = getKlineData(code);

        // 从K线数据中获取当前日期的价格
        let currentPrice = position.cost;
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate); // 转换为 YYYY-MM-DD 格式
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1]; // 收盘价
            }
        }

        positionValue += position.quantity * currentPrice;
    });

    const totalAsset = AppState.account.available + positionValue;
    const totalReturn = ((totalAsset - AppState.account.initialFund) / AppState.account.initialFund * 100);

    document.getElementById('availableFund').textContent = formatNumber(AppState.account.available);
    document.getElementById('positionValue').textContent = formatNumber(positionValue);
    document.getElementById('totalAsset').textContent = formatNumber(totalAsset);

    const returnEl = document.getElementById('totalReturn');
    returnEl.textContent = `${totalReturn >= 0 ? '+' : ''}${totalReturn.toFixed(2)}%`;
    returnEl.style.color = totalReturn >= 0 ? 'var(--success-color)' : 'var(--danger-color)';
}

// ==================== 渲染函数 ====================
function renderConditionOrders() {
    const container = document.getElementById('conditionOrdersList');
    const pendingOrders = AppState.conditionOrders.filter(o => o.status === 'PENDING');

    if (pendingOrders.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无条件单</p>';
        return;
    }

    container.innerHTML = pendingOrders.map(order => `
        <div class="condition-order-item">
            <div>
                <span class="code">${order.code.split('.')[0]}</span>
                <span class="status pending">待执行</span>
            </div>
            <div style="margin-top: 5px;">
                触发价: <span class="price">¥${order.triggerPrice.toFixed(2)}</span>
                数量: <span class="quantity">${order.quantity}股</span>
            </div>
            <div style="font-size: 11px; color: #999;">
                执行日: ${formatDate(order.date)}
            </div>
            <button class="btn-cancel" onclick="cancelConditionOrder('${order.id}')">取消</button>
        </div>
    `).join('');
}

function renderNextDayActions() {
    const container = document.getElementById('nextDayActions');

    // 合并待执行的交易和条件单
    const pendingTrades = AppState.trades.filter(t => t.status === 'PENDING' && t.date === getNextDate());
    const pendingOrders = AppState.conditionOrders.filter(o => o.status === 'PENDING' && o.date === getNextDate());

    const allActions = [
        ...pendingTrades.map(t => ({
            type: t.type === 'BUY' ? '开盘买入' : '加仓',
            code: t.code,
            name: t.name,
            price: t.price,
            quantity: t.quantity
        })),
        ...pendingOrders.map(o => ({
            type: '条件单',
            code: o.code,
            name: o.name,
            price: o.triggerPrice,
            quantity: o.quantity
        }))
    ];

    if (allActions.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无待执行操作</p>';
        return;
    }

    container.innerHTML = allActions.map(action => `
        <div class="next-day-action-item">
            <div class="type">${action.type}</div>
            <div>${action.code.split('.')[0]} - ${action.name}</div>
            <div>价格: ¥${action.price.toFixed(2)} | 数量: ${action.quantity}股</div>
        </div>
    `).join('');
}

function renderTradeRecords() {
    const tbody = document.getElementById('recordsBody');

    if (AppState.trades.length === 0) {
        tbody.innerHTML = '<tr><td colspan="9" class="empty-tip">暂无交易记录</td></tr>';
        return;
    }

    const sortedTrades = [...AppState.trades].sort((a, b) => {
        return (b.date + b.orderDate).localeCompare(a.date + a.orderDate);
    });

    tbody.innerHTML = sortedTrades.map(trade => {
        const date = trade.status === 'PENDING' ? trade.date : trade.date;
        return `
            <tr>
                <td>${formatDate(date)}</td>
                <td>${trade.code.split('.')[0]}</td>
                <td>${trade.name}</td>
                <td class="${trade.type === 'BUY' || trade.type === 'ADD' ? '' : 'profit'}">
                    ${trade.type === 'BUY' ? '买入' : trade.type === 'ADD' ? '加仓' : '卖出'}
                </td>
                <td>¥${trade.price.toFixed(2)}</td>
                <td>${trade.quantity}</td>
                <td>¥${formatNumber(trade.amount)}</td>
                <td>¥${trade.fees?.toFixed(2) || '0.00'}</td>
                <td>${getStatusText(trade.status)}</td>
            </tr>
        `;
    }).join('');

    // 更新统计数据
    updateSummaryStats();
}

function renderPositions() {
    const tbody = document.getElementById('positionsBody');
    const positions = Object.entries(AppState.account.positions);

    if (positions.length === 0) {
        tbody.innerHTML = '<tr><td colspan="10" class="empty-tip">暂无持仓</td></tr>';
        return;
    }

    tbody.innerHTML = positions.map(([code, pos]) => {
        const klineData = getKlineData(code);
        const stockName = klineData?.name || '未知';

        // 从K线数据中获取当前日期的价格
        let currentPrice = pos.cost; // 默认使用成本价
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate); // 转换为 YYYY-MM-DD 格式
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1]; // 收盘价
            }
        }

        const profit = (currentPrice - pos.cost) * pos.quantity;
        const profitRate = ((currentPrice - pos.cost) / pos.cost * 100);

        // 检查是否可卖出
        const buyDateIdx = AppState.availableDates.indexOf(pos.buyDate);
        const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);
        const canSell = currentDateIdx > buyDateIdx;

        return `
            <tr>
                <td class="clickable-cell" onclick="viewPositionChart('${code}')">${code.split('.')[0]}</td>
                <td class="clickable-cell" onclick="viewPositionChart('${code}')">${stockName}</td>
                <td>${pos.quantity}</td>
                <td>¥${pos.cost.toFixed(2)}</td>
                <td>¥${currentPrice.toFixed(2)}</td>
                <td class="${profit >= 0 ? 'profit' : 'loss'}">${profit >= 0 ? '+' : ''}${profit.toFixed(2)}</td>
                <td class="${profitRate >= 0 ? 'profit' : 'loss'}">${profitRate >= 0 ? '+' : ''}${profitRate.toFixed(2)}%</td>
                <td>${formatDate(pos.buyDate)}</td>
                <td>${canSell ? '是' : '否'}</td>
                <td>
                    ${canSell ? `<button class="btn btn-small btn-sell" onclick="quickSell('${code}')">卖出</button>` : '-'}
                </td>
            </tr>
        `;
    }).join('');
}

function quickSell(code) {
    // 检查是否可卖出（T+1）
    const position = AppState.account.positions[code];
    if (!position) {
        showToast('没有该股票持仓', 'warning');
        return;
    }

    const buyDateIdx = AppState.availableDates.indexOf(position.buyDate);
    const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);
    const canSell = currentDateIdx > buyDateIdx;

    if (!canSell) {
        const daysWait = buyDateIdx + 1 - currentDateIdx;
        showToast(`T+1交易，还需等待${daysWait}个交易日`, 'warning');
        return;
    }

    // 获取当前价格（从K线数据获取当前日期的收盘价）
    const klineData = getKlineData(code);
    let currentPrice = position.cost;
    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const dateIdx = klineData.dates.indexOf(targetDate);
        if (dateIdx >= 0 && klineData.values[dateIdx]) {
            currentPrice = klineData.values[dateIdx][1]; // 收盘价
        }
    }

    // 计算卖出金额
    const amount = currentPrice * position.quantity;
    const fees = calculateFees(amount, true);
    const netAmount = amount - fees.total;
    const profit = (currentPrice - position.cost) * position.quantity;
    const profitRate = ((currentPrice - position.cost) / position.cost * 100);

    // 显示确认对话框
    showConfirmModal(
        `卖出 ${klineData?.name || code}\n价格: ¥${currentPrice.toFixed(2)} | 数量: ${position.quantity}股\n手续费: ¥${fees.total.toFixed(2)} | 净得: ¥${netAmount.toFixed(2)}\n盈亏: ${profit >= 0 ? '+' : ''}${profit.toFixed(2)} (${profitRate >= 0 ? '+' : ''}${profitRate.toFixed(2)}%)`,
        () => {
            // 执行卖出
            const tradeRecord = {
                id: generateId(),
                date: AppState.currentDate,
                orderDate: AppState.currentDate,
                code: code,
                name: klineData?.name || '未知',
                type: 'SELL',
                price: currentPrice,
                quantity: position.quantity,
                amount: amount,
                fees: fees.total,
                netAmount: netAmount,
                positionCost: position.cost * position.quantity,
                status: 'COMPLETED'
            };

            AppState.trades.push(tradeRecord);

            // 更新持仓（删除持仓）
            delete AppState.account.positions[code];

            // 更新资金（卖出款冻结，次日解冻到可用资金）
            AppState.account.frozen += netAmount;

            // 更新界面
            renderPositions();
            renderMyPositions();
            updateAccountInfo();

            saveData();

            showToast(`卖出成功，净得 ¥${netAmount.toFixed(2)}元`, 'success');
        }
    );
}

function updateSummaryStats() {
    const completedTrades = AppState.trades.filter(t => t.status === 'COMPLETED' || t.status === 'EXECUTED');
    const sells = completedTrades.filter(t => t.type === 'SELL');
    const buys = completedTrades.filter(t => t.type === 'BUY' || t.type === 'ADD');
    const conditionBuys = completedTrades.filter(t => t.type === 'CONDITION_BUY' && t.status === 'EXECUTED');

    // 合并所有买入记录并按日期排序（FIFO）
    const allBuys = [...buys, ...conditionBuys].sort((a, b) => a.date.localeCompare(b.date));

    // 计算胜率
    let profitCount = 0;
    let lossCount = 0;
    let totalProfit = 0;
    let totalFees = 0;

    // 按日期排序卖出记录
    const sortedSells = [...sells].sort((a, b) => a.date.localeCompare(b.date));

    // 使用FIFO方式匹配买卖记录
    const buyQueue = []; // 存储当前股票的待匹配买入记录
    let currentCode = null;

    sortedSells.forEach(sell => {
        totalFees += sell.fees || 0;

        // 切换到新股票时，清空之前的队列
        if (currentCode !== sell.code) {
            buyQueue.length = 0;
            currentCode = sell.code;
        }

        // 找到对应股票的买入记录（FIFO）
        let remainingQuantity = sell.quantity;
        let totalBuyCost = 0;

        for (let i = 0; i < allBuys.length && remainingQuantity > 0; i++) {
            const buy = allBuys[i];
            if (buy.code === sell.code && !buy.used) {
                const matchQuantity = Math.min(buy.quantity - (buy.usedQuantity || 0), remainingQuantity);
                totalBuyCost += buy.price * matchQuantity;

                if (buy.usedQuantity === undefined) buy.usedQuantity = 0;
                buy.usedQuantity += matchQuantity;
                remainingQuantity -= matchQuantity;

                if (buy.usedQuantity >= buy.quantity) {
                    buy.used = true;
                }
            }
        }

        if (totalBuyCost > 0) {
            const sellRevenue = sell.netAmount;
            const profit = sellRevenue - totalBuyCost;

            if (profit > 0) profitCount++;
            else if (profit < 0) lossCount++;

            totalProfit += profit;
        }
    });

    const totalTrades = profitCount + lossCount;
    const winRate = totalTrades > 0 ? (profitCount / totalTrades * 100) : 0;

    document.getElementById('totalTrades').textContent = totalTrades;
    document.getElementById('profitTrades').textContent = profitCount;
    document.getElementById('lossTrades').textContent = lossCount;
    document.getElementById('winRate').textContent = `${winRate.toFixed(1)}%`;

    const profitEl = document.getElementById('totalProfitLoss');
    profitEl.textContent = `${totalProfit >= 0 ? '+' : ''}${totalProfit.toFixed(2)}`;
    profitEl.className = totalProfit >= 0 ? 'profit' : 'loss';

    document.getElementById('totalFees').textContent = `${totalFees.toFixed(2)} 元`;
}

function getStatusText(status) {
    const statusMap = {
        'PENDING': '<span style="color: #fbbc04;">待执行</span>',
        'COMPLETED': '<span style="color: #34a853;">已完成</span>',
        'FAILED': '<span style="color: #ea4335;">失败</span>',
        'EXECUTED': '<span style="color: #34a853;">已触发</span>',
        'EXPIRED': '<span style="color: #999;">已过期</span>',
        'CANCELLED': '<span style="color: #999;">已取消</span>'
    };
    return statusMap[status] || status;
}

function getNextDate() {
    const currentIdx = AppState.availableDates.indexOf(AppState.currentDate);
    if (currentIdx < AppState.availableDates.length - 1) {
        return AppState.availableDates[currentIdx + 1];
    }
    return null;
}

// ==================== 数据持久化 ====================
function saveData() {
    const saveState = {
        account: AppState.account,
        trades: AppState.trades,
        conditionOrders: AppState.conditionOrders,
        currentDate: AppState.currentDate
    };
    localStorage.setItem('tradingSimulatorData', JSON.stringify(saveState));
}

function loadSavedData() {
    const saved = localStorage.getItem('tradingSimulatorData');
    if (saved) {
        try {
            const data = JSON.parse(saved);
            AppState.account = data.account || AppState.account;
            AppState.trades = data.trades || [];
            AppState.conditionOrders = data.conditionOrders || [];

            // 检查是否有保存的日期，如果有且在当前数据中，则使用保存的日期
            // 否则使用最接近今天的真实交易日
            if (data.currentDate && AppState.availableDates.includes(data.currentDate)) {
                AppState.currentDate = data.currentDate;
            } else {
                // 如果没有保存的日期或保存的日期不在当前数据中，使用智能选择
                AppState.currentDate = findNearestTradingDay(AppState.availableDates);
            }

            document.getElementById('currentDate').value = AppState.currentDate;
        } catch (e) {
            console.error('加载保存数据失败:', e);
            // 如果解析失败，使用智能选择
            AppState.currentDate = findNearestTradingDay(AppState.availableDates);
            document.getElementById('currentDate').value = AppState.currentDate;
        }
    } else {
        // 如果没有保存的数据，使用智能选择
        AppState.currentDate = findNearestTradingDay(AppState.availableDates);
        document.getElementById('currentDate').value = AppState.currentDate;
    }
}

function resetData() {
    if (confirm('确定要重置所有数据吗？这将清除所有交易记录和持仓。')) {
        localStorage.removeItem('tradingSimulatorData');
        location.reload();
    }
}

// ==================== 工具函数 ====================
function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

function formatNumber(num) {
    return num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast show ${type}`;
    setTimeout(() => {
        toast.className = 'toast';
    }, 3000);
}

// ==================== 事件监听初始化 ====================
function initEventListeners() {
    // 连板列表点击事件（委托）
    document.getElementById('ladderList').addEventListener('click', (e) => {
        const item = e.target.closest('.ladder-item');
        if (item) {
            const code = item.dataset.code;
            console.log('Clicked on stock:', code);
            selectStock(code);
        }
    });

    // 股票搜索 - 添加防抖处理
    const searchInput = document.getElementById('stockSearch');
    document.getElementById('btnSearch').addEventListener('click', searchStock);
    searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') searchStock();
    });
    // 输入时实时搜索（防抖）
    let searchDebounceTimer = null;
    searchInput.addEventListener('input', (e) => {
        clearTimeout(searchDebounceTimer);
        const keyword = e.target.value.trim();
        if (!keyword) {
            document.getElementById('searchResults').innerHTML = '<p class="empty-tip">请输入股票代码或名称</p>';
            return;
        }
        searchDebounceTimer = setTimeout(() => {
            searchStock();
        }, 300); // 300ms防抖
    });
    // 条件单按钮
    document.getElementById('btnConditionOrder').addEventListener('click', () => {
        showConfirmModal('确认设置条件单', () => setConditionOrder());
    });

    // 加仓按钮
    document.getElementById('btnAddPosition').addEventListener('click', () => {
        showConfirmModal('确认加仓', () => addPosition());
    });

    // 快捷仓位比例按钮
    document.querySelectorAll('.btn-ratio[data-ratio]').forEach(btn => {
        btn.addEventListener('click', () => {
            const ratio = parseFloat(btn.dataset.ratio);
            const price = parseFloat(document.getElementById('addPrice').value) || 0;
            if (price > 0) {
                const availableFund = parseFloat(document.getElementById('availableFund').textContent.replace(/,/g, ''));
                const maxAmount = availableFund * ratio;
                const maxShares = Math.floor(maxAmount / price / 100) * 100;
                if (maxShares > 0) {
                    document.getElementById('addQuantity').value = maxShares;
                } else {
                    showToast('资金不足，无法买入');
                }
            } else {
                showToast('请先输入买入价格');
            }
        });
    });

    // 卖出按钮
    document.getElementById('btnSell').addEventListener('click', () => {
        showConfirmModal('确认卖出', () => sellStock());
    });

    // 连板天数过滤
    document.getElementById('limitDaysFilter').addEventListener('change', renderLadderList);

    // K线图缩放控制
    document.getElementById('zoomIn').addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'zoom', zoomSize: 20 });
    });
    document.getElementById('zoomOut').addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'zoom', zoomSize: -20 });
    });
    document.getElementById('resetChart').addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'restore' });
    });

    // K线周期切换
    document.getElementById('chartPeriod').addEventListener('change', (e) => {
        if (AppState.selectedStock) {
            updateKlineChart(AppState.selectedStock.code);
        }
    });

    // 底部日期导航按钮 - 使用全局函数
    const prevDayBottom = document.getElementById('prevDayBottom');
    const nextDayBottom = document.getElementById('nextDayBottom');

    if (prevDayBottom) {
        prevDayBottom.onclick = window.goToPrevDay;
    }
    if (nextDayBottom) {
        nextDayBottom.onclick = window.goToNextDay;
    }

    // 重置按钮
    document.getElementById('btnReset').addEventListener('click', showResetModal);
    // 主题切换
    const themeSwitchBtn = document.getElementById('btnThemeSwitch');
    if (themeSwitchBtn) {
        themeSwitchBtn.addEventListener('click', toggleTheme);
    }
    document.getElementById('btnResetNoSave').addEventListener('click', () => resetSystem(false));
    document.getElementById('btnResetSave').addEventListener('click', () => resetSystem(true));
    document.getElementById('btnResetCancel').addEventListener('click', hideResetModal);
}

// 确认对话框
function showConfirmModal(message, onConfirm, tradeDetail = null) {
    const modal = document.getElementById('confirmModal');
    const detailDiv = document.getElementById('modalTradeDetail');

    document.getElementById('modalTitle').textContent = '确认交易';
    document.getElementById('modalMessage').textContent = message;

    // 显示/隐藏交易详情
    if (tradeDetail) {
        detailDiv.style.display = 'block';
        document.getElementById('detailPrice').textContent = '¥' + tradeDetail.price.toFixed(2);
        document.getElementById('detailQuantity').textContent = tradeDetail.quantity + '股';
        document.getElementById('detailAmount').textContent = '¥' + tradeDetail.amount.toFixed(2);
        document.getElementById('detailCommission').textContent = '¥' + tradeDetail.commission.toFixed(2);

        const stampTaxRow = document.getElementById('stampTaxRow');
        if (tradeDetail.stampTax > 0) {
            stampTaxRow.style.display = 'flex';
            document.getElementById('detailStampTax').textContent = '¥' + tradeDetail.stampTax.toFixed(2);
        } else {
            stampTaxRow.style.display = 'none';
        }

        document.getElementById('detailTotal').textContent = '¥' + tradeDetail.total.toFixed(2);
    } else {
        detailDiv.style.display = 'none';
    }

    document.getElementById('modalConfirm').onclick = () => {
        modal.classList.remove('show');
        onConfirm();
    };

    document.getElementById('modalCancel').onclick = () => {
        modal.classList.remove('show');
    };

    modal.classList.add('show');
}

// ==================== 主题切换功能 ====================
function toggleTheme() {
    const html = document.documentElement;
    const currentTheme = html.getAttribute('data-theme');
    const themeIcon = document.getElementById('themeIcon');

    if (currentTheme === 'dark') {
        html.removeAttribute('data-theme');
        localStorage.removeItem('theme');
        // 太阳图标
        themeIcon.innerHTML = `
            <circle cx="12" cy="12" r="5"/>
            <path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>
        `;
        showToast('已切换到亮色模式', 'info');
    } else {
        html.setAttribute('data-theme', 'dark');
        localStorage.setItem('theme', 'dark');
        // 月亮图标
        themeIcon.innerHTML = `
            <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
        `;
        showToast('已切换到暗色模式', 'info');
    }

    // 更新ECharts配置
    if (AppState.klineChart) {
        updateChartTheme();
    }
}

// 更新图表主题
function updateChartTheme() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const textColor = isDark ? '#e0e0e0' : '#333';
    const gridColor = isDark ? '#404040' : '#e0e0e0';

    if (AppState.klineChart) {
        AppState.klineChart.setOption({
            backgroundColor: 'transparent',
            textStyle: { color: textColor },
            xAxis: [{ axisLabel: { color: textColor }, axisLine: { lineStyle: { color: gridColor } } },
                     { axisLine: { lineStyle: { color: gridColor } } }],
            yAxis: [{ axisLabel: { color: textColor }, splitLine: { lineStyle: { color: gridColor, type: 'dashed' } } },
                     { splitLine: { lineStyle: { color: gridColor, type: 'dashed' } } }]
        });
    }
}

// 初始化主题
function initTheme() {
    const savedTheme = localStorage.getItem('theme');
    const themeIcon = document.getElementById('themeIcon');

    if (savedTheme === 'dark') {
        document.documentElement.setAttribute('data-theme', 'dark');
        if (themeIcon) {
            themeIcon.innerHTML = `
                <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
            `;
        }
    }
}

// ==================== 重置功能 ====================
function showResetModal() {
    document.getElementById('resetModal').classList.add('show');
}

function hideResetModal() {
    document.getElementById('resetModal').classList.remove('show');
}

function resetSystem(saveData = false) {
    if (saveData) {
        // 导出数据
        const exportData = {
            trades: AppState.trades,
            conditionOrders: AppState.conditionOrders,
            currentSummary: {
                totalTrades: 0,
                profitTrades: 0,
                lossTrades: 0,
                winRate: 0,
                totalProfit: 0,
                totalFees: 0
            }
        };

        // 计算当前收益统计
        const completedTrades = AppState.trades.filter(t => t.status === 'COMPLETED' || t.status === 'EXECUTED');
        const sells = completedTrades.filter(t => t.type === 'SELL');
        const buys = completedTrades.filter(t => t.type === 'BUY' || t.type === 'ADD');
        const conditionBuys = completedTrades.filter(t => t.type === 'CONDITION_BUY' && t.status === 'EXECUTED');
        const allBuys = [...buys, ...conditionBuys].sort((a, b) => a.date.localeCompare(b.date));

        let profitCount = 0, lossCount = 0, totalProfit = 0, totalFees = 0;
        const sortedSells = [...sells].sort((a, b) => a.date.localeCompare(b.date));
        const buyQueue = [];
        let currentCode = null;

        sortedSells.forEach(sell => {
            totalFees += sell.fees || 0;
            if (currentCode !== sell.code) {
                buyQueue.length = 0;
                currentCode = sell.code;
            }

            let remainingQuantity = sell.quantity;
            let totalBuyCost = 0;

            for (let i = 0; i < allBuys.length && remainingQuantity > 0; i++) {
                const buy = allBuys[i];
                if (buy.code === sell.code && !buy.used) {
                    const matchQuantity = Math.min(buy.quantity - (buy.usedQuantity || 0), remainingQuantity);
                    totalBuyCost += buy.price * matchQuantity;

                    if (buy.usedQuantity === undefined) buy.usedQuantity = 0;
                    buy.usedQuantity += matchQuantity;
                    remainingQuantity -= matchQuantity;

                    if (buy.usedQuantity >= buy.quantity) {
                        buy.used = true;
                    }
                }
            }

            if (totalBuyCost > 0) {
                const sellRevenue = sell.netAmount;
                const profit = sellRevenue - totalBuyCost;
                if (profit > 0) profitCount++;
                else if (profit < 0) lossCount++;
                totalProfit += profit;
            }
        });

        exportData.currentSummary.totalTrades = profitCount + lossCount;
        exportData.currentSummary.profitTrades = profitCount;
        exportData.currentSummary.lossTrades = lossCount;
        exportData.currentSummary.winRate = profitCount + lossCount > 0 ? (profitCount / (profitCount + lossCount) * 100) : 0;
        exportData.currentSummary.totalProfit = totalProfit;
        exportData.currentSummary.totalFees = totalFees;

        // 下载文件
        const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `交易记录_${AppState.currentDate}.json`;
        a.click();
        URL.revokeObjectURL(url);

        showToast('数据已保存，系统重置中...', 'success');
    }

    // 重置账户状态
    AppState.account = {
        initialFund: 100000,
        available: 100000,
        positions: {},
        frozen: 0
    };

    // 清空交易记录和条件单
    AppState.trades = [];
    AppState.conditionOrders = [];

    // 清空选中股票
    AppState.selectedStock = null;

    // 清空本地存储（使用正确的键名）
    localStorage.removeItem('tradingSimulatorData');

    // 先关闭弹窗
    hideResetModal();
    showToast('系统已重置', 'success');

    // 更新界面
    updateAccountInfo();
    renderMyPositions();
    renderPositions();
    renderTradeRecords();
    renderConditionOrders();
    renderNextDayActions();
    updateSummaryStats();

    // 更新股票信息显示
    document.getElementById('selectedStock').innerHTML = '<p>请从左侧选择一只股票</p>';
    document.getElementById('stockName').textContent = '选择一只股票查看K线';
    document.getElementById('stockCode').textContent = '--';
    document.getElementById('currentPrice').textContent = '--';
    document.getElementById('limitUpDays').textContent = '--';
}

// ==================== 标签页初始化 ====================
function initTabs() {
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const tabId = btn.dataset.tab;

            // 切换按钮状态
            tabBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // 切换内容
            tabContents.forEach(content => {
                content.classList.remove('active');
                if (content.id === tabId + 'Tab') {
                    content.classList.add('active');
                }
            });

            // 如果是持仓标签，刷新持仓显示
            if (tabId === 'positions') {
                renderPositions();
            }
        });
    });

    // 初始渲染持仓
    renderPositions();
    renderTradeRecords();
    renderConditionOrders();
}