/**
 * 连板股票模拟交易训练系统 - 移动端核心逻辑
 */

// ==================== K线数据获取辅助函数 ====================
function getKlineData(code) {
    const coreData = window.KLINE_DATA_CORE?.[code];
    if (coreData) return coreData;
    const globalData = window.KLINE_DATA_GLOBAL?.[code];
    return globalData || null;
}

function getAllKlineData() {
    const core = window.KLINE_DATA_CORE || {};
    const global = window.KLINE_DATA_GLOBAL || {};
    return { ...global, ...core };
}

// ==================== 全局状态管理 ====================
const AppState = {
    currentDate: null,
    availableDates: [],
    account: {
        initialFund: 100000,
        available: 100000,
        positions: {},
        frozen: 0,
    },
    trades: [],
    conditionOrders: [],
    selectedStock: null,
    klineChart: null,
    currentPage: 'pageLadder',
};

// ==================== 常量配置 ====================
const CONFIG = {
    FEES: {
        STAMP_TAX: 0.001,
        COMMISSION: 0.0003,
        MIN_COMMISSION: 5,
    },
    LIMIT_UP: 0.10,
    LIMIT_DOWN: 0.10,
    ST_LIMIT: 0.05,
    TRADING_UNIT: 100,
};

// ==================== 初始化 ====================
document.addEventListener('DOMContentLoaded', () => {
    initApp();
});

async function initApp() {
    try {
        await waitForData();
        initDateSelector();
        loadSavedData();
        renderLadderList();
        initKlineChart();
        initNavigation();
        initEventListeners();
        updateAccountInfo();
        renderMyPositions();
        console.log('移动端交易系统初始化完成');
    } catch (error) {
        console.error('初始化失败:', error);
        showToast('系统初始化失败: ' + error.message, 'error');
    }
}

function waitForData() {
    return new Promise((resolve, reject) => {
        const checkData = () => {
            if (window.LADDER_DATA && window.KLINE_DATA_CORE) {
                resolve();
            } else {
                setTimeout(checkData, 100);
            }
        };
        setTimeout(() => reject(new Error('数据加载超时')), 10000);
        checkData();
    });
}

// ==================== 日期管理 ====================
function initDateSelector() {
    const dates = Object.keys(window.LADDER_DATA).sort();
    AppState.availableDates = dates;
    AppState.currentDate = findNearestTradingDay(dates);

    const select = document.getElementById('currentDate');
    dates.forEach(date => {
        const option = document.createElement('option');
        option.value = date;
        option.textContent = formatDate(date);
        select.appendChild(option);
    });
    select.value = AppState.currentDate;

    select.addEventListener('change', (e) => switchDate(e.target.value));

    document.getElementById('prevDay').addEventListener('click', () => {
        const idx = dates.indexOf(AppState.currentDate);
        if (idx > 0) switchDate(dates[idx - 1]);
    });

    document.getElementById('nextDay').addEventListener('click', () => {
        const idx = dates.indexOf(AppState.currentDate);
        if (idx < dates.length - 1) switchDate(dates[idx + 1]);
    });
}

function findNearestTradingDay(availableDates) {
    const today = new Date();
    const todayStr = formatDateForComparison(today);
    if (availableDates.includes(todayStr)) return todayStr;
    for (let i = availableDates.length - 1; i >= 0; i--) {
        if (availableDates[i] <= todayStr) return availableDates[i];
    }
    return availableDates[0];
}

function formatDateForComparison(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
}

function switchDate(newDate) {
    executePendingOrders(newDate);
    AppState.currentDate = newDate;
    document.getElementById('currentDate').value = newDate;

    renderLadderList();
    if (AppState.selectedStock) updateStockPanel(AppState.selectedStock);
    updatePositionsInfo();
    renderMyPositions();
    updateAccountInfo();
    renderConditionOrders();
    saveData();
}

function formatDate(dateStr) {
    return `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`;
}

// ==================== 页面导航 ====================
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item');

    navItems.forEach(item => {
        item.addEventListener('click', () => {
            const pageId = item.dataset.page;
            switchPage(pageId);
        });
    });
}

function switchPage(pageId) {
    // 隐藏所有页面
    document.querySelectorAll('.page').forEach(page => {
        page.classList.remove('active');
    });

    // 显示目标页面
    const targetPage = document.getElementById(pageId);
    if (targetPage) {
        targetPage.classList.add('active');
        AppState.currentPage = pageId;
    }

    // 更新导航状态
    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.page === pageId);
    });

    // 页面特定操作
    if (pageId === 'pageChart' && AppState.selectedStock) {
        setTimeout(() => {
            if (AppState.klineChart) AppState.klineChart.resize();
        }, 100);
    } else if (pageId === 'pageRecords') {
        renderMobileTrades();
    } else if (pageId === 'pagePositions') {
        renderMobilePositions();
    } else if (pageId === 'pageTrade') {
        updateTradePanel();
    }
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

    const allStocks = [];
    Object.keys(dateData).forEach(key => {
        allStocks.push(...(dateData[key] || []));
    });

    let filteredStocks = filter === 0 ? allStocks : allStocks.filter(s => {
        if (filter === 5) return s.limitUpDays >= 5;
        return s.limitUpDays === filter;
    });

    filteredStocks.sort((a, b) => b.limitUpDays - a.limitUpDays);

    if (filteredStocks.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无符合条件的股票</p>';
        return;
    }

    container.innerHTML = filteredStocks.map(stock => {
        const limitClass = stock.limitUpDays >= 5 ? 'limit-up-5' :
            stock.limitUpDays >= 3 ? 'limit-up-4' :
            stock.limitUpDays >= 2 ? 'limit-up-3' : 'limit-up-2';
        const selected = AppState.selectedStock?.code === stock.code ? 'selected' : '';
        const limitDaysClass = stock.limitUpDays >= 5 ? 'several' : '';

        return `
            <div class="ladder-item ${limitClass} ${selected}" onclick="selectStock('${stock.code}')">
                <div class="ladder-main">
                    <div class="stock-code">${stock.code.split('.')[0]} ${stock.name}</div>
                    <span class="limit-days ${limitDaysClass}">${stock.limitUpDays}连板</span>
                </div>
                <div class="ladder-price-box">
                    <div class="price">¥${stock.price.toFixed(2)}</div>
                </div>
            </div>
        `;
    }).join('');
}

// 绑定筛选事件
document.getElementById('limitDaysFilter')?.addEventListener('change', renderLadderList);

// ==================== 股票选择 ====================
function selectStock(code) {
    const stockData = getStockData(code);
    if (!stockData) {
        showToast('股票数据不存在', 'error');
        return;
    }

    AppState.selectedStock = stockData;
    renderLadderList();
    updateStockPanel(stockData);

    // 自动跳转到K线页面
    const chartBtn = document.querySelector('.nav-item[data-page="pageChart"]');
    if (chartBtn) chartBtn.click();
}

function getStockData(code) {
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
    chartDom.style.width = '100%';
    chartDom.style.height = '100%';

    AppState.klineChart = echarts.init(chartDom);

    AppState.klineChart.setOption({
        title: {
            text: '请从天梯选择股票查看K线',
            left: 'center',
            top: 'center',
            textStyle: { color: '#999', fontSize: 14 }
        },
        grid: { hidden: true },
        xAxis: { hidden: true },
        yAxis: { hidden: true }
    });

    window.addEventListener('resize', () => {
        AppState.klineChart?.resize();
    });
}

function updateKlineChart(code) {
    const klineData = getKlineData(code);

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
                    _renderKlineChartMobile(code, data);
                } else {
                    console.error('Failed to load K-line data');
                    showKlineLoadErrorMobile(code);
                }
            }).catch(err => {
                console.error('Error loading K-line data:', err);
                showKlineLoadErrorMobile(code);
            });
        }
        return;
    }

    // 使用现有数据渲染
    _renderKlineChartMobile(code, klineData);
}

// 内部渲染K线图的函数（移动端版本）
function _renderKlineChartMobile(code, klineData) {
    const dates = klineData.dates;
    const values = klineData.values;
    const currentDateFormatted = AppState.currentDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');

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

    if (validDates.length === 0) return;

    const candlestickData = validValues.map(v => [v[0], v[1], v[2], v[3]]);
    const volumes = klineData.volumes || [];
    const validVolumes = currentDateIdx >= 0 ? volumes.slice(0, currentDateIdx + 1) : volumes;

    const option = {
        title: { show: false },
        tooltip: {
            trigger: 'item',
            axisPointer: { type: 'cross' },
            formatter: function(params) {
                if (!params || !params.data) return '';
                const data = params.data;
                const dateIndex = params.dataIndex;
                const volume = validVolumes[dateIndex] || 0;

                let open, close, low, high;
                if (Array.isArray(data) && data.length >= 5) {
                    open = data[1];
                    close = data[2];
                    low = data[3];
                    high = data[4];
                } else if (Array.isArray(data) && data.length === 4) {
                    open = data[0];
                    close = data[1];
                    low = data[2];
                    high = data[3];
                } else {
                    return '';
                }

                let prevClose = 0;
                if (dateIndex > 0 && validValues[dateIndex - 1]) {
                    prevClose = validValues[dateIndex - 1][1];
                }
                if (!prevClose || prevClose === 0) {
                    prevClose = close;
                }

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
            { left: '8%', right: '8%', top: '8%', height: '45%' },
            { left: '8%', right: '8%', top: '60%', height: '18%' }
        ],
        xAxis: [
            {
                type: 'category',
                data: validDates,
                boundaryGap: false,
                axisLine: { onZero: false },
                axisLabel: { fontSize: 10 },
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
                axisLabel: { fontSize: 10 }
            },
            {
                scale: true,
                splitNumber: 2,
                axisLabel: { show: false },
                gridIndex: 1,
                valueFormatter: function(value) {
                    return (value / 10000).toFixed(0) + '万';
                }
            }
        ],
        dataZoom: [
            {
                type: 'inside',
                startValue: Math.max(0, validDates.length - 30),
                endValue: validDates.length - 1,
                xAxisIndex: [0, 1]
            },
            {
                type: 'slider',
                bottom: 5,
                startValue: Math.max(0, validDates.length - 30),
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
}

// 显示K线加载错误（移动端版本）
function showKlineLoadErrorMobile(code) {
    if (AppState.klineChart) {
        AppState.klineChart.setOption({
            title: {
                text: `${code} 数据不可用`,
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

// ==================== 股票面板更新 ====================
function updateStockPanel(stock) {
    const klineData = getKlineData(stock.code);

    // 更新头部信息
    document.getElementById('stockName').textContent = stock.name;
    document.getElementById('stockCode').textContent = stock.code;
    document.getElementById('currentPrice').textContent = `¥${stock.price.toFixed(2)}`;

    document.getElementById('detailPrice').textContent = `¥${stock.price.toFixed(2)}`;
    document.getElementById('detailLimitUp').textContent = `${stock.limitUpDays}连板`;

    // 涨停价
    const isST = stock.name.includes('ST');
    const limitRate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_UP;
    const limitUpPrice = Math.round(stock.price * (1 + limitRate) * 100) / 100;
    document.getElementById('limitUpPrice').value = limitUpPrice.toFixed(2);

    // 次日开盘信息
    let nextDayOpenChangePct = 0;
    let nextDayOpenPrice = stock.price;

    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const currentDateIdx = klineData.dates.indexOf(targetDate);

        if (currentDateIdx >= 0 && currentDateIdx + 1 < klineData.dates.length) {
            const currentClosePrice = klineData.values[currentDateIdx][1];
            const nextDayOpenPriceVal = klineData.values[currentDateIdx + 1][0];
            nextDayOpenChangePct = ((nextDayOpenPriceVal - currentClosePrice) / currentClosePrice) * 100;
            nextDayOpenPrice = nextDayOpenPriceVal;
        }
    }

    const openChangeEl = document.getElementById('nextDayOpenChange');
    openChangeEl.textContent = `${nextDayOpenChangePct >= 0 ? '+' : ''}${nextDayOpenChangePct.toFixed(2)}%`;
    openChangeEl.className = nextDayOpenChangePct >= 0 ? 'positive' : 'negative';
    document.getElementById('nextDayOpenPrice').textContent = `¥${nextDayOpenPrice.toFixed(2)}`;

    document.getElementById('addPrice').value = nextDayOpenPrice.toFixed(2);

    // 更新选中股票卡片
    document.getElementById('selectedStock').innerHTML = `
        <div class="stock">${stock.name}</div>
        <div class="code">${stock.code} | ¥${stock.price.toFixed(2)} | ${stock.limitUpDays}连板</div>
    `;

    // 更新持仓信息
    updatePositionInfo(stock.code);
    updateTradePanel();

    // 更新K线图
    updateKlineChart(stock.code);
}

function updatePositionInfo(code) {
    const position = AppState.account.positions[code];
    const klineData = getKlineData(code);

    let currentPrice = position ? position.cost : 0;
    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const dateIdx = klineData.dates.indexOf(targetDate);
        if (dateIdx >= 0 && klineData.values[dateIdx]) {
            currentPrice = klineData.values[dateIdx][1];
        }
    }

    const quantityEl = document.getElementById('holdQuantity');
    const costEl = document.getElementById('holdCost');
    const profitEl = document.getElementById('holdProfit');
    const sellTip = document.getElementById('sellTip');
    const sellSection = document.getElementById('sellSection');

    if (position) {
        quantityEl.textContent = position.quantity;
        costEl.textContent = `¥${position.cost.toFixed(2)}`;

        const profit = (currentPrice - position.cost) * position.quantity;
        profitEl.textContent = `${profit >= 0 ? '+' : ''}${profit.toFixed(2)}`;
        profitEl.className = profit >= 0 ? 'profit' : 'loss';

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

        sellSection.style.display = 'block';
    } else {
        sellSection.style.display = 'none';
    }
}

function updatePositionsInfo() {
    Object.keys(AppState.account.positions).forEach(code => {
        updatePositionInfo(code);
    });
}

function updateTradePanel() {
    if (AppState.selectedStock) {
        const code = AppState.selectedStock.code;
        updatePositionInfo(code);
    }
}

// ==================== 我的持仓 ====================
function renderMyPositions() {
    const container = document.getElementById('myPositionsList');
    const positions = AppState.account.positions;

    if (Object.keys(positions).length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无持仓</p>';
        return;
    }

    container.innerHTML = Object.entries(positions).map(([code, pos]) => {
        const klineData = getKlineData(code);
        const stockName = klineData?.name || '未知';

        let currentPrice = pos.cost;
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate);
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1];
            }
        }

        const profit = (currentPrice - pos.cost) * pos.quantity;
        const profitRate = ((currentPrice - pos.cost) / pos.cost * 100);
        const profitClass = profit >= 0 ? 'profit' : 'loss';

        return `
            <div class="my-position-item" onclick="viewPositionAndTrade('${code}')">
                <div class="item-header">
                    <span class="code">${code.split('.')[0]} ${stockName}</span>
                    <span class="quantity">${pos.quantity}股</span>
                </div>
                <div class="price-info">
                    <span>成本 ¥${pos.cost.toFixed(2)}</span>
                    <span class="${profitClass}">${profit >= 0 ? '+' : ''}${profit.toFixed(2)} (${profitRate.toFixed(1)}%)</span>
                </div>
            </div>
        `;
    }).join('');
}

function renderMobilePositions() {
    const container = document.getElementById('positionsListBody');
    const positions = AppState.account.positions;

    document.getElementById('positionCount').textContent = Object.keys(positions).length;

    if (Object.keys(positions).length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无持仓</p>';
        return;
    }

    container.innerHTML = Object.entries(positions).map(([code, pos]) => {
        const klineData = getKlineData(code);
        const stockName = klineData?.name || '未知';

        let currentPrice = pos.cost;
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate);
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1];
            }
        }

        const profit = (currentPrice - pos.cost) * pos.quantity;
        const profitRate = ((currentPrice - pos.cost) / pos.cost * 100);
        const profitClass = profit >= 0 ? 'profit' : 'loss';

        const buyDateIdx = AppState.availableDates.indexOf(pos.buyDate);
        const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);
        const canSell = currentDateIdx > buyDateIdx;

        return `
            <div class="position-item-action">
                <div class="header">
                    <span class="code-name">${code.split('.')[0]} ${stockName}</span>
                    <span class="profit-info ${profitClass}">${profit >= 0 ? '+' : ''}${profit.toFixed(2)} (${profitRate.toFixed(1)}%)</span>
                </div>
                <div class="detail-row">
                    <span>持仓: ${pos.quantity}股</span>
                    <span>成本: ¥${pos.cost.toFixed(2)}</span>
                </div>
                <div class="detail-row">
                    <span>现价: ¥${currentPrice.toFixed(2)}</span>
                    <span class="can-sell">${canSell ? '✓可卖出' : '✗不可卖'}</span>
                </div>
                ${canSell ? `<button class="quick-sell-btn" onclick="mobileQuickSell('${code}')">一键卖出</button>` : ''}
            </div>
        `;
    }).join('');
}

function viewPositionAndTrade(code) {
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

    // 跳转到交易页面
    const tradeBtn = document.querySelector('.nav-item[data-page="pageTrade"]');
    if (tradeBtn) tradeBtn.click();
}

function mobileQuickSell(code) {
    const position = AppState.account.positions[code];
    if (!position) {
        showToast('没有该股票持仓', 'warning');
        return;
    }

    const buyDateIdx = AppState.availableDates.indexOf(position.buyDate);
    const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);
    const canSell = currentDateIdx > buyDateIdx;

    if (!canSell) {
        showToast('T+1交易，今日买入不可卖出', 'warning');
        return;
    }

    const klineData = getKlineData(code);
    let currentPrice = position.cost;
    if (klineData && klineData.dates && klineData.values) {
        const targetDate = formatDate(AppState.currentDate);
        const dateIdx = klineData.dates.indexOf(targetDate);
        if (dateIdx >= 0 && klineData.values[dateIdx]) {
            currentPrice = klineData.values[dateIdx][1];
        }
    }

    const amount = currentPrice * position.quantity;
    const fees = calculateFees(amount, true);
    const netAmount = amount - fees.total;
    const profit = (currentPrice - position.cost) * position.quantity;
    const profitRate = ((currentPrice - position.cost) / position.cost * 100);

    showConfirmModal(
        `卖出 ${klineData?.name || code}\n价格: ¥${currentPrice.toFixed(2)} | 数量: ${position.quantity}股\n手续费: ¥${fees.total.toFixed(2)} | 净得: ¥${netAmount.toFixed(2)}\n盈亏: ${profit >= 0 ? '+' : ''}${profit.toFixed(2)} (${profitRate >= 0 ? '+' : ''}${profitRate.toFixed(2)}%)`,
        () => {
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
            delete AppState.account.positions[code];

            updateAccountInfo();
            renderMyPositions();
            renderMobilePositions();
            saveData();

            showToast(`卖出成功，净得 ¥${netAmount.toFixed(2)}元`, 'success');
        }
    );
}

// ==================== 股票搜索 ====================
function searchStock() {
    const keyword = document.getElementById('stockSearch').value.trim().toUpperCase();
    if (!keyword) {
        document.getElementById('searchResults').innerHTML = '<p class="empty-tip">请输入股票代码或名称</p>';
        return;
    }

    const results = [];
    const klineData = getAllKlineData();

    for (const [code, data] of Object.entries(klineData)) {
        const codeMatch = code.toUpperCase().includes(keyword);
        const nameMatch = data.name && data.name.toUpperCase().includes(keyword);

        if (codeMatch || nameMatch) {
            const values = data.values || [];
            const currentPrice = values.length > 0 ? values[values.length - 1][1] : 0;
            results.push({ code: code, name: data.name || '未知', price: currentPrice });
        }
    }

    const container = document.getElementById('searchResults');
    if (results.length === 0) {
        container.innerHTML = '<p class="empty-tip">未找到相关股票</p>';
        return;
    }

    container.innerHTML = results.slice(0, 20).map(stock => `
        <div class="search-result-item" onclick="selectStockByCode('${stock.code}')">
            <div class="item-header">
                <span class="code">${stock.code}</span>
                <span class="price">¥${stock.price.toFixed(2)}</span>
            </div>
            <div class="name">${stock.name}</div>
        </div>
    `).join('');
}

function selectStockByCode(code) {
    const klineData = getKlineData(code);
    if (!klineData) {
        showToast('股票数据不存在', 'error');
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

    document.getElementById('searchResults').innerHTML = '';
    document.getElementById('stockSearch').value = '';

    updateStockPanel(stock);

    const chartBtn = document.querySelector('.nav-item[data-page="pageChart"]');
    if (chartBtn) chartBtn.click();

    showToast(`已选择: ${stock.name}`, 'success');
}

// ==================== 交易逻辑 ====================
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
    AppState.account.frozen += amount;

    showToast(`条件单已设置，将于${formatDate(nextDate)}监控执行`, 'success');
    saveData();
    renderConditionOrders();
}

function addPosition() {
    const code = AppState.selectedStock?.code;
    if (!code) {
        showToast('请先选择股票', 'warning');
        return;
    }

    const price = parseFloat(document.getElementById('addPrice').value);
    const quantity = parseInt(document.getElementById('addQuantity').value);

    if (!price || price <= 0) {
        showToast('请输入有效的加仓价格', 'warning');
        return;
    }

    if (!quantity || quantity % CONFIG.TRADING_UNIT !== 0) {
        showToast(`加仓股数必须是${CONFIG.TRADING_UNIT}的整数倍`, 'warning');
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
        fees: calculateFees(amount).total,
        status: 'PENDING'
    };

    AppState.trades.push(tradeRecord);
    AppState.account.frozen += amount;

    showToast(`加仓委托已提交，将于${formatDate(nextDate)}执行`, 'success');
    saveData();
}

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

    if (!quantity || quantity <= 0 || quantity > position.quantity) {
        showToast('卖出股数不能超过持仓', 'warning');
        return;
    }

    const buyDateIdx = AppState.availableDates.indexOf(position.buyDate);
    const currentDateIdx = AppState.availableDates.indexOf(AppState.currentDate);

    if (currentDateIdx <= buyDateIdx) {
        showToast('T+1交易制度，今日买入不可卖出', 'warning');
        return;
    }

    const amount = price * quantity;
    const fees = calculateFees(amount, true);
    const netAmount = amount - fees.total;

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

    showToast(`卖出成功，款项已到账`, 'success');
    saveData();
    renderMyPositions();
    updateAccountInfo();
    updatePositionInfo(code);
}

// ==================== 订单执行 ====================
function executePendingOrders(executeDate) {
    AppState.trades.forEach(trade => {
        if (trade.status === 'PENDING' && trade.date === executeDate) {
            executeTrade(trade);
        }
    });

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
        return;
    }

    const targetDate = formatDate(trade.date);
    const dateIdx = klineData.dates.findIndex(d => d === targetDate);
    if (dateIdx < 0) {
        trade.status = 'FAILED';
        return;
    }

    const dayData = klineData.values[dateIdx];
    const actualPrice = dayData[0];

    if (trade.type === 'BUY' || trade.type === 'ADD') {
        trade.actualPrice = actualPrice;
        trade.actualAmount = actualPrice * trade.quantity;
        trade.fees = calculateFees(trade.actualAmount).total;
        trade.status = 'COMPLETED';

        const totalDeduction = trade.actualAmount + trade.fees;
        AppState.account.frozen -= trade.amount;
        AppState.account.frozen += (trade.amount - totalDeduction);
        AppState.account.available -= totalDeduction;

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

        showToast(`${trade.name} 买入成交，净得 ¥${trade.actualAmount.toFixed(2)}元`, 'success');
        updateAccountInfo();
        renderMyPositions();
    }
}

function executeConditionOrder(order) {
    const klineData = getKlineData(order.code);
    if (!klineData) {
        order.status = 'FAILED';
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        return;
    }

    const targetDate = formatDate(order.date);
    const dateIdx = klineData.dates.findIndex(d => d === targetDate);
    if (dateIdx < 0) {
        order.status = 'FAILED';
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        return;
    }

    const dayData = klineData.values[dateIdx];
    const high = dayData[3];

    if (high >= order.triggerPrice) {
        const actualPrice = order.triggerPrice;

        order.actualPrice = actualPrice;
        order.actualAmount = actualPrice * order.quantity;
        order.fees = calculateFees(order.actualAmount).total;
        order.status = 'EXECUTED';

        const totalDeduction = order.actualAmount + order.fees;
        AppState.account.frozen -= order.amount;
        AppState.account.frozen += (order.amount - totalDeduction);
        AppState.account.available -= totalDeduction;

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

        showToast(`条件单触发：${order.name} 买入 ${order.quantity}股`, 'success');
        updateAccountInfo();
        renderMyPositions();
    } else {
        order.status = 'EXPIRED';
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        showToast(`条件单过期：${order.name} 未触及涨停价`, 'warning');
    }
}

function cancelConditionOrder(orderId) {
    const order = AppState.conditionOrders.find(o => o.id === orderId);
    if (order && order.status === 'PENDING') {
        order.status = 'CANCELLED';
        AppState.account.frozen -= order.amount;
        AppState.account.available += order.amount;
        showToast('条件单已取消', 'success');
        saveData();
        renderConditionOrders();
        updateAccountInfo();
    }
}

// ==================== 条件单渲染 ====================
function renderConditionOrders() {
    const container = document.getElementById('conditionOrdersList');
    const pendingOrders = AppState.conditionOrders.filter(o => o.status === 'PENDING');

    if (pendingOrders.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无条件单</p>';
        return;
    }

    container.innerHTML = pendingOrders.map(order => `
        <div class="condition-order-item">
            <div class="order-header">
                <span class="code">${order.code.split('.')[0]} ${order.name}</span>
                <span class="status">待执行</span>
            </div>
            <div class="order-detail">
                触发价: ¥${order.triggerPrice.toFixed(2)} | 数量: ${order.quantity}股
            </div>
            <div class="order-detail">执行日: ${formatDate(order.date)}</div>
            <button class="cancel-btn" onclick="cancelConditionOrder('${order.id}')">取消</button>
        </div>
    `).join('');
}

// ==================== 交易记录 ====================
function renderMobileTrades() {
    const container = document.getElementById('tradesList');

    if (AppState.trades.length === 0) {
        container.innerHTML = '<p class="empty-tip">暂无交易记录</p>';
        return;
    }

    const sortedTrades = [...AppState.trades].sort((a, b) => {
        return (b.date + b.orderDate).localeCompare(a.date + a.orderDate);
    });

    container.innerHTML = sortedTrades.map(trade => {
        const typeClass = trade.type === 'BUY' || trade.type === 'ADD' ? 'buy' : 'sell';
        const typeText = trade.type === 'BUY' ? '买入' : trade.type === 'ADD' ? '加仓' : '卖出';

        return `
            <div class="trade-item">
                <div class="trade-item-header">
                    <span class="stock-name">${trade.name}</span>
                    <span class="trade-type ${typeClass}">${typeText}</span>
                </div>
                <div class="trade-item-detail">
                    <span>代码: ${trade.code.split('.')[0]}</span>
                    <span>价格: ¥${trade.price.toFixed(2)}</span>
                    <span>数量: ${trade.quantity}股</span>
                    <span>金额: ¥${trade.amount.toFixed(0)}</span>
                </div>
                <div class="trade-item-status">
                    <span class="${trade.status === 'PENDING' ? 'pending' : 'completed'}">${getStatusTextShort(trade.status)}</span>
                </div>
            </div>
        `;
    }).join('');

    updateMobileSummary();
}

function renderMobileSummary() {
    document.getElementById('recordContent').querySelectorAll('.record-section').forEach(section => {
        section.classList.remove('active');
    });
    document.getElementById('summaryContent').classList.add('active');

    document.getElementById('tabRecords').classList.remove('active');
    document.getElementById('tabSummary').classList.add('active');

    updateMobileSummary();
}

function updateMobileSummary() {
    const completedTrades = AppState.trades.filter(t => t.status === 'COMPLETED' || t.status === 'EXECUTED');
    const sells = completedTrades.filter(t => t.type === 'SELL');
    const buys = completedTrades.filter(t => t.type === 'BUY' || t.type === 'ADD');
    const conditionBuys = completedTrades.filter(t => t.type === 'CONDITION_BUY' && t.status === 'EXECUTED');
    const allBuys = [...buys, ...conditionBuys].sort((a, b) => a.date.localeCompare(b.date));

    let profitCount = 0, lossCount = 0, totalProfit = 0, totalFees = 0;
    const sortedSells = [...sells].sort((a, b) => a.date.localeCompare(b.date));

    sortedSells.forEach(sell => {
        totalFees += sell.fees || 0;

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
    profitEl.className = `value ${totalProfit >= 0 ? 'profit' : 'loss'}`;

    document.getElementById('totalFees').textContent = `¥${totalFees.toFixed(2)}`;
}

function switchRecordTab(tab) {
    document.getElementById('tabRecords').classList.toggle('active', tab === 'records');
    document.getElementById('tabSummary').classList.toggle('active', tab === 'summary');

    document.getElementById('recordsContent').classList.toggle('active', tab === 'records');
    document.getElementById('summaryContent').classList.toggle('active', tab === 'summary');

    if (tab === 'records') {
        renderMobileTrades();
    } else {
        updateMobileSummary();
    }
}

function getStatusTextShort(status) {
    const statusMap = {
        'PENDING': '待执行',
        'COMPLETED': '已完成',
        'FAILED': '失败',
        'EXECUTED': '已触发',
        'EXPIRED': '已过期',
        'CANCELLED': '已取消'
    };
    return statusMap[status] || status;
}

// ==================== 账户信息 ====================
function updateAccountInfo() {
    let positionValue = 0;
    Object.keys(AppState.account.positions).forEach(code => {
        const position = AppState.account.positions[code];
        const klineData = getKlineData(code);

        let currentPrice = position.cost;
        if (klineData && klineData.dates && klineData.values) {
            const targetDate = formatDate(AppState.currentDate);
            const dateIdx = klineData.dates.indexOf(targetDate);
            if (dateIdx >= 0 && klineData.values[dateIdx]) {
                currentPrice = klineData.values[dateIdx][1];
            }
        }

        positionValue += position.quantity * currentPrice;
    });

    const totalAsset = AppState.account.available + positionValue;
    const totalReturn = ((totalAsset - AppState.account.initialFund) / AppState.account.initialFund * 100);

    document.getElementById('availableFund').textContent = formatNumber(AppState.account.available);
    document.getElementById('totalAsset').textContent = formatNumber(totalAsset);

    const returnEl = document.getElementById('totalReturn');
    returnEl.textContent = `${totalReturn >= 0 ? '+' : ''}${totalReturn.toFixed(2)}%`;
    returnEl.style.color = totalReturn >= 0 ? 'var(--success-color)' : 'var(--danger-color)';
}

// ==================== 数据持久化 ====================
function saveData() {
    const saveState = {
        account: AppState.account,
        trades: AppState.trades,
        conditionOrders: AppState.conditionOrders,
        currentDate: AppState.currentDate
    };
    localStorage.setItem('tradingSimulatorMobile', JSON.stringify(saveState));
}

function loadSavedData() {
    const saved = localStorage.getItem('tradingSimulatorMobile');
    if (saved) {
        try {
            const data = JSON.parse(saved);
            AppState.account = data.account || AppState.account;
            AppState.trades = data.trades || [];
            AppState.conditionOrders = data.conditionOrders || [];

            if (data.currentDate && AppState.availableDates.includes(data.currentDate)) {
                AppState.currentDate = data.currentDate;
            }
            document.getElementById('currentDate').value = AppState.currentDate;
        } catch (e) {
            console.error('加载保存数据失败:', e);
        }
    }
}

// 重置功能 - 简化版移动端
function resetMobile() {
    if (confirm('确定要重置所有数据吗？这将清除所有交易记录和持仓。')) {
        AppState.account = {
            initialFund: 100000,
            available: 100000,
            positions: {},
            frozen: 0
        };
        AppState.trades = [];
        AppState.conditionOrders = [];
        AppState.selectedStock = null;
        localStorage.removeItem('tradingSimulatorMobile');

        updateAccountInfo();
        renderMyPositions();
        renderMobilePositions();
        renderMobileTrades();
        renderConditionOrders();

        document.getElementById('selectedStock').innerHTML = '<p>请选择股票</p>';
        document.getElementById('stockName').textContent = '选择股票';
        document.getElementById('stockCode').textContent = '--';

        showToast('系统已重置', 'success');
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

function showConfirmModal(message, onConfirm) {
    const modal = document.getElementById('confirmModal');
    document.getElementById('modalTitle').textContent = '确认';
    document.getElementById('modalMessage').textContent = message;

    document.getElementById('modalConfirm').onclick = () => {
        modal.classList.remove('show');
        onConfirm();
    };

    document.getElementById('modalCancel').onclick = () => {
        modal.classList.remove('show');
    };

    modal.classList.add('show');
}

// 切换交易面板展开/收起
function toggleSection(sectionId) {
    const content = document.getElementById(sectionId);
    const icon = document.previousElementSibling?.querySelector('.toggle-icon');

    if (content) {
        content.classList.toggle('expanded');
        if (icon) {
            icon.classList.toggle('collapsed');
        }
    }
}

// ==================== 事件监听 ====================
function initEventListeners() {
    // 搜索
    document.getElementById('btnSearch')?.addEventListener('click', searchStock);
    document.getElementById('stockSearch')?.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') searchStock();
    });
    // 输入时实时搜索（防抖）
    let searchDebounceTimer = null;
    document.getElementById('stockSearch')?.addEventListener('input', (e) => {
        clearTimeout(searchDebounceTimer);
        const keyword = e.target.value.trim();
        if (!keyword) {
            document.getElementById('searchResults').innerHTML = '<p class="empty-tip">输入股票代码或名称进行搜索</p>';
            return;
        }
        searchDebounceTimer = setTimeout(() => {
            searchStock();
        }, 300); // 300ms防抖
    });

    // 条件单
    document.getElementById('btnConditionOrder')?.addEventListener('click', () => {
        showConfirmModal('确认设置条件单', () => setConditionOrder());
    });

    // 加仓
    document.getElementById('btnAddPosition')?.addEventListener('click', () => {
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

    // 卖出
    document.getElementById('btnSell')?.addEventListener('click', () => {
        showConfirmModal('确认卖出', () => sellStock());
    });

    // K线控制
    document.getElementById('zoomIn')?.addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'zoom', zoomSize: 20 });
    });

    document.getElementById('zoomOut')?.addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'zoom', zoomSize: -20 });
    });

    document.getElementById('resetChart')?.addEventListener('click', () => {
        AppState.klineChart?.dispatchAction({ type: 'restore' });
    });

    // 重置
    document.getElementById('btnReset')?.addEventListener('click', showResetModal);
    document.getElementById('btnResetNoSave')?.addEventListener('click', () => {
        hideResetModal();
        resetMobile();
    });
    document.getElementById('btnResetSave')?.addEventListener('click', () => {
        hideResetModal();
        // 简化版直接重置，不保存
        resetMobile();
    });
    document.getElementById('btnResetCancel')?.addEventListener('click', hideResetModal);

    // 筛选
    document.querySelectorAll('.filter-select').forEach(select => {
        select.addEventListener('change', () => {
            if (select.id === 'limitDaysFilter') {
                renderLadderList();
            }
        });
    });
}

function showResetModal() {
    document.getElementById('resetModal').classList.add('show');
}

function hideResetModal() {
    document.getElementById('resetModal').classList.remove('show');
}
