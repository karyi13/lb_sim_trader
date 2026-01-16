/**
 * 公共工具函数模块
 * 提取自app.js和mobile.js的共享逻辑
 */

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

    // K线图配置
    CHART: {
        DEFAULT_VISIBLE_BARS: 30,
        MIN_VISIBLE_BARS: 10,
    }
};

// ==================== 日期工具函数 ====================

/**
 * 将YYYYMMDD格式的日期转换为YYYY-MM-DD格式
 * @param {string} dateStr - YYYYMMDD格式日期
 * @returns {string} YYYY-MM-DD格式日期
 */
function formatDate(dateStr) {
    const year = dateStr.slice(0, 4);
    const month = dateStr.slice(4, 6);
    const day = dateStr.slice(6, 8);
    return `${year}-${month}-${day}`;
}

/**
 * 将Date对象格式化为YYYYMMDD字符串
 * @param {Date} date - Date对象
 * @returns {string} YYYYMMDD格式日期
 */
function formatDateForComparison(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}${month}${day}`;
}

/**
 * 查找最接近今天的交易日
 * @param {string[]} availableDates - 可用日期列表
 * @returns {string} 最接近今天的日期
 */
function findNearestTradingDay(availableDates) {
    const today = new Date();
    const todayStr = formatDateForComparison(today);

    // 检查今天是否在可用数据中
    if (availableDates.includes(todayStr)) {
        return todayStr;
    }

    // 如果今天不在数据中，查找最接近的日期
    for (let i = availableDates.length - 1; i >= 0; i--) {
        if (availableDates[i] <= todayStr) {
            return availableDates[i];
        }
    }

    return availableDates[0];
}

/**
 * 将YYYYMMDD转换为当前时区的Date对象
 * @param {string} yyyymmdd - YYYYMMDD格式日期
 * @returns {Date} Date对象
 */
function parseDate(yyyymmdd) {
    if (!yyyymmdd || yyyymmdd.length !== 8) {
        return new Date();
    }
    const year = parseInt(yyyymmdd.slice(0, 4));
    const month = parseInt(yyyymmdd.slice(4, 6)) - 1;
    const day = parseInt(yyyymmdd.slice(6, 8));
    return new Date(year, month, day);
}

// ==================== 计算工具函数 ====================

/**
 * 计算涨停价
 * @param {number} price - 当前价格
 * @param {boolean} isST - 是否为ST股票
 * @returns {number} 涨停价
 */
function calculateLimitUpPrice(price, isST = false) {
    const rate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_UP;
    return Math.round(price * (1 + rate) * 100) / 100;
}

/**
 * 计算跌停价
 * @param {number} price - 当前价格
 * @param {boolean} isST - 是否为ST股票
 * @returns {number} 跌停价
 */
function calculateLimitDownPrice(price, isST = false) {
    const rate = isST ? CONFIG.ST_LIMIT : CONFIG.LIMIT_DOWN;
    return Math.round(price * (1 - rate) * 100) / 100;
}

/**
 * 计算交易费用
 * @param {number} amount - 交易金额
 * @param {boolean} isSell - 是否为卖出
 * @returns {Object} 包含commission, stampTax, total的对象
 */
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

/**
 * 计算涨跌幅百分比
 * @param {number} current - 当前值
 * @param {number} previous - 前值
 * @returns {number} 涨跌幅百分比
 */
function calculateChangePct(current, previous) {
    if (!previous || previous === 0) {
        return 0;
    }
    return ((current - previous) / previous * 100);
}

/**
 * 格式化数字为千分位
 * @param {number} num - 数字
 * @returns {string} 格式化后的字符串
 */
function formatNumber(num) {
    if (num === null || num === undefined) return '0.00';
    return num.toLocaleString('zh-CN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

/**
 * 格式化金额
 * @param {number} amount - 金额
 * @returns {string} 格式化后的金额字符串
 */
function formatMoney(amount) {
    if (Math.abs(amount) >= 100000000) {
        return (amount / 100000000).toFixed(2) + '亿';
    } else if (Math.abs(amount) >= 10000) {
        return (amount / 10000).toFixed(2) + '万';
    }
    return formatNumber(amount);
}

// ==================== 验证工具函数 ====================

/**
 * 验证价格是否有效
 * @param {number} price - 价格
 * @returns {boolean} 是否有效
 */
function isValidPrice(price) {
    return typeof price === 'number' &&
           !isNaN(price) &&
           isFinite(price) &&
           price > 0 &&
           price < 10000; // 股价通常不会超过1万
}

/**
 * 验证股数是否有效
 * @param {number} quantity - 股数
 * @returns {boolean} 是否有效
 */
function isValidQuantity(quantity) {
    return typeof quantity === 'number' &&
           !isNaN(quantity) &&
           isFinite(quantity) &&
           quantity > 0 &&
           quantity % CONFIG.TRADING_UNIT === 0;
}

// ==================== K线图工具函数 ====================

/**
 * 过滤K线数据到指定日期
 * @param {Array} dates - 日期数组 (YYYY-MM-DD)
 * @param {Array} values - K线values数组
 * @param {Array} volumes - 成交量数组
 * @param {string} currentDate - 当前日期 (YYYYMMDD)
 * @returns {Object} 过滤后的数据 {dates, values, volumes, idx}
 */
function filterKlineDataToDate(dates, values, volumes, currentDate) {
    const currentDateFormatted = currentDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');

    let currentDateIdx = -1;
    for (let i = 0; i < dates.length; i++) {
        if (dates[i] <= currentDateFormatted) {
            currentDateIdx = i;
        } else {
            break;
        }
    }

    if (currentDateIdx < 0) {
        return { dates: [], values: [], volumes: [], idx: -1 };
    }

    return {
        dates: dates.slice(0, currentDateIdx + 1),
        values: currentDateIdx >= 0 ? values.slice(0, currentDateIdx + 1) : values,
        volumes: currentDateIdx >= 0 ? volumes.slice(0, currentDateIdx + 1) : volumes,
        idx: currentDateIdx
    };
}

/**
 * 计算K线图缩放起始位置
 * @param {number} totalCount - 总数据条数
 * @param {number} defaultCount - 默认显示条数
 * @param {Object} savedZoom - 保存的缩放状态
 * @returns {number} 起始位置
 */
function calculateZoomStart(totalCount, defaultCount, savedZoom = null) {
    if (savedZoom && savedZoom.startValue !== undefined) {
        const savedStart = savedZoom.startValue;
        const savedEnd = savedZoom.endValue;
        if (savedStart >= 0 && savedEnd < totalCount) {
            return savedStart;
        }
    }
    return Math.max(0, totalCount - defaultCount);
}

// ==================== 防抖和节流 ====================

/**
 * 防抖函数
 * @param {Function} func - 要防抖的函数
 * @param {number} delay - 延迟时间（毫秒）
 * @returns {Function} 防抖后的函数
 */
function debounce(func, delay = 300) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => {
            func.apply(this, args);
        }, delay);
    };
}

/**
 * 节流函数
 * @param {Function} func - 要节流的函数
 * @param {number} delay - 延迟时间（毫秒）
 * @returns {Function} 节流后的函数
 */
function throttle(func, delay = 300) {
    let lastCall = 0;
    return function(...args) {
        const now = Date.now();
        if (now - lastCall >= delay) {
            lastCall = now;
            func.apply(this, args);
        }
    };
}

// ==================== ID生成工具 ====================

/**
 * 生成唯一ID
 * @returns {string} 唯一ID
 */
function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

// ==================== 颜色工具 ====================

/**
 * 获取涨跌颜色
 * @param {number} change - 涨跌幅
 * @returns {string} 颜色值
 */
function getChangeColor(change) {
    if (change > 0) return '#ff0000';      // 涨-红色
    if (change < 0) return '#00aa00';      // 跌-绿色
    return '#999999';                      // 平-灰色
}

/**
 * 获取盈亏类名
 * @param {number} profit - 盈亏金额
 * @returns {string} CSS类名
 */
function getProfitClass(profit) {
    return profit >= 0 ? 'profit' : 'loss';
}

// ==================== DOM工具 ====================

/**
 * 安全设置元素文本
 * @param {string} id - 元素ID
 * @param {string|number} text - 文本内容
 */
function setText(id, text) {
    const el = document.getElementById(id);
    if (el) {
        el.textContent = String(text);
    }
}

/**
 * 安全获取元素
 * @param {string} id - 元素ID
 * @returns {HTMLElement|null} 元素或null
 */
function getElement(id) {
    return document.getElementById(id);
}

/**
 * 显示/隐藏元素
 * @param {string} id - 元素ID
 * @param {boolean} show - 是否显示
 */
function toggleElement(id, show) {
    const el = document.getElementById(id);
    if (el) {
        el.style.display = show ? '' : 'none';
    }
}

// ==================== Toast提示 ====================

/**
 * 显示Toast消息
 * @param {string} message - 消息内容
 * @param {string} type - 消息类型: 'info', 'success', 'warning', 'error'
 */
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    if (!toast) {
        console.warn('Toast element not found');
        return;
    }

    toast.textContent = message;
    toast.className = `toast show ${type}`;

    setTimeout(() => {
        toast.className = 'toast';
    }, 3000);
}

// ==================== 状态文字 ====================

/**
 * 获取订单状态文本
 * @param {string} status - 状态代码
 * @returns {string} 带样式的文字
 */
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

// ==================== 导出 ====================

// ES6模块导出
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        CONFIG,
        formatDate,
        formatDateForComparison,
        findNearestTradingDay,
        parseDate,
        calculateLimitUpPrice,
        calculateLimitDownPrice,
        calculateFees,
        calculateChangePct,
        formatNumber,
        formatMoney,
        isValidPrice,
        isValidQuantity,
        filterKlineDataToDate,
        calculateZoomStart,
        debounce,
        throttle,
        generateId,
        getChangeColor,
        getProfitClass,
        setText,
        getElement,
        toggleElement,
        showToast,
        getStatusText
    };
}