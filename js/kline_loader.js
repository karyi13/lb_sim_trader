/**
 * K线数据加载器 - 按需加载单股票数据
 * 解决48MB大文件加载问题
 */

// K线数据加载器类
class KlineDataLoader {
    constructor(options = {}) {
        this.baseDir = options.baseDir || 'data/';
        this.splitDir = options.splitDir || 'kline_split/';
        this.cache = new Map();
        this.loading = new Map();
        this.indexData = null;
        this.indexLoaded = false;

        // 回调函数
        this.onProgress = options.onProgress || (() => {});
        this.onLoaded = options.onLoaded || (() => {});
        this.onError = options.onError || ((code, error) => console.error(error));

        // 统计
        this.stats = {
            loaded: 0,
            cacheHits: 0,
            totalSize: 0
        };
    }

    /**
     * 加载索引文件
     */
    async loadIndex() {
        if (this.indexLoaded) {
            return this.indexData;
        }

        try {
            const startTime = performance.now();
            const response = await fetch(`${this.baseDir}${this.splitDir}index.json`);
            if (!response.ok) {
                throw new Error(`Failed to load index: ${response.status}`);
            }
            this.indexData = await response.json();
            this.indexLoaded = true;

            const loadTime = performance.now() - startTime;
            this.onProgress({
                type: 'index_loaded',
                count: Object.keys(this.indexData).length,
                loadTime: loadTime.toFixed(1)
            });

            console.log('[KlineLoader] Index loaded with', Object.keys(this.indexData).length, 'stocks');
            return this.indexData;
        } catch (error) {
            console.error('[KlineLoader] Error loading index:', error);
            this.onError('index', error);
            return null;
        }
    }

    /**
     * 加载单个股票的K线数据
     * @param {string} code - 股票代码，如 "000001.SZ"
     * @returns {Promise<Object>} K线数据
     */
    async loadKline(code) {
        // 标准化股票代码
        code = code.toUpperCase();

        console.log('[KlineLoader] Loading K-line for:', code);

        // 检查缓存
        if (this.cache.has(code)) {
            this.stats.cacheHits++;
            this.onProgress({
                type: 'cache_hit',
                code: code
            });
            console.log('[KlineLoader] Cache hit for:', code);
            return this.cache.get(code);
        }

        // 检查是否正在加载
        if (this.loading.has(code)) {
            console.log('[KlineLoader] Already loading:', code);
            return this.loading.get(code);
        }

        // 确保索引已加载
        if (!this.indexLoaded) {
            await this.loadIndex();
        }

        // 获取文件路径
        const stockInfo = this.indexData?.[code];
        if (!stockInfo) {
            console.warn('[KlineLoader] Stock not found in index:', code);
            // 回退：尝试直接用代码构造路径
            const fallbackFile = `${this.splitDir}${code.replace(/\./g, '_')}.json`;
            console.log('[KlineLoader] Trying fallback path:', fallbackFile);

            const loadPromise = this._loadFile(code, fallbackFile);
            this.loading.set(code, loadPromise);

            try {
                const data = await loadPromise;
                this.cache.set(code, data);
                this.loading.delete(code);
                this.stats.loaded++;

                console.log('[KlineLoader] Fallback load successful for:', code);
                this.onLoaded(code, data);
                return data;
            } catch (fallbackError) {
                this.loading.delete(code);
                this.onError(code, fallbackError);
                throw fallbackError;
            }
        }

        // 创建加载Promise
        console.log('[KlineLoader] Loading from index file:', stockInfo.file);
        const loadPromise = this._loadFile(code, stockInfo.file);
        this.loading.set(code, loadPromise);

        try {
            const data = await loadPromise;
            this.cache.set(code, data);
            this.loading.delete(code);
            this.stats.loaded++;

            this.onProgress({
                type: 'loaded',
                code: code,
                name: data.name,
                bars: data.dates?.length || 0,
                totalLoaded: this.stats.loaded
            });

            this.onLoaded(code, data);
            return data;
        } catch (error) {
            this.loading.delete(code);
            console.error('[KlineLoader] Error loading', code, ':', error);
            this.onError(code, error);
            throw error;
        }
    }

    /**
     * 实际加载文件
     */
    async _loadFile(code, filename) {
        const startTime = performance.now();
        const url = `${this.baseDir}${filename}`;

        try {
            const response = await fetch(url);
            if (!response.ok) {
                throw new Error(`Failed to load ${url}: ${response.status}`);
            }

            const data = await response.json();
            const loadTime = performance.now() - startTime;
            const fileSize = response.headers.get('Content-Length') || 0;

            this.stats.totalSize += parseInt(fileSize);

            this.onProgress({
                type: 'file_loaded',
                code: code,
                url: url,
                loadTime: loadTime.toFixed(1),
                size: (parseInt(fileSize) / 1024).toFixed(1) + ' KB'
            });

            return data;
        } catch (error) {
            console.error(`Error loading ${url}:`, error);
            throw error;
        }
    }

    /**
     * 批量加载K线数据
     * @param {string[]} codes - 股票代码数组
     * @returns {Promise<Object>} 键为股票代码，值为K线数据
     */
    async loadBatch(codes) {
        const results = {};

        // 并发加载，但限制并发数
        const batchSize = 5;
        for (let i = 0; i < codes.length; i += batchSize) {
            const batch = codes.slice(i, i + batchSize);
            const promises = batch.map(code =>
                this.loadKline(code).catch(err => {
                    console.error(`Failed to load ${code}:`, err);
                    return null;
                })
            );

            const batchResults = await Promise.all(promises);
            batchResults.forEach((data, idx) => {
                if (data) {
                    results[batch[idx]] = data;
                }
            });
        }

        return results;
    }

    /**
     * 预加载热门股票（前N个连板股票）
     */
    async preloadHotStocks(ladderData, count = 20) {
        if (!ladderData) return;

        const codesToLoad = new Set();

        // 从最近的日期开始收集
        const dates = Object.keys(ladderData).sort().reverse().slice(0, 5);
        for (const date of dates) {
            const dateData = ladderData[date];
            let collected = 0;

            for (const level in dateData) {
                if (collected >= count) break;
                const stocks = dateData[level] || [];
                for (const stock of stocks) {
                    if (!codesToLoad.has(stock.code)) {
                        codesToLoad.add(stock.code);
                        collected++;
                        if (collected >= count) break;
                    }
                }
            }
            if (codesToLoad.size >= count) break;
        }

        if (codesToLoad.size > 0) {
            this.onProgress({
                type: 'preload_start',
                count: codesToLoad.size
            });

            const codes = Array.from(codesToLoad);
            await this.loadBatch(codes);

            this.onProgress({
                type: 'preload_complete',
                count: codes.length
            });
        }
    }

    /**
     * 清理缓存
     */
    clearCache() {
        const size = this.cache.size;
        this.cache.clear();
        this.onProgress({
            type: 'cache_cleared',
            count: size
        });
    }

    /**
     * 预热缓存（从精简版K线数据中加载）
     */
    warmupFromCoreData() {
        if (window.KLINE_DATA_CORE) {
            this.onProgress({
                type: 'warmup_start',
                count: Object.keys(window.KLINE_DATA_CORE).length
            });

            for (const [code, data] of Object.entries(window.KLINE_DATA_CORE)) {
                this.cache.set(code, data);
                this.cacheHits++;
            }

            this.onProgress({
                type: 'warmup_complete',
                count: Object.keys(window.KLINE_DATA_CORE).length
            });
        }
    }

    /**
     * 获取统计数据
     */
    getStats() {
        return {
            ...this.stats,
            cacheSize: this.cache.size,
            indexLoaded: this.indexLoaded,
            loadedFromCache: this.stats.cacheHits,
            loadedFromNetwork: this.stats.loaded - this.stats.cacheHits
        };
    }
}

// 兼容旧API - getKlineData
(function() {
    // 创建全局加载器实例
    window.klineLoader = new KlineDataLoader({
        onProgress: (event) => {
            console.log('[KlineLoader]', event);
        }
    });

    // 旧版函数 - 优先使用加载器，回退到全局数据
    window.getKlineData = function(code) {
        // 先检查加载器缓存
        if (window.klineLoader.cache.has(code)) {
            return window.klineLoader.cache.get(code);
        }

        // 回退到精简版数据
        const coreData = window.KLINE_DATA_CORE?.[code];
        if (coreData) return coreData;

        // 回退到完整版数据
        const globalData = window.KLINE_DATA_GLOBAL?.[code];
        return globalData || null;
    };

    // 异步加载函数
    window.loadKlineAsync = async function(code) {
        try {
            return await window.klineLoader.loadKline(code);
        } catch (error) {
            console.warn('[KlineLoader] Failed to load from split files, trying fallback...');

            // 回退：尝试从精简版数据中获取
            const coreData = window.KLINE_DATA_CORE?.[code];
            if (coreData) {
                console.log('[KlineLoader] Found in KLINE_DATA_CORE');
                window.klineLoader.cache.set(code, coreData);
                return coreData;
            }

            // 回退：尝试从完整版数据中获取
            const globalData = window.KLINE_DATA_GLOBAL?.[code];
            if (globalData) {
                console.log('[KlineLoader] Found in KLINE_DATA_GLOBAL');
                window.klineLoader.cache.set(code, globalData);
                return globalData;
            }

            console.error('[KlineLoader] No data found for', code);
            return null;
        }
    };

    // 检查代码是否存在
    window.hasKlineData = function(code) {
        code = code.toUpperCase();
        return window.klineLoader.cache.has(code) ||
               window.KLINE_DATA_CORE?.[code] ||
               window.KLINE_DATA_GLOBAL?.[code] ||
               window.klineLoader.indexData?.[code];
    };

    // 页面加载完成后初始化
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            window.klineLoader.warmupFromCoreData();
            window.klineLoader.loadIndex();
        });
    } else {
        window.klineLoader.warmupFromCoreData();
        window.klineLoader.loadIndex();
    }
})();
