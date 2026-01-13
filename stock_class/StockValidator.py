import asyncio
from concurrent.futures import ThreadPoolExecutor
from stock_class.RareLimitManager import RateLimitManager
import yfinance as yf


class StockValidator:
    """
    股票代碼驗證器 - 混合使用 yfinance 和 schwabdev

    🔥 改進：基於公司註冊國家的二分類系統
    - US Stocks: country == 'United States'（可爬 financial/ratios）
    - Non-US Stocks: country != 'United States'（跳過 financial/ratios）

    同時使用 Schwab API 獲取交易所資訊（供 TradingView 使用）

    使用範例：
        validator = StockValidator(schwab_client=self.schwab_client)
        valid_stocks, invalid_stocks = await validator.validate_stocks_async(
            stocks=['TSM', 'AAPL', 'GRAB'],
            log_callback=self.log
        )

        # 分類股票（基於註冊國家）
        us_stocks, non_us_stocks = await validator.classify_stocks_async(
            valid_stocks,
            log_callback=self.log
        )
    """

    def __init__(self, schwab_client=None, request_delay=1.0):
        """
        初始化驗證器

        Args:
            schwab_client: schwabdev.Client 實例（用於獲取交易所資訊）
            request_delay: 請求延遲（秒）
        """
        self.schwab_client = schwab_client

        # 驗證結果
        self.valid_stocks = []
        self.invalid_stocks = []

        # 🔥 二分類（基於公司註冊國家）
        self.us_stocks = []  # 美國公司（可爬 financial/ratios）
        self.non_us_stocks = []  # 非美國公司（跳過 financial/ratios）

        # 🔥 儲存詳細資訊
        self.stock_details = {}  # {stock: {'country': 'United States', 'exchangeName': 'NYSE', ...}}
        self.stock_exchanges = {}  # {stock: 'NYSE'} - 供 TradingView 使用

        # 使用統一的速率限制管理器
        self.rate_limiter = RateLimitManager(request_delay)

    def validate_single_stock(self, stock):
        """
        驗證單一股票代碼 - 使用 schwabdev

        Args:
            stock: 股票代碼（例如：TSM, AAPL, NVDA）

        Returns:
            (is_valid, message): (布林值, 訊息字串)
        """
        try:
            if not self.schwab_client:
                return False, f"❌ {stock}: Schwab Client 未初始化"

            # 🔥 呼叫 Schwab API 獲取股票報價
            response = self.schwab_client.quote(stock)

            # 🔥 簡單判斷：200 = 有效，其他 = 無效
            if hasattr(response, 'status_code'):
                if response.status_code == 200:
                    # 進一步驗證回應內容
                    try:
                        data = response.json()
                        if stock in data and 'quote' in data[stock]:
                            return True, f"✅ {stock}: 有效股票代碼"
                        else:
                            return False, f"❌ {stock}: API 回應格式異常"
                    except Exception as json_error:
                        return False, f"❌ {stock}: 無法解析 API 回應 - {str(json_error)}"

                elif response.status_code in [400, 404]:
                    return False, f"❌ {stock}: 無效股票代碼（API 返回 {response.status_code}）"

                elif response.status_code == 401:
                    return False, f"❌ {stock}: Token 認證失敗（請重新認證）"

                else:
                    return False, f"❌ {stock}: API 錯誤（狀態碼 {response.status_code}）"
            else:
                return False, f"❌ {stock}: API 回應異常"

        except Exception as e:
            error_str = str(e).lower()

            # 檢查是否為 Token 錯誤
            if 'refresh_token_authentication_error' in error_str or \
                    'unsupported_token_type' in error_str or \
                    '401' in error_str:
                return False, f"❌ {stock}: Token 認證失敗 - {str(e)}"
            else:
                return False, f"❌ {stock}: 驗證失敗 - {str(e)}"

    def classify_single_stock(self, stock):
        """
        分類單一股票 - 使用 yfinance 判斷國家，Schwab 獲取交易所

        🔥 新邏輯：
        1. 用 yfinance 的 country 判斷是否為美國公司
        2. 用 Schwab API 獲取 exchangeName（供 TradingView 使用）

        Args:
            stock: 股票代碼

        Returns:
            (stock_type, details):
                stock_type: 'US' or 'NON_US'
                details: {
                    'country': 'United States',
                    'exchangeName': 'NYSE',
                    'description': '公司全名',
                    ...
                }
        """
        details = {}

        try:
            # 🔥 步驟 1: 用 yfinance 獲取公司註冊國家
            ticker = yf.Ticker(stock)
            country = ticker.info.get('country', None)

            if country:
                details['country'] = country
                details['yfinance_name'] = ticker.info.get('longName', ticker.info.get('shortName', ''))
            else:
                # 如果 yfinance 沒有 country 資訊，保守處理為非美國
                return 'NON_US', {'error': 'yfinance 無 country 資訊'}

            # 🔥 步驟 2: 用 Schwab API 獲取交易所資訊（供 TradingView 使用）
            if self.schwab_client:
                try:
                    response = self.schwab_client.quote(stock)

                    if hasattr(response, 'status_code') and response.status_code == 200:
                        data = response.json()

                        if stock in data:
                            reference = data[stock].get('reference', {})
                            details['exchangeName'] = reference.get('exchangeName', 'NYSE')
                            details['schwab_description'] = reference.get('description', '')
                            details['exchange'] = reference.get('exchange', '')
                except Exception as schwab_error:
                    # Schwab API 失敗不影響分類，只是沒有交易所資訊
                    details['exchangeName'] = 'NYSE'  # 預設值
                    details['schwab_error'] = str(schwab_error)
            else:
                details['exchangeName'] = 'NYSE'  # 預設值

            print(details)
            # 🔥 步驟 3: 根據 country 判斷類型
            if country == 'United States':
                return 'US', details
            else:
                return 'NON_US', details

        except Exception as e:
            return 'NON_US', {'error': str(e)}

    async def validate_stocks_async(self, stocks, log_callback=None):
        """
        異步驗證多個股票代碼

        Args:
            stocks: 股票代碼列表
            log_callback: 日誌回調函數

        Returns:
            (valid_stocks, invalid_stocks): (有效股票列表, 無效股票列表)
        """
        self.valid_stocks = []
        self.invalid_stocks = []

        if log_callback:
            log_callback("🔍 開始驗證股票代碼（使用 Schwab API）...")

        # 使用線程池執行同步的股票驗證
        with ThreadPoolExecutor(max_workers=3) as executor:
            tasks = []
            for stock in stocks:
                task = asyncio.get_event_loop().run_in_executor(
                    executor, self.validate_single_stock, stock
                )
                tasks.append((stock, task))

            # 等待所有驗證完成
            for stock, task in tasks:
                try:
                    # 應用速率限制
                    await self.rate_limiter.rate_limit("schwab_validator")

                    is_valid, message = await task

                    if log_callback:
                        log_callback(message)

                    if is_valid:
                        self.valid_stocks.append(stock)
                    else:
                        self.invalid_stocks.append(stock)

                except Exception as e:
                    error_msg = f"❌ {stock}: 驗證過程發生錯誤 - {str(e)}"
                    if log_callback:
                        log_callback(error_msg)
                    self.invalid_stocks.append(stock)

        if log_callback:
            log_callback(f"🎯 股票驗證完成！有效股票: {len(self.valid_stocks)}，無效股票: {len(self.invalid_stocks)}")

        return self.valid_stocks, self.invalid_stocks

    async def classify_stocks_async(self, stocks, log_callback=None):
        """
        異步分類多個股票（基於公司註冊國家）

        🔥 新邏輯：
        - 用 yfinance 的 country 判斷是否為美國公司
        - 用 Schwab API 獲取 exchangeName（供 TradingView 使用）

        Args:
            stocks: 股票代碼列表
            log_callback: 日誌回調函數

        Returns:
            (us_stocks, non_us_stocks): (美國公司列表, 非美國公司列表)
        """
        self.us_stocks = []
        self.non_us_stocks = []
        self.stock_details = {}
        self.stock_exchanges = {}

        if log_callback:
            log_callback("🌍 開始分類股票（基於公司註冊國家）...")

        # 使用線程池執行同步的分類
        with ThreadPoolExecutor(max_workers=3) as executor:
            tasks = []
            for stock in stocks:
                task = asyncio.get_event_loop().run_in_executor(
                    executor, self.classify_single_stock, stock
                )
                tasks.append((stock, task))

            # 等待所有分類完成
            for stock, task in tasks:
                try:
                    # 應用速率限制
                    await self.rate_limiter.rate_limit("yfinance_classifier")

                    stock_type, details = await task

                    # 🔥 儲存詳細資訊
                    self.stock_details[stock] = details

                    # 🔥 儲存交易所資訊（供 TradingView 使用）
                    if 'exchangeName' in details:
                        self.stock_exchanges[stock] = details['exchangeName']

                    # 🔥 分類
                    if stock_type == 'US':
                        self.us_stocks.append(stock)
                        if log_callback:
                            country = details.get('country', 'N/A')
                            name = details.get('yfinance_name', details.get('schwab_description', ''))
                            log_callback(f"🟢 {stock}: 美國公司 ({country}) - {name}")
                    else:
                        self.non_us_stocks.append(stock)
                        if log_callback:
                            country = details.get('country', '未知')
                            name = details.get('yfinance_name', details.get('schwab_description', ''))
                            error_msg = details.get('error', '')

                            if error_msg:
                                log_callback(f"🔴 {stock}: 非美國公司 - {error_msg}")
                            else:
                                log_callback(f"🔴 {stock}: 非美國公司 ({country}) - {name}")
                                log_callback(f"   ⚠️  roic.ai 的 financial 和 ratios 需付費，將跳過")

                except Exception as e:
                    error_msg = f"❌ {stock}: 分類過程發生錯誤 - {str(e)}"
                    if log_callback:
                        log_callback(error_msg)
                    # 發生錯誤時，保守處理為非美國股票
                    self.non_us_stocks.append(stock)

        if log_callback:
            log_callback(f"\n🎯 股票分類完成！")
            log_callback(
                f"   🟢 美國公司: {len(self.us_stocks)} 支 - {', '.join(self.us_stocks) if self.us_stocks else '無'}")
            log_callback(
                f"   🔴 非美國公司: {len(self.non_us_stocks)} 支 - {', '.join(self.non_us_stocks) if self.non_us_stocks else '無'}")

            if self.non_us_stocks:
                log_callback(f"\n   💡 非美國公司說明：")
                for stock in self.non_us_stocks:
                    details = self.stock_details.get(stock, {})
                    country = details.get('country', '未知')
                    name = details.get('yfinance_name', 'N/A')
                    log_callback(f"      • {stock} - {country} ({name})")
                log_callback(f"      → 這些股票在 roic.ai 的 financial 和 ratios 頁面需付費")
                log_callback(f"      → 系統將自動跳過這些頁面以節省時間")

        return self.us_stocks, self.non_us_stocks

    def get_stock_detail(self, stock):
        """獲取特定股票的詳細資訊"""
        return self.stock_details.get(stock, {})

    def get_stock_exchange(self, stock):
        """獲取特定股票的交易所名稱（供 TradingView 使用）"""
        return self.stock_exchanges.get(stock, 'NYSE')  # 預設 NYSE

    def is_us_stock(self, stock):
        """檢查是否為美國公司"""
        return stock in self.us_stocks

    def is_non_us_stock(self, stock):
        """檢查是否為非美國公司"""
        return stock in self.non_us_stocks