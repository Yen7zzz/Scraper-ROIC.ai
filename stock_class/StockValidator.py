import asyncio
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
from stock_class.RareLimitManager import RateLimitManager


class StockValidator:
    """股票代碼驗證器 - 含國籍檢查功能"""

    def __init__(self, request_delay=2.0):
        self.valid_stocks = []
        self.invalid_stocks = []
        self.us_stocks = []  # 美國股票
        self.non_us_stocks = []  # 非美國股票
        self.stock_countries = {}  # 儲存每支股票的國家資訊

        # 使用統一的速率限制管理器
        self.rate_limiter = RateLimitManager(request_delay)

    def validate_single_stock(self, stock):
        """驗證單一股票代碼"""
        try:
            ticker = yf.Ticker(stock)

            # 先獲取歷史數據來驗證股票
            hist = ticker.history(period="5d")

            if not hist.empty:
                return True, f"✅ {stock}: 有效股票代碼"
            else:
                return False, f"❌ {stock}: 無法獲得股價資訊"

        except Exception as e:
            return False, f"❌ {stock}: 驗證失敗 - {str(e)}"

    def check_stock_nationality(self, stock):
        """檢查股票國籍"""
        try:
            ticker = yf.Ticker(stock)
            country = ticker.info.get('country', 'Unknown')

            is_us = country == 'United States'
            return is_us, country

        except Exception as e:
            return False, f"Error: {str(e)}"

    async def validate_stocks_async(self, stocks, log_callback=None):
        """異步驗證多個股票代碼"""
        self.valid_stocks = []
        self.invalid_stocks = []
        self.us_stocks = []
        self.non_us_stocks = []
        self.stock_countries = {}

        if log_callback:
            log_callback("🔍 開始驗證股票代碼...")

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
                    await self.rate_limiter.rate_limit("yfinance_validator")

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

    async def check_stocks_nationality_async(self, stocks, log_callback=None):
        """異步檢查多個股票的國籍"""
        if log_callback:
            log_callback("🌍 開始檢查股票國籍...")

        # 使用線程池執行同步的國籍檢查
        with ThreadPoolExecutor(max_workers=3) as executor:
            tasks = []
            for stock in stocks:
                task = asyncio.get_event_loop().run_in_executor(
                    executor, self.check_stock_nationality, stock
                )
                tasks.append((stock, task))

            # 等待所有國籍檢查完成
            for stock, task in tasks:
                try:
                    # 應用速率限制
                    await self.rate_limiter.rate_limit("yfinance_nationality")

                    is_us, country = await task

                    # 儲存國家資訊
                    self.stock_countries[stock] = country

                    if is_us:
                        self.us_stocks.append(stock)
                        if log_callback:
                            log_callback(f"🇺🇸 {stock}: 美國股票")
                    else:
                        self.non_us_stocks.append(stock)
                        if log_callback:
                            log_callback(f"🌍 {stock}: {country} 股票（roic.ai 部分功能需付費）")

                except Exception as e:
                    error_msg = f"❌ {stock}: 國籍檢查發生錯誤 - {str(e)}"
                    if log_callback:
                        log_callback(error_msg)
                    # 發生錯誤時，保守處理為非美國股票
                    self.non_us_stocks.append(stock)
                    self.stock_countries[stock] = "Unknown"

        if log_callback:
            log_callback(f"🎯 國籍檢查完成！")
            log_callback(f"   🇺🇸 美國股票: {len(self.us_stocks)}")
            log_callback(f"   🌍 非美國股票: {len(self.non_us_stocks)}")
            if self.non_us_stocks:
                countries_list = [f"{stock}({self.stock_countries[stock]})" for stock in self.non_us_stocks]
                log_callback(f"   📝 非美國股票詳細: {', '.join(countries_list)}")

        return self.us_stocks, self.non_us_stocks

    def get_stock_country(self, stock):
        """獲取特定股票的國家"""
        return self.stock_countries.get(stock, "Unknown")

    def is_us_stock(self, stock):
        """檢查是否為美國股票"""
        return stock in self.us_stocks