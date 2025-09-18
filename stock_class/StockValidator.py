import asyncio
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

# ===== 新增：股票代碼驗證器類別 =====
class StockValidator:
    """股票代碼驗證器"""

    def __init__(self):
        self.valid_stocks = []
        self.invalid_stocks = []

    def validate_single_stock(self, stock):
        """驗證單一股票代碼"""
        try:
            # 使用 yfinance 獲取歷史數據來驗證股票
            ticker = yf.Ticker(stock)

            # 先嘗試獲取基本歷史數據，避免使用 .info 屬性
            hist = ticker.history(period="5d")

            if not hist.empty:
                return True, f"✅ {stock}: 有效股票代碼"
            else:
                return False, f"❌ {stock}: 無法獲得股價資訊"

        except Exception as e:
            return False, f"❌ {stock}: 驗證失敗 - {str(e)}"

    async def validate_stocks_async(self, stocks, log_callback=None):
        """異步驗證多個股票代碼"""
        self.valid_stocks = []
        self.invalid_stocks = []

        if log_callback:
            log_callback("🔍 開始驗證股票代碼...")

        # 使用線程池執行同步的股票驗證
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 創建任務列表
            tasks = []
            for stock in stocks:
                task = asyncio.get_event_loop().run_in_executor(
                    executor, self.validate_single_stock, stock
                )
                tasks.append((stock, task))

            # 等待所有驗證完成
            for stock, task in tasks:
                try:
                    is_valid, message = await task

                    if log_callback:
                        log_callback(message)

                    if is_valid:
                        self.valid_stocks.append(stock)
                    else:
                        self.invalid_stocks.append(stock)

                    # 添加小延遲避免API限制
                    await asyncio.sleep(0.5)

                except Exception as e:
                    error_msg = f"❌ {stock}: 驗證過程發生錯誤 - {str(e)}"
                    if log_callback:
                        log_callback(error_msg)
                    self.invalid_stocks.append(stock)

        if log_callback:
            log_callback(f"🎯 驗證完成！有效股票: {len(self.valid_stocks)}，無效股票: {len(self.invalid_stocks)}")

        return self.valid_stocks, self.invalid_stocks