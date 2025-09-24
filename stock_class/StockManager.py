import asyncio
import os
from datetime import datetime
from stock_class.RareLimitManager import RateLimitManager

class StockManager:
    def __init__(self, scraper, processor, stocks, validator=None, max_concurrent=3, delay=1):
        self.scraper = scraper
        self.processor = processor
        self.stocks = stocks.get('final_stocks')
        self.us_stocks = stocks.get('us_stocks')
        self.non_us_stocks = stocks.get('non_us_stocks')
        self.validator = validator  # 新增：股票驗證器引用
        self.pattern1 = r'^[a-zA-Z\-\.]{1,5}'
        self.pattern2 = r'是非美國企業，此頁面需付費！$'
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay
        self.excel_files = {}  # 儲存每支股票的Excel base64
        self.max_concurrent = max_concurrent

        # 使用共享的速率限制管理器
        if hasattr(processor, 'rate_limiter'):
            self.rate_limiter = processor.rate_limiter
        else:
            self.rate_limiter = RateLimitManager(request_delay=2.0)

        # 確保 processor 使用同一個速率限制管理器
        if not hasattr(processor, 'rate_limiter'):
            processor.rate_limiter = self.rate_limiter

    async def initialize_excel_files(self):
        """為所有股票初始化Excel檔案"""
        for stock in self.stocks:
            excel_base64, message = self.processor.create_excel_from_base64(stock)
            if excel_base64:
                self.excel_files[stock] = excel_base64
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
                return False
        return True

    # async def process_summary(self):
    #     """處理Summary數據"""
    #     raw_df_summary = await self.scraper.run_summary()
    #     for index, stock in enumerate(self.stocks):
    #         if stock in self.excel_files:
    #             modified_base64, message = await self.processor.process_df_summary(
    #                 raw_df_summary[index], stock, self.excel_files[stock]
    #             )
    #             self.excel_files[stock] = modified_base64
    #             print(f"✅ {message}")

    async def process_financial(self):
        """處理Financial數據"""

        if self.us_stocks:
            raw_df_financial = await self.scraper.run_financial()

            for index, stock in enumerate(self.us_stocks):
                if stock in self.excel_files:
                    modified_base64, message = await self.processor.process_df_financial(
                        raw_df_financial[index], stock, self.excel_files[stock]
                    )
                    self.excel_files[stock] = modified_base64
                    print(f"✅ {message}")
        # print(stocks.get('non_us_stocks'), type(stocks.get('non_us_stocks')))
        # print('程式有到這邊')
        # print(stocks.get('non_us_stocks'), type(stocks.get('non_us_stocks')))
        if self.non_us_stocks:
            # 非美國代碼不需進行爬蟲
            # raw_df_financial = await self.scraper.run_financial()
            raw_df_financial = None

            for index, stock in enumerate(self.non_us_stocks):
                if stock in self.excel_files:
                    modified_base64, message = await self.processor.process_df_financial(
                        raw_df_financial, stock, self.excel_files[stock]
                    )
                    self.excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_ratios(self):
        """處理Ratios數據"""
        if self.us_stocks:
            raw_df_ratios = await self.scraper.run_ratios()
            # print(raw_df_ratios)
            for index, stock in enumerate(self.us_stocks):
                if stock in self.excel_files:
                    modified_base64, message = await self.processor.process_df_ratios(
                        raw_df_ratios[index], stock, self.excel_files[stock]
                    )
                    self.excel_files[stock] = modified_base64
                    print(f"✅ {message}")

        if self.non_us_stocks:
            # raw_df_ratios = await self.scraper.run_ratios()
            # print(raw_df_ratios)
            raw_df_ratios = None
            for index, stock in enumerate(self.non_us_stocks):
                if stock in self.excel_files:
                    modified_base64, message = await self.processor.process_df_ratios(
                        raw_df_ratios, stock, self.excel_files[stock]
                    )
                    self.excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_EPS_PE_MarketCap(self):
        """處理EPS/PE/MarketCap數據"""
        raw_df_EPS_PE_MarketCap = await self.scraper.run_EPS_PE_MarketCap()
        for index, stock in enumerate(self.stocks):
            if stock in self.excel_files:
                modified_base64, message = await self.processor.EPS_PE_MarketCap_data_write_to_excel(
                    raw_df_EPS_PE_MarketCap[index], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_others_data(self):
        """處理其他數據"""
        for stock in self.stocks:
            if stock in self.excel_files:
                modified_base64, message = await self.processor.others_data(
                    stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_EPS_Growth_Rate(self):
        """處理EPS成長率"""
        for stock in self.stocks:
            if stock in self.excel_files:
                message, modified_base64 = await self.scraper.EPS_Growth_Rate_and_write_to_excel(
                    stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    def save_all_excel_files(self, output_folder=None):
        """保存所有Excel檔案"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for stock in self.stocks:
            if stock in self.excel_files:
                output_filename = f"STOCK_{stock}.xlsx"
                output_path = os.path.join(output_folder, output_filename)

                if self.processor.save_excel_to_file(self.excel_files[stock], output_path):
                    saved_files.append(output_path)
                    print(f"✅ {stock} 檔案已保存至：{output_path}")
                else:
                    print(f"❌ {stock} 檔案保存失敗")

        return saved_files

    async def process_seekingalpha(self):
        """處理Revenue growth數據"""
        raw_revenue_growth = await self.scraper.run_seekingalpha()
        print(f"獲取到的revenue_growth數據: {raw_revenue_growth}")

        # 遍歷每個字典，提取股票代碼和對應的revenue growth數據
        for revenue_dict in raw_revenue_growth:
            for stock, revenue_data in revenue_dict.items():
                if stock in self.excel_files and revenue_data is not None:
                    # 檢查是否包含錯誤信息
                    if isinstance(revenue_data, dict) and "error" not in revenue_data:
                        # 調用processor寫入數據
                        modified_base64, message = self.processor.write_seekingalpha_data_to_excel(
                            stock=stock,
                            raw_revenue_growth=revenue_data,
                            excel_base64=self.excel_files[stock]
                        )
                        if modified_base64:
                            # 更新Excel base64數據
                            self.excel_files[stock] = modified_base64
                            print(f"✅ {message}")
                        else:
                            print(f"❌ {message}")
                    else:
                        print(f"❌ {stock} 的數據包含錯誤或格式不正確: {revenue_data}")
                else:
                    if stock not in self.excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if revenue_data is None:
                        print(f"❌ {stock} 的revenue_growth值為None")

    async def process_wacc(self):
        """處理wacc數據"""
        raw_wacc = await self.scraper.run_wacc()
        print(f"獲取到的WACC數據: {raw_wacc}")

        # 遍歷每個字典，提取股票代碼和對應的WACC值
        for wacc_dict in raw_wacc:
            for stock, wacc_value in wacc_dict.items():
                if stock in self.excel_files and wacc_value is not None:
                    # 調用processor寫入數據
                    modified_base64, message = self.processor.write_wacc_data_to_excel(
                        stock=stock,
                        wacc_value=wacc_value,
                        excel_base64=self.excel_files[stock]
                    )
                    if modified_base64:
                        # 更新Excel base64數據
                        self.excel_files[stock] = modified_base64
                        print(f"✅ {message}")
                    else:
                        print(f"❌ {message}")
                else:
                    if stock not in self.excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if wacc_value is None:
                        print(f"❌ {stock} 的WACC值為None")

    async def process_TradingView(self):
        """處理TradingView數據"""
        raw_TradingView = await self.scraper.run_TradingView()
        print(f"獲取到的TradingView數據: {raw_TradingView}")

        # 遍歷每個字典，提取股票代碼和對應的TradingView值
        for TradingView_dict in raw_TradingView:
            for stock, TradingView_value in TradingView_dict.items():
                if stock in self.excel_files and TradingView_value is not None:
                    # 調用processor寫入數據
                    modified_base64, message = self.processor.write_TradeingView_data_to_excel(
                        stock=stock,
                        tradingview_data=TradingView_value,
                        excel_base64=self.excel_files[stock]
                    )
                    if modified_base64:
                        # 更新Excel base64數據
                        self.excel_files[stock] = modified_base64
                        print(f"✅ {message}")
                    else:
                        print(f"❌ {message}")
                else:
                    if stock not in self.excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if TradingView_value is None:
                        print(f"❌ {stock} 的TradingView值為None")

    async def process_combined_summary_and_metrics(self):
        """處理合併的Summary和指標數據"""
        summary_results, metrics_results = await self.scraper.run_combined_summary_and_metrics()

        # 處理Summary數據
        for index, stock in enumerate(self.stocks):
            if stock in self.excel_files and index < len(summary_results):
                modified_base64, message = await self.processor.process_df_summary(
                    summary_results[index][stock], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

        # 處理指標數據
        for index, stock in enumerate(self.stocks):
            if stock in self.excel_files and index < len(metrics_results):
                modified_base64, message = await self.processor.EPS_PE_MarketCap_data_write_to_excel(
                    {stock: [metrics_results[index][stock]]}, stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")



from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess


# 修正後的 main 函數
async def main():
    stocks = ['AMAT', 'NVTS', 'PLTR', 'GOOGL', 'CCL', 'O', 'META', 'TSLA', 'CCL']
    scraper = StockScraper(stocks=stocks)
    process = StockProcess()
    # manager = StockManager(scraper=scraper, processor=process, stocks=stocks)

    # 初始化Excel檔案
    # success = await manager.initialize_excel_files()
    # if not success:
    #     print("Excel檔案初始化失敗，程式終止")
    #     return

    # 處理SeekingAlpha數據
    # await manager.process_TradingView(stocks=stocks)
    # await manager.process_combined_summary_and_metrics(stocks=stocks)
    # await manager.process_others_data(stocks=stocks)
    # await manager.process_seekingalpha(stocks=stocks)
    # await manager.process_financial(stocks=stocks)
    # await manager.process_ratios()
    # print("所有股票的revenue growth數據處理完成！")



if __name__ == "__main__":
    asyncio.run(main())