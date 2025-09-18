import asyncio
import os
from datetime import datetime

class StockManager:
    def __init__(self, scraper, processor, max_concurrent=3, delay=1):
        self.scraper = scraper
        self.processor = processor
        self.pattern1 = r'^[a-zA-Z\-\.]{1,5}'
        self.pattern2 = r'是非美國企業，此頁面須付費！$'
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay
        self.excel_files = {}  # 儲存每支股票的Excel base64
        self.max_concurrent = max_concurrent

    async def initialize_excel_files(self, stocks):
        """為所有股票初始化Excel檔案"""
        for stock in stocks:
            excel_base64, message = self.processor.create_excel_from_base64(stock)
            if excel_base64:
                self.excel_files[stock] = excel_base64
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
                return False
        return True

    async def process_summary(self, stocks):
        """處理Summary數據"""
        raw_df_summary = await self.scraper.run_summary()
        for index, stock in enumerate(stocks):
            if stock in self.excel_files:
                modified_base64, message = await self.processor.process_df_summary(
                    raw_df_summary[index], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_financial(self, stocks):
        """處理Financial數據"""
        raw_df_financial = await self.scraper.run_financial()
        for index, stock in enumerate(stocks):
            if stock in self.excel_files:
                modified_base64, message = await self.processor.process_df_financial(
                    raw_df_financial[index], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_ratios(self, stocks):
        """處理Ratios數據"""
        raw_df_ratios = await self.scraper.run_ratios()
        for index, stock in enumerate(stocks):
            if stock in self.excel_files:
                modified_base64, message = await self.processor.process_df_ratios(
                    raw_df_ratios[index], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_EPS_PE_MarketCap(self, stocks):
        """處理EPS/PE/MarketCap數據"""
        raw_df_EPS_PE_MarketCap = await self.scraper.run_EPS_PE_MarketCap()
        for index, stock in enumerate(stocks):
            if stock in self.excel_files:
                modified_base64, message = await self.processor.EPS_PE_MarketCap_data_write_to_excel(
                    raw_df_EPS_PE_MarketCap[index], stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_others_data(self, stocks):
        """處理其他數據"""
        for stock in stocks:
            if stock in self.excel_files:
                modified_base64, message = await self.processor.others_data(
                    stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_EPS_Growth_Rate(self, stocks):
        """處理EPS成長率"""
        for stock in stocks:
            if stock in self.excel_files:
                message, modified_base64 = await self.scraper.EPS_Growth_Rate_and_write_to_excel(
                    stock, self.excel_files[stock]
                )
                self.excel_files[stock] = modified_base64
                print(f"✅ {message}")

    def save_all_excel_files(self, stocks, output_folder=None):
        """保存所有Excel檔案"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for stock in stocks:
            if stock in self.excel_files:
                output_filename = f"STOCK_{stock}.xlsx"
                output_path = os.path.join(output_folder, output_filename)

                if self.processor.save_excel_to_file(self.excel_files[stock], output_path):
                    saved_files.append(output_path)
                    print(f"✅ {stock} 檔案已保存至：{output_path}")
                else:
                    print(f"❌ {stock} 檔案保存失敗")

        return saved_files


    async def get_multiple_wacc_data(self, stocks):
        """
        批量獲取多個股票的WACC數據
        返回格式: {'O': 8, 'AAPL': 10, 'MSFT': 9}
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        tasks = [self.scraper.fetch_wacc_data(stock, semaphore) for stock in stocks]
        results = await asyncio.gather(*tasks)

        # 合併結果為單一字典
        final_result = {}
        for result in results:
            final_result.update(result)

        return final_result

    async def process_wacc(self, stocks):
        """處理Ratios數據"""
        raw_wacc = await self.scraper.run_wacc()
        print(raw_wacc)
        # for index, stock in enumerate(stocks):
        #     if stock in self.excel_files:
        #         modified_base64, message = await self.processor.process_df_ratios(
        #             raw_df_ratios[index], stock, self.excel_files[stock]
        #         )
        #         self.excel_files[stock] = modified_base64
        #         print(f"✅ {message}")



from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess
async def main():
    # 可以輸入多個股票代碼
    stocks = ['O', 'AAPL', 'MSFT']  # 範例：多個股票
    scraper = StockScraper(stocks=stocks)
    process = StockProcess()
    manager = StockManager(scraper=scraper, processor=process)
    await manager.process_wacc(stocks=stocks)


if __name__ == "__main__":
    asyncio.run(main())