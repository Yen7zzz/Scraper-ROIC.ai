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
        # print(raw_df_ratios)
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

    async def process_seekingalpha(self, stocks):
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

    async def process_wacc(self, stocks):
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



from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess


# 修正後的 main 函數
async def main():
    stocks = ['O', 'AAPL', 'CCL', 'NVDA']
    scraper = StockScraper(stocks=stocks)
    process = StockProcess()
    manager = StockManager(scraper=scraper, processor=process)

    # 初始化Excel檔案
    success = await manager.initialize_excel_files(stocks=stocks)
    if not success:
        print("Excel檔案初始化失敗，程式終止")
        return

    # 處理SeekingAlpha數據
    await manager.process_seekingalpha(stocks=stocks)

    print("所有股票的revenue growth數據處理完成！")


if __name__ == "__main__":
    asyncio.run(main())