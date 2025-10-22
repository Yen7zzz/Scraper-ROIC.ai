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
        self.validator = validator
        self.pattern1 = r'^[a-zA-Z\-\.]{1,5}'
        self.pattern2 = r'是非美國企業，此頁面需付費！$'
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay

        # 修改：分別管理兩種模板的Excel檔案
        self.fundamental_excel_files = {}  # 股票分析模板
        self.option_excel_files = {}  # 選擇權模板

        self.max_concurrent = max_concurrent

        # 使用共享的速率限制管理器=
        if hasattr(processor, 'rate_limiter'):
            self.rate_limiter = processor.rate_limiter
        else:
            self.rate_limiter = RateLimitManager(request_delay=2.0)

        if not hasattr(processor, 'rate_limiter'):
            processor.rate_limiter = self.rate_limiter

    async def initialize_excel_files(self):
        """為所有股票初始化股票分析Excel檔案"""
        for stock in self.stocks:
            excel_base64, message = self.processor.create_excel_from_base64(stock)
            if excel_base64:
                self.fundamental_excel_files[stock] = excel_base64
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
                return False
        return True

    async def initialize_option_excel_files(self):
        """為所有股票初始化選擇權Excel檔案"""
        for stock in self.stocks:
            excel_base64, message = self.processor.create_option_excel_from_base64(stock)
            if excel_base64:
                self.option_excel_files[stock] = excel_base64
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
                return False
        return True

    async def process_financial(self):
        """處理Financial數據"""
        if self.us_stocks:
            raw_df_financial = await self.scraper.run_financial()

            for index, stock in enumerate(self.us_stocks):
                if stock in self.fundamental_excel_files:
                    modified_base64, message = await self.processor.process_df_financial(
                        raw_df_financial[index], stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

        if self.non_us_stocks:
            raw_df_financial = None

            for index, stock in enumerate(self.non_us_stocks):
                if stock in self.fundamental_excel_files:
                    modified_base64, message = await self.processor.process_df_financial(
                        raw_df_financial, stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_ratios(self):
        """處理Ratios數據"""
        if self.us_stocks:
            raw_df_ratios = await self.scraper.run_ratios()
            for index, stock in enumerate(self.us_stocks):
                if stock in self.fundamental_excel_files:
                    modified_base64, message = await self.processor.process_df_ratios(
                        raw_df_ratios[index], stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

        if self.non_us_stocks:
            raw_df_ratios = None
            for index, stock in enumerate(self.non_us_stocks):
                if stock in self.fundamental_excel_files:
                    modified_base64, message = await self.processor.process_df_ratios(
                        raw_df_ratios, stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_others_data(self):
        """處理其他數據"""
        for stock in self.stocks:
            if stock in self.fundamental_excel_files:
                modified_base64, message = await self.processor.others_data(
                    stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

    def save_all_excel_files(self, output_folder=None):
        """保存所有股票分析Excel檔案"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for stock in self.stocks:
            if stock in self.fundamental_excel_files:
                output_filename = f"Stock_{stock}.xlsx"
                output_path = os.path.join(output_folder, output_filename)

                if self.processor.save_excel_to_file(self.fundamental_excel_files[stock], output_path):
                    saved_files.append(output_path)
                    print(f"✅ {stock} 檔案已保存至：{output_path}")
                else:
                    print(f"❌ {stock} 檔案保存失敗")

        return saved_files

    def save_all_option_excel_files(self, output_folder=None):
        """保存所有選擇權Excel檔案"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []

        for stock in self.stocks:
            if stock in self.option_excel_files:
                output_filename = f"Option_{stock}.xlsm"
                output_path = os.path.join(output_folder, output_filename)

                if self.processor.save_excel_to_file(self.option_excel_files[stock], output_path):
                    saved_files.append(output_path)
                    print(f"✅ {stock} 選擇權檔案已保存至：{output_path}")
                else:
                    print(f"❌ {stock} 選擇權檔案保存失敗")

        return saved_files

    async def process_seekingalpha(self):
        """處理Revenue growth數據"""
        raw_revenue_growth = await self.scraper.run_seekingalpha()
        print(f"獲取到的revenue_growth數據: {raw_revenue_growth}")

        for revenue_dict in raw_revenue_growth:
            for stock, revenue_data in revenue_dict.items():
                if stock in self.fundamental_excel_files and revenue_data is not None:
                    if isinstance(revenue_data, dict) and "error" not in revenue_data:
                        modified_base64, message = self.processor.write_seekingalpha_data_to_excel(
                            stock=stock,
                            raw_revenue_growth=revenue_data,
                            excel_base64=self.fundamental_excel_files[stock]
                        )
                        if modified_base64:
                            self.fundamental_excel_files[stock] = modified_base64
                            print(f"✅ {message}")
                        else:
                            print(f"❌ {message}")
                    else:
                        print(f"❌ {stock} 的數據包含錯誤或格式不正確: {revenue_data}")
                else:
                    if stock not in self.fundamental_excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if revenue_data is None:
                        print(f"❌ {stock} 的revenue_growth值為None")

    async def process_wacc(self):
        """處理wacc數據"""
        raw_wacc = await self.scraper.run_wacc()
        print(f"獲取到的WACC數據: {raw_wacc}")

        for wacc_dict in raw_wacc:
            for stock, wacc_value in wacc_dict.items():
                if stock in self.fundamental_excel_files and wacc_value is not None:
                    modified_base64, message = self.processor.write_wacc_data_to_excel(
                        stock=stock,
                        wacc_value=wacc_value,
                        excel_base64=self.fundamental_excel_files[stock]
                    )
                    if modified_base64:
                        self.fundamental_excel_files[stock] = modified_base64
                        print(f"✅ {message}")
                    else:
                        print(f"❌ {message}")
                else:
                    if stock not in self.fundamental_excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if wacc_value is None:
                        print(f"❌ {stock} 的WACC值為None")

    async def process_TradingView(self):
        """處理TradingView數據"""
        raw_TradingView = await self.scraper.run_TradingView()
        print(f"獲取到的TradingView數據: {raw_TradingView}")

        for TradingView_dict in raw_TradingView:
            for stock, TradingView_value in TradingView_dict.items():
                if stock in self.fundamental_excel_files and TradingView_value is not None:
                    modified_base64, message = self.processor.write_TradeingView_data_to_excel(
                        stock=stock,
                        tradingview_data=TradingView_value,
                        excel_base64=self.fundamental_excel_files[stock]
                    )
                    if modified_base64:
                        self.fundamental_excel_files[stock] = modified_base64
                        print(f"✅ {message}")
                    else:
                        print(f"❌ {message}")
                else:
                    if stock not in self.fundamental_excel_files:
                        print(f"❌ {stock} 的Excel檔案不存在")
                    if TradingView_value is None:
                        print(f"❌ {stock} 的TradingView值為None")

    async def process_combined_summary_and_metrics(self):
        """處理合併的Summary和指標數據"""
        summary_results, metrics_results = await self.scraper.run_combined_summary_and_metrics()

        for index, stock in enumerate(self.stocks):
            if stock in self.fundamental_excel_files and index < len(summary_results):
                modified_base64, message = await self.processor.process_df_summary(
                    summary_results[index][stock], stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

        for index, stock in enumerate(self.stocks):
            if stock in self.fundamental_excel_files and index < len(metrics_results):
                modified_base64, message = await self.processor.EPS_PE_MarketCap_data_write_to_excel(
                    {stock: [metrics_results[index][stock]]}, stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_barchart_for_options(self):
        """處理Barchart波動率數據（選擇權模板）"""
        raw_barchart = await self.scraper.run_barchart()
        print(f"獲取到的Barchart數據: {raw_barchart}")

        for barchart_dict in raw_barchart:
            for stock, barchart_text in barchart_dict.items():
                if stock in self.option_excel_files and barchart_text is not None:
                    # 檢查是否包含錯誤信息
                    if not isinstance(barchart_text, dict) or "error" not in barchart_text:
                        modified_base64, message = self.processor.write_barchart_data_to_excel(
                            stock=stock,
                            barchart_text=barchart_text,
                            excel_base64=self.option_excel_files[stock]
                        )
                        if modified_base64:
                            self.option_excel_files[stock] = modified_base64
                            print(f"✅ {message}")
                        else:
                            print(f"❌ {message}")
                    else:
                        print(f"❌ {stock} 的Barchart數據包含錯誤: {barchart_text}")
                else:
                    if stock not in self.option_excel_files:
                        print(f"❌ {stock} 的選擇權Excel檔案不存在")
                    if barchart_text is None:
                        print(f"❌ {stock} 的Barchart數據為None")

    async def process_option_chains(self):
        """處理選擇權鏈數據（整合到選擇權Excel）"""
        print("\n開始抓取選擇權鏈數據...")
        raw_option_data = await self.scraper.run_option_chains()

        print(f"獲取到的選擇權數據: {len(raw_option_data)} 檔")

        for option_dict in raw_option_data:
            for stock, option_data in option_dict.items():
                if stock in self.option_excel_files:
                    # 檢查是否有錯誤
                    if isinstance(option_data, dict) and "error" in option_data:
                        print(f"❌ {stock} 選擇權數據抓取失敗: {option_data['error']}")
                        continue

                    # 展平數據為DataFrame
                    option_df = self.processor.flatten_option_chain(option_data, stock)

                    if option_df is not None and not option_df.empty:
                        # 寫入Excel
                        modified_base64, message = self.processor.write_option_chain_to_excel(
                            stock=stock,
                            option_df=option_df,
                            excel_base64=self.option_excel_files[stock]
                        )

                        if modified_base64:
                            self.option_excel_files[stock] = modified_base64
                            print(message)
                        else:
                            print(f"❌ {message}")
                    else:
                        print(f"❌ {stock} 的選擇權數據展平失敗")
                else:
                    print(f"❌ {stock} 的選擇權Excel檔案不存在")
