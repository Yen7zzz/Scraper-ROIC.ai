import asyncio
import os
from datetime import datetime
from stock_class.RareLimitManager import RateLimitManager
import shutil
import tempfile
import sys


class StockManager:
    def __init__(self, scraper, processor, stocks, validator=None, max_concurrent=3, delay=1):
        self.scraper = scraper
        self.processor = processor
        self.stocks = stocks.get('final_stocks')
        self.us_stocks = stocks.get('us_stocks')
        self.non_us_stocks = stocks.get('non_us_stocks')
        self.validator = validator
        self.pattern1 = r'^[a-zA-Z\-\.]{1,5}'
        self.pattern2 = r'是非美國企業,此頁面需付費!$'
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay

        # 修改：分別管理兩種模板的Excel檔案
        self.fundamental_excel_files = {}  # 股票分析模板 (base64)
        self.option_excel_files = {}  # 選擇權模板 (檔案路徑) 👈 改這裡的註解

        self.max_concurrent = max_concurrent

        # 🔥 新增: 選擇權模板路徑
        self.option_template_path = self._get_option_template_path()

        # 🔥 新增: 臨時資料夾 (用於存放複製的檔案)
        self.temp_dir = None

        # 使用共享的速率限制管理器
        if hasattr(processor, 'rate_limiter'):
            self.rate_limiter = processor.rate_limiter
        else:
            self.rate_limiter = RateLimitManager(request_delay=2.0)

        if not hasattr(processor, 'rate_limiter'):
            processor.rate_limiter = self.rate_limiter

    def _get_option_template_path(self):
        """取得選擇權模板路徑 (支援打包後的 exe)"""
        if getattr(sys, 'frozen', False):
            # 打包後: exe 所在目錄
            base_path = os.path.dirname(sys.executable)
        else:
            # 開發環境: 專案根目錄
            current_file = os.path.abspath(__file__)
            base_path = os.path.dirname(os.path.dirname(current_file))

        template_path = os.path.join(base_path, 'excel_template', 'Option_Chain_Template.xlsm')

        # 驗證檔案是否存在
        if not os.path.exists(template_path):
            print(f"⚠️ 警告: 找不到選擇權模板檔案")
            print(f"   預期路徑: {template_path}")

        return template_path

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
        """快速初始化 - 直接複製模板檔案 (不用 base64)"""

        # 檢查模板是否存在
        if not os.path.exists(self.option_template_path):
            print(f"❌ 找不到選擇權模板: {self.option_template_path}")
            print(f"   請確認 'excel_templates/Option_Chain_Template.xlsm' 存在")
            return False

        print(f"📦 正在快速複製選擇權模板給 {len(self.stocks)} 支股票...")
        print(f"   模板來源: {self.option_template_path}")

        # 建立臨時資料夾
        self.temp_dir = tempfile.mkdtemp()
        print(f"   臨時資料夾: {self.temp_dir}")

        import time
        start_time = time.time()

        for stock in self.stocks:
            try:
                # 🔥 直接複製檔案 (超快!)
                temp_file = os.path.join(self.temp_dir, f"{stock}_option.xlsm")
                shutil.copy2(self.option_template_path, temp_file)

                # 儲存檔案路徑 (不是 base64!)
                self.option_excel_files[stock] = temp_file
                print(f"   ✅ {stock} 模板已複製")

            except Exception as e:
                print(f"   ❌ {stock} 複製失敗: {e}")
                return False

        elapsed = time.time() - start_time
        print(f"✅ 所有模板複製完成 (耗時 {elapsed:.2f} 秒)")
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
        """將臨時檔案移動到輸出資料夾"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []

        for stock in self.stocks:
            if stock in self.option_excel_files:
                try:
                    temp_file = self.option_excel_files[stock]

                    # 🔥 檢查臨時檔案是否存在
                    if not os.path.exists(temp_file):
                        print(f"⚠️ {stock} 臨時檔案不存在: {temp_file}")
                        continue

                    # 🔥 直接移動檔案到輸出資料夾
                    output_filename = f"Option_{stock}.xlsm"
                    final_path = os.path.join(output_folder, output_filename)

                    # 如果目標檔案已存在,先刪除
                    if os.path.exists(final_path):
                        os.remove(final_path)

                    shutil.move(temp_file, final_path)

                    saved_files.append(final_path)
                    print(f"✅ {stock} 選擇權檔案已儲存至: {final_path}")

                except Exception as e:
                    print(f"❌ {stock} 選擇權檔案儲存失敗: {e}")

        # 🔥 清理臨時資料夾
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                print(f"🧹 已清理臨時資料夾")
            except Exception as e:
                print(f"⚠️ 清理臨時資料夾時發生錯誤: {e}")

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

    async def process_earnings_dates(self):
        """處理財報日期（支援雙模板）"""
        raw_earnings = await self.scraper.run_earnings_dates()
        print(f"獲取到的財報日期數據: {raw_earnings}")

        for earnings_dict in raw_earnings:
            for stock, earnings_data in earnings_dict.items():
                if earnings_data is None:
                    print(f"❌ {stock} 的財報日期為 None")
                    continue

                # 🔥 寫入 Fundamental 模板（如果有）
                if stock in self.fundamental_excel_files:
                    modified_base64, message = self.processor.write_earnings_date_to_fundamental_excel(
                        stock=stock,
                        earnings_data=earnings_data,
                        excel_base64=self.fundamental_excel_files[stock]
                    )
                    if modified_base64:
                        self.fundamental_excel_files[stock] = modified_base64
                        print(f"✅ {message}")
                    else:
                        print(f"❌ {message}")

                # 🔥 寫入 Option 模板（如果有）
                if stock in self.option_excel_files:
                    file_path, message = self.processor.write_earnings_date_to_option_excel(
                        stock=stock,
                        earnings_data=earnings_data,
                        file_path=self.option_excel_files[stock]
                    )
                    # Option 模板的檔案路徑保持不變
                    print(f"{'✅' if '成功' in message else '❌'} {message}")

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
        """處理 Barchart 波動率數據（批次優化版）"""
        # 🔥 步驟 1: 批次抓取
        raw_barchart = await self.scraper.run_barchart()
        print(f"獲取到的 Barchart 數據: {raw_barchart}")

        # 🔥 步驟 2: 暫存數據
        barchart_cache = {}
        for barchart_dict in raw_barchart:
            for stock, barchart_text in barchart_dict.items():
                if barchart_text is not None and not isinstance(barchart_text, dict):
                    barchart_cache[stock] = barchart_text

        # 🔥 步驟 3: 批次寫入
        if barchart_cache:
            print(f"\n📝 開始批次寫入 {len(barchart_cache)} 支股票的 Barchart 數據...")

            stock_data = {}
            excel_files = {}

            for stock, barchart_text in barchart_cache.items():
                if stock in self.option_excel_files:
                    stock_data[stock] = {'barchart': barchart_text}
                    excel_files[stock] = self.option_excel_files[stock]

            if stock_data:
                updated_files, messages = self.processor.batch_write_options_to_excel(
                    stock_data,
                    excel_files
                )

                for stock, new_base64 in updated_files.items():
                    self.option_excel_files[stock] = new_base64

                for stock, message in messages.items():
                    print(message)

    async def process_option_chains(self):
        """處理選擇權鏈數據（批次優化版）"""
        print("\n開始抓取選擇權鏈數據...")

        # 🔥 步驟 1: 批次抓取所有選擇權數據
        raw_option_data = await self.scraper.run_option_chains()
        print(f"獲取到的選擇權數據: {len(raw_option_data)} 檔")

        # 🔥 步驟 2: 準備數據結構 (不立即寫入)
        stock_data_cache = {}  # {stock: {'option_chain': df, 'beta': None, 'barchart': None}}

        for option_dict in raw_option_data:
            for stock, option_data in option_dict.items():
                # 初始化該股票的數據容器
                if stock not in stock_data_cache:
                    stock_data_cache[stock] = {
                        'option_chain': None,
                        'beta': None,
                        'barchart': None
                    }

                # 檢查是否有錯誤
                if isinstance(option_data, dict) and "error" in option_data:
                    print(f"❌ {stock} 選擇權數據抓取失敗: {option_data['error']}")
                    continue

                # 展平數據為 DataFrame
                option_df = self.processor.flatten_option_chain(option_data, stock)

                if option_df is not None and not option_df.empty:
                    stock_data_cache[stock]['option_chain'] = option_df
                    print(f"✅ {stock} 選擇權數據已準備 ({len(option_df)} 筆合約)")
                else:
                    print(f"❌ {stock} 的選擇權數據展平失敗")

        # 🔥 步驟 3: 批次寫入所有數據
        if stock_data_cache:
            print(f"\n📝 開始批次寫入 {len(stock_data_cache)} 支股票的選擇權數據...")

            # 準備要寫入的數據
            stocks_to_write = {}
            excel_files_to_write = {}

            for stock in stock_data_cache.keys():
                if stock in self.option_excel_files:
                    stocks_to_write[stock] = stock_data_cache[stock]
                    excel_files_to_write[stock] = self.option_excel_files[stock]

            if stocks_to_write:
                # 呼叫批次寫入方法
                updated_files, messages = self.processor.batch_write_options_to_excel(
                    stocks_to_write,
                    excel_files_to_write
                )

                # 更新 Excel 檔案
                for stock, new_base64 in updated_files.items():
                    self.option_excel_files[stock] = new_base64

                # 顯示結果
                for stock, message in messages.items():
                    print(message)
            else:
                print("⚠️ 沒有需要寫入的數據")
        else:
            print("⚠️ 沒有成功抓取到任何選擇權數據")

    async def process_beta(self):
        """處理 Beta 數據（批次優化版）"""
        if not self.option_excel_files:
            print("ℹ️ 未啟用選擇權模板，跳過 Beta 數據處理")
            return

        # 🔥 步驟 1: 批次抓取 Beta
        raw_beta = await self.scraper.run_beta()
        print(f"獲取到的 Beta 數據: {raw_beta}")

        # 🔥 步驟 2: 暫存數據
        beta_cache = {}
        for beta_dict in raw_beta:
            for stock, beta_value in beta_dict.items():
                if beta_value is not None:
                    beta_cache[stock] = beta_value

        # 🔥 步驟 3: 批次寫入
        if beta_cache:
            print(f"\n📝 開始批次寫入 {len(beta_cache)} 支股票的 Beta 數據...")

            stock_data = {}
            excel_files = {}

            for stock, beta_value in beta_cache.items():
                if stock in self.option_excel_files:
                    stock_data[stock] = {'beta': beta_value}
                    excel_files[stock] = self.option_excel_files[stock]

            if stock_data:
                updated_files, messages = self.processor.batch_write_options_to_excel(
                    stock_data,
                    excel_files
                )

                for stock, new_base64 in updated_files.items():
                    self.option_excel_files[stock] = new_base64

                for stock, message in messages.items():
                    print(message)