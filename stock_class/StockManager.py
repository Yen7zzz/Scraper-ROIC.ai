import asyncio
import os
from stock_class.RareLimitManager import RateLimitManager
import shutil
import tempfile
import sys


class StockManager:
    def __init__(self, scraper, processor, stocks, validator=None, max_concurrent=3, delay=1):
        """
        初始化 StockManager

        🔥 改進：改回二分類系統（US / Non-US）

        Args:
            stocks: 股票字典 {
                'final_stocks': [...],      # 所有有效股票
                'us_stocks': [...],          # 美國公司（可跑 financial/ratios）
                'non_us_stocks': [...]       # 非美國公司（全跳過 financial/ratios）
            }
        """
        self.scraper = scraper
        self.processor = processor

        # 🔥 改回二分類
        self.stocks = stocks.get('final_stocks', [])
        self.us_stocks = stocks.get('us_stocks', [])
        self.non_us_stocks = stocks.get('non_us_stocks', [])
        self.validator = validator
        self.pattern1 = r'^[a-zA-Z\-\.]{1,5}'
        self.pattern2 = r'是非美國企業,此頁面需付費!$'
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.delay = delay

        # 修改：分別管理兩種模板的Excel檔案
        self.fundamental_excel_files = {}
        self.option_excel_files = {}

        self.max_concurrent = max_concurrent

        # 🔥 選擇權模板路徑
        self.option_template_path = self._get_option_template_path()
        self.temp_dir = None

        self.cached_earnings_data = None  # 緩存財報日期數據

        # 使用共享的速率限制管理器
        if hasattr(processor, 'rate_limiter'):
            self.rate_limiter = processor.rate_limiter
        else:
            self.rate_limiter = RateLimitManager(request_delay=2.0)

        if not hasattr(processor, 'rate_limiter'):
            processor.rate_limiter = self.rate_limiter

        # 🔥 新增：傳遞資源給 scraper 和 processor
        self._setup_cross_references()

    def _setup_cross_references(self):
        """設定類別之間的引用關係"""

        # 🔥 1. 傳遞 stock_exchanges 給 scraper（供 TradingView 使用）
        if self.validator and hasattr(self.validator, 'stock_exchanges'):
            self.scraper.stock_exchanges = self.validator.stock_exchanges
            print(f"✓ 已傳遞 {len(self.validator.stock_exchanges)} 個交易所資訊給 StockScraper")

        # 🔥 2. 傳遞 schwab_client 給 processor（供 others_data 使用）
        if self.scraper and hasattr(self.scraper, 'schwab_client'):
            self.processor.schwab_client = self.scraper.schwab_client
            print(f"✓ 已傳遞 Schwab Client 給 StockProcess")

    def _get_option_template_path(self):
        """取得選擇權模板路徑"""
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            current_file = os.path.abspath(__file__)
            base_path = os.path.dirname(os.path.dirname(current_file))

        template_path = os.path.join(base_path, 'excel_template', 'Option_Chain_Template.xlsm')

        if not os.path.exists(template_path):
            print(f"⚠️ 警告: 找不到選擇權模板檔案")
            print(f"   預期路徑: {template_path}")

        return template_path

    async def initialize_excel_files(self):
        """
        為所有股票初始化 Excel 檔案

        🔥 改進：為所有股票（包括非美國公司）初始化
                非美國公司的 financial/ratios 會被清空
        """
        for stock in self.stocks:  # 🔥 改回 self.stocks
            excel_base64, message = self.processor.create_excel_from_base64(stock)
            if excel_base64:
                self.fundamental_excel_files[stock] = excel_base64
                print(f"✅ {message}")
            else:
                print(f"❌ {message}")
                return False
        return True

    async def initialize_option_excel_files(self):
        """快速初始化選擇權模板（只為 COE + ADR）"""

        if not os.path.exists(self.option_template_path):
            print(f"❌ 找不到選擇權模板: {self.option_template_path}")
            return False

        print(f"📦 正在複製選擇權模板給 {len(self.stocks)} 支股票...")

        self.temp_dir = tempfile.mkdtemp()

        import time
        start_time = time.time()

        for stock in self.stocks:
            try:
                temp_file = os.path.join(self.temp_dir, f"{stock}_option.xlsm")
                shutil.copy2(self.option_template_path, temp_file)
                self.option_excel_files[stock] = temp_file
                print(f"   ✅ {stock} 模板已複製")

            except Exception as e:
                print(f"   ❌ {stock} 複製失敗: {e}")
                return False

        elapsed = time.time() - start_time
        print(f"✅ 所有模板複製完成 (耗時 {elapsed:.2f} 秒)")
        return True

    async def process_financial(self):
        """
        處理 Financial 數據

        🔥 改進：只處理美國公司，跳過非美國公司
        """
        if not self.us_stocks:
            print("ℹ️ 沒有美國公司需要處理 Financial 數據")
            return

        print(f"\n🔄 開始處理 Financial 數據（僅 {len(self.us_stocks)} 支美國公司）...")

        # 🔥 只跑美國公司
        raw_df_financial = await self.scraper.run_financial()

        for index, stock in enumerate(self.us_stocks):
            if stock in self.fundamental_excel_files:
                modified_base64, message = await self.processor.process_df_financial(
                    raw_df_financial[index], stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

        # 🔥 非美國公司：清空 Financial 區域
        if self.non_us_stocks:
            print(f"\n⚠️  正在清空 {len(self.non_us_stocks)} 支非美國公司的 Financial 區域...")
            for stock in self.non_us_stocks:
                if stock in self.fundamental_excel_files:
                    # print('stock:',stock)
                    modified_base64, message = await self.processor.process_df_financial(
                        None, stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_ratios(self):
        """
        處理 Ratios 數據

        🔥 改進：只處理美國公司，跳過非美國公司
        """
        if not self.us_stocks:
            print("ℹ️ 沒有美國公司需要處理 Ratios 數據")
            return

        print(f"\n🔄 開始處理 Ratios 數據（僅 {len(self.us_stocks)} 支美國公司）...")

        # 🔥 只跑美國公司
        raw_df_ratios = await self.scraper.run_ratios()

        for index, stock in enumerate(self.us_stocks):
            if stock in self.fundamental_excel_files:
                modified_base64, message = await self.processor.process_df_ratios(
                    raw_df_ratios[index], stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

        # 🔥 非美國公司：清空 Ratios 區域
        if self.non_us_stocks:
            print(f"\n⚠️  正在清空 {len(self.non_us_stocks)} 支非美國公司的 Ratios 區域...")
            for stock in self.non_us_stocks:
                if stock in self.fundamental_excel_files:
                    modified_base64, message = await self.processor.process_df_ratios(
                        None, stock, self.fundamental_excel_files[stock]
                    )
                    self.fundamental_excel_files[stock] = modified_base64
                    print(f"✅ {message}")

    async def process_others_data(self):
        """
        處理其他數據（CurrentPrice 等）

        🔥 改進：處理 COE + ADR（使用 Schwab API，無限制）
        """
        print(f"\n🔄 開始處理其他數據（{len(self.stocks)} 支股票）...")

        for stock in self.stocks:
            if stock in self.fundamental_excel_files:
                modified_base64, message = await self.processor.others_data(
                    stock, self.fundamental_excel_files[stock]
                )
                self.fundamental_excel_files[stock] = modified_base64
                print(f"✅ {message}")

    async def process_combined_summary_and_metrics(self):
        """
        處理 Summary 和指標數據

        🔥 改進：處理 COE + ADR（roic.ai 的 Summary 頁面可以看）
        """
        print(f"\n🔄 開始處理 Summary 和指標數據（{len(self.stocks)} 支股票）...")

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

    async def process_seekingalpha(self):
        """處理 Revenue Growth（COE + ADR 都處理）"""
        print(f"\n🔄 開始處理 Revenue Growth 數據（{len(self.stocks)} 支股票）...")

        raw_revenue_growth = await self.scraper.run_seekingalpha()

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

    async def process_wacc(self):
        """處理 WACC（COE + ADR 都處理）"""
        print(f"\n🔄 開始處理 WACC 數據（{len(self.stocks)} 支股票）...")

        raw_wacc = await self.scraper.run_wacc()

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

    async def process_TradingView(self):
        """處理 TradingView（COE + ADR 都處理）"""
        print(f"\n🔄 開始處理 TradingView 數據（{len(self.stocks)} 支股票）...")

        raw_TradingView = await self.scraper.run_TradingView()

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

    async def fetch_earnings_dates(self):
        """
        【步驟1】抓取財報日期數據（只爬網站，不寫入）

        Returns:
            抓取的原始數據（會自動緩存）
        """
        print(f"\n🔄 開始抓取財報日期（{len(self.stocks)} 支股票）...")

        # 🔥 關鍵：抓取並緩存
        self.cached_earnings_data = await self.scraper.run_earnings_dates()

        print(f"✅ 財報日期抓取完成")
        return self.cached_earnings_data

    async def write_earnings_to_fundamental(self):
        """
        【步驟2a】將緩存的財報日期寫入 Fundamental 模板

        ⚠️ 必須先調用 fetch_earnings_dates()
        """
        if self.cached_earnings_data is None:
            print("⚠️ 警告：尚未抓取財報日期，請先調用 fetch_earnings_dates()")
            return

        print(f"\n📄 正在寫入財報日期到 Fundamental 模板...")

        for earnings_dict in self.cached_earnings_data:
            for stock, earnings_data in earnings_dict.items():
                if earnings_data is None:
                    print(f"   ⚠️ {stock} 的財報日期為 None")
                    continue

                # 只寫入 Fundamental 模板
                if stock in self.fundamental_excel_files:
                    modified_base64, message = self.processor.write_earnings_date_to_fundamental_excel(
                        stock=stock,
                        earnings_data=earnings_data,
                        excel_base64=self.fundamental_excel_files[stock]
                    )
                    if modified_base64:
                        self.fundamental_excel_files[stock] = modified_base64
                        print(f"   ✅ {message}")
                    else:
                        print(f"   ❌ {message}")

        print("✅ Fundamental 模板寫入完成")

    async def write_earnings_to_option(self):
        """
        【步驟2b】將緩存的財報日期寫入 Option 模板

        ⚠️ 必須先調用 fetch_earnings_dates()
        """
        if self.cached_earnings_data is None:
            print("⚠️ 警告：尚未抓取財報日期，請先調用 fetch_earnings_dates()")
            return

        print(f"\n📄 正在寫入財報日期到 Option 模板...")

        for earnings_dict in self.cached_earnings_data:
            for stock, earnings_data in earnings_dict.items():
                if earnings_data is None:
                    print(f"   ⚠️ {stock} 的財報日期為 None")
                    continue

                # 只寫入 Option 模板
                if stock in self.option_excel_files:
                    file_path, message = self.processor.write_earnings_date_to_option_excel(
                        stock=stock,
                        earnings_data=earnings_data,
                        file_path=self.option_excel_files[stock]
                    )
                    print(f"   {'✅' if '成功' in message else '❌'} {message}")

        print("✅ Option 模板寫入完成")

    # ===== 🔥 可選：保留舊方法作為向後兼容 =====
    async def process_earnings_dates(self):
        """
        ⚠️ 已過時：建議使用新的分離方法

        舊方法：同時抓取並寫入兩個模板（保留用於向後兼容）
        """
        await self.fetch_earnings_dates()
        await self.write_earnings_to_fundamental()
        await self.write_earnings_to_option()

    def save_all_excel_files(self, output_folder=None):
        """保存所有股票分析Excel檔案"""
        if output_folder is None:
            output_folder = os.getcwd()

        saved_files = []

        for stock in self.stocks:  # 🔥 改回 self.stocks
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

        for stock in self.stocks:  # 🔥 改回 self.stocks
            if stock in self.option_excel_files:
                try:
                    temp_file = self.option_excel_files[stock]

                    if not os.path.exists(temp_file):
                        print(f"⚠️ {stock} 臨時檔案不存在")
                        continue

                    output_filename = f"Option_{stock}.xlsm"
                    final_path = os.path.join(output_folder, output_filename)

                    if os.path.exists(final_path):
                        os.remove(final_path)

                    shutil.move(temp_file, final_path)

                    saved_files.append(final_path)
                    print(f"✅ {stock} 選擇權檔案已儲存")

                except Exception as e:
                    print(f"❌ {stock} 選擇權檔案儲存失敗: {e}")

        # 清理臨時資料夾
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                print(f"🧹 已清理臨時資料夾")
            except Exception as e:
                print(f"⚠️ 清理臨時資料夾時發生錯誤: {e}")

        return saved_files

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
# 🔥 總結修改：
# 1. 新增 coe_stocks, adr_stocks, non_us_stocks 分類
# 2. 新增 scrapable_stocks = coe_stocks + adr_stocks
# 3. 新增 _setup_cross_references() 傳遞 stock_exchanges 和 schwab_client
# 4. financial 和 ratios 只處理 coe_stocks，並清空 adr_stocks 的對應區域
# 5. 其他數據處理 scrapable_stocks（COE + ADR）