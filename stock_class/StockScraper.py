# 🔥 完整的瀏覽器路徑偵測邏輯
# 複製此函數替換 StockScraper.py 中的 setup_playwright_path()

import sys
import os


def setup_playwright_path():
    """
    設定 Playwright 瀏覽器路徑

    偵測優先順序：
    1. 打包後的相對路徑（與 .exe 同層的 ms-playwright）
    2. PyInstaller 的臨時資料夾（_MEIPASS）
    3. 開發環境的 AppData 路徑
    """

    # 🔥 方法 1：檢查是否在打包環境中（最優先）
    if getattr(sys, 'frozen', False):
        # 打包後的路徑（.exe 所在目錄）
        base_path = os.path.dirname(sys.executable)

        # 檢查與 .exe 同層的 ms-playwright 資料夾
        relative_browser_path = os.path.join(base_path, 'ms-playwright')

        if os.path.exists(relative_browser_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = relative_browser_path
            print(f"✓ 使用打包的瀏覽器: {relative_browser_path}")

            # 驗證 Chromium 是否存在
            chromium_path = os.path.join(relative_browser_path, 'chromium-1187', 'chrome-win', 'chrome.exe')
            if os.path.exists(chromium_path):
                print(f"✓ Chromium 驗證通過: {chromium_path}")
            else:
                print(f"⚠️ 警告：Chromium 執行檔不存在於預期位置")
                print(f"   預期位置: {chromium_path}")

            return

        # 🔥 方法 2：檢查 PyInstaller 的臨時解壓縮資料夾
        if hasattr(sys, '_MEIPASS'):
            meipass_browser_path = os.path.join(sys._MEIPASS, 'ms-playwright')

            if os.path.exists(meipass_browser_path):
                os.environ['PLAYWRIGHT_BROWSERS_PATH'] = meipass_browser_path
                print(f"✓ 使用 _MEIPASS 瀏覽器: {meipass_browser_path}")
                return

        # 如果打包環境找不到，警告使用者
        print("⚠️ 警告：打包環境中找不到 ms-playwright 資料夾")
        print("   程式可能無法正常運行，請確認以下路徑是否存在：")
        print(f"   1. {relative_browser_path}")

    # 🔥 方法 3：開發環境的 AppData 路徑（僅供開發時使用）
    else:
        appdata_browser_path = os.path.join(
            os.path.expanduser('~'),
            'AppData',
            'Local',
            'ms-playwright'
        )

        if os.path.exists(appdata_browser_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = appdata_browser_path
            print(f"✓ 開發環境：使用 AppData 瀏覽器")
            print(f"   路徑: {appdata_browser_path}")
        else:
            print("⚠️ 警告：未找到 Playwright 瀏覽器")
            print("   請執行：playwright install chromium")


# 在模組載入時立即設定
setup_playwright_path()

# 現在才導入 playwright 和其他模組
import asyncio
import pandas as pd
import random
from io import StringIO
from bs4 import BeautifulSoup
import json
import re
import schwabdev

# 自定義異常類別
class TokenExpiredException(Exception):
    """Token 過期異常"""
    pass


class StockScraper:
    def __init__(self, stocks, config=None, headless=True, max_concurrent=15):
        """
        初始化爬蟲類別。
        """
        self.stocks = stocks.get('final_stocks')
        self.us_stocks = stocks.get('us_stocks')
        self.non_us_stocks = stocks.get('non_us_stocks')

        # 🔥 新增：coe_stocks 和 adr_stocks
        self.coe_stocks = stocks.get('coe_stocks', [])
        self.adr_stocks = stocks.get('adr_stocks', [])

        self.config = config
        self.headless = headless
        self.max_concurrent = max_concurrent
        self.browser = None
        self.playwright = None
        self.contexts = []
        self.contexts_lock = asyncio.Lock()
        self._validate_schwab_config()

        # 🔥 關鍵修改：Schwab Client 重用
        self.schwab_client = None
        self.schwab_client_lock = asyncio.Lock()

        # 🔥 新增：交易所資訊（供 TradingView 使用）
        self.stock_exchanges = {}  # {stock: 'NYSE'} - 由 StockManager 設定

        # 🔥 新增：立即初始化 Schwab Client（用於驗證階段）
        if self.schwab_available:
            try:
                self.initialize_schwab_client()
                print("✅ Schwab Client 已在初始化階段準備就緒")
            except Exception as e:
                print(f"⚠️ Schwab Client 初始化失敗: {e}")
                print("   驗證功能將無法使用")
                self.schwab_available = False

    # 在 StockScraper 類別中，只需要修改 initialize_schwab_client 方法

    def initialize_schwab_client(self):
        """
        初始化 Schwab Client（只執行一次）- 支援 3.0.0 .db 格式

        ⚠️ 注意：此方法現在會在 __init__ 時立即執行，
                 確保驗證階段可以使用 schwab_client
        """
        if self.schwab_client is not None:
            return  # 已初始化，跳過

        if not self.schwab_available or not self.config:
            raise ValueError("Schwab API 配置未設定")

        print("🔧 初始化 Schwab API Client...")

        # 計算路徑
        if getattr(sys, 'frozen', False):
            base_path = os.path.dirname(sys.executable)
        else:
            current_file = os.path.abspath(__file__)
            project_root = os.path.dirname(os.path.dirname(current_file))
            base_path = os.path.join(project_root, 'schwab')

        # 🔥 關鍵修正：完整的 tokens.db 檔案路徑
        tokens_file_path = os.path.join(base_path, 'tokens.db')

        print(f"📁 Token DB 路徑: {tokens_file_path}")
        print(f"📁 檔案是否存在: {os.path.exists(tokens_file_path)}")

        # 🔥 使用正確的參數：tokens_db (完整檔案路徑)
        self.schwab_client = schwabdev.Client(
            self.config['app_key'],
            self.config['app_secret'],
            callback_url="https://127.0.0.1",
            tokens_db=tokens_file_path,
            timeout=30
        )

        print("✅ Schwab Client 已初始化（可用於驗證和選擇權鏈）")

    def _validate_schwab_config(self):
        """驗證 Schwab API 配置是否完整"""
        if self.config is None:
            print("⚠️ 警告：未提供 Schwab API 配置")
            print("選擇權鏈功能將無法使用")
            self.schwab_available = False
            return

        required_keys = ['app_key', 'app_secret']
        missing_keys = [key for key in required_keys if not self.config.get(key)]

        if missing_keys:
            print(f"⚠️ 警告：Schwab API 配置不完整，缺少：{', '.join(missing_keys)}")
            print("選擇權鏈功能將無法使用")
            self.schwab_available = False
        else:
            print("✓ Schwab API 配置已載入")
            self.schwab_available = True

    async def setup_browser(self):
        """啟動瀏覽器（加入反偵測）- 自動偵測螢幕並置中"""
        print("🔧 正在啟動 Playwright...")
        from playwright.async_api import async_playwright

        self.playwright = await async_playwright().start()

        print(f"🔧 正在啟動 Chromium（{'無頭' if self.headless else '有頭'}模式，反偵測）...")

        # 🔥 基礎參數（headless 和 有頭模式都需要）
        base_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--ignore-certifcate-errors',
            '--ignore-certifcate-errors-spki-list',
        ]

        # 🔥 根據 headless 狀態決定是否添加視窗參數
        if not self.headless:
            # 只有在有頭模式才處理視窗位置
            try:
                from screeninfo import get_monitors
                monitors = get_monitors()
                primary_monitor = next((m for m in monitors if m.is_primary), monitors[0])

                screen_width = primary_monitor.width
                screen_height = primary_monitor.height
                x_offset = primary_monitor.x
                y_offset = primary_monitor.y

                print(f"📺 偵測到主螢幕解析度: {screen_width}x{screen_height} (偏移: {x_offset}, {y_offset})")
                print(f"📺 共 {len(monitors)} 個螢幕")

            except Exception as e:
                print(f"⚠️ 無法偵測螢幕解析度，使用預設值: {e}")
                screen_width = 1920
                screen_height = 1080
                x_offset = 0
                y_offset = 0

            # 設定視窗大小
            window_width = int(screen_width * 0.8)
            window_height = int(screen_height * 0.85)
            x_position = x_offset + (screen_width - window_width) // 2
            y_position = y_offset + (screen_height - window_height) // 2

            print(f"🪟 瀏覽器視窗: {window_width}x{window_height} (位置: {x_position},{y_position})")

            # 添加視窗相關參數
            base_args.append('--start-maximized')
        else:
            # 無頭模式：不添加任何視窗參數
            print("👻 無頭模式：瀏覽器將在背景執行")

        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=base_args
        )

        if self.headless:
            print("✅ 瀏覽器啟動成功（無頭模式）")
        else:
            print("✅ 瀏覽器啟動成功（視窗已置中）")

    async def cleanup(self):
        """清理資源 - 確保 Playwright 子進程完全關閉"""
        import asyncio

        print("🧹 開始清理 StockScraper 資源...")

        try:
            # Step 1: 關閉所有 contexts
            if hasattr(self, 'contexts') and self.contexts:
                print(f"🧹 關閉 {len(self.contexts)} 個未關閉的 context...")
                contexts_to_close = list(self.contexts)

                for context in contexts_to_close:
                    try:
                        await asyncio.wait_for(context.close(), timeout=2.0)
                    except Exception as e:
                        print(f"⚠️ Context 關閉錯誤: {e}")

                self.contexts.clear()
                print("✅ 所有 context 已關閉")

            # Step 2: 關閉瀏覽器
            if self.browser:
                print("🧹 關閉 Playwright 瀏覽器...")
                try:
                    await asyncio.wait_for(self.browser.close(), timeout=3.0)
                    print("✅ 瀏覽器已關閉")
                except Exception as e:
                    print(f"⚠️ 瀏覽器關閉錯誤: {e}")
                finally:
                    self.browser = None

            # Step 3: 停止 Playwright
            if self.playwright:
                print("🧹 停止 Playwright...")
                try:
                    await asyncio.wait_for(self.playwright.stop(), timeout=3.0)
                    print("✅ Playwright 已停止")
                except Exception as e:
                    print(f"⚠️ Playwright 停止錯誤: {e}")
                finally:
                    self.playwright = None

            # 🔥 Step 4: 關鍵！等待子進程完全結束
            print("🧹 等待子進程完全結束...")
            await asyncio.sleep(1.0)  # 給 1 秒讓子進程清理
            print("✅ 子進程清理完成")

            # Step 5: 清理 Schwab Client
            if self.schwab_client:
                self.schwab_client = None

            print("✅ StockScraper 資源清理完成")

        except Exception as e:
            print(f"❌ 清理過程發生錯誤: {e}")
            # 確保變數被重置
            self.browser = None
            self.playwright = None
            self.schwab_client = None
            if hasattr(self, 'contexts'):
                self.contexts.clear()

    async def fetch_financials_data(self, stock, semaphore):
        """抓取單一股票的數據（financials）。"""
        async with semaphore:
            context = None  # 🔥 初始化
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                    java_script_enabled=True
                )
                # 🔥 追蹤 context
                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page_financials = await context.new_page()
                    financials = await asyncio.gather(self.get_financials(stock, page_financials))
                    return {stock: financials}
                finally:
                    await context.close()
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                # 確保 context 被關閉
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {"stock": stock, "error": str(e)}

    async def get_financials(self, stock, page, retries=3):
        """抓取特定股票的財務資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/financials'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='networkidle', timeout=100000) # networkidle

                # 2025/09/23 更新新邏輯
                # await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                # content = await page.content()
                # dfs = pd.read_html(StringIO(content))
                # return dfs

                # 之前的邏輯
                if await page.query_selector(
                        'div.rounded-lg.bg-card.text-card-foreground.shadow-sm.mx-auto.flex.w-\\[500px\\].flex-col.items-center.border.drop-shadow-lg'):
                    return f'{stock}是非美國企業，此頁面須付費！'
                else:
                    await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                    content = await page.content()
                    dfs = pd.read_html(StringIO(content))
                    return dfs

            except Exception as e:
                attempt += 1
                if attempt == retries:
                    return f"Error for {stock}: {e}"

        return f"Failed to retrieve data for {stock}"

    async def run_financial(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_financials_data(stock, semaphore) for stock in self.us_stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    async def fetch_ratios_data(self, stock, semaphore):
        """抓取單一股票的數據（Ratios）。"""
        async with semaphore:
            context = None  # 🔥 初始化
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                    java_script_enabled=True
                )
                # 🔥 追蹤 context
                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page_ratios = await context.new_page()
                    ratios = await asyncio.gather(self.get_ratios(stock, page_ratios))
                    # print({stock: ratios})
                    return {stock: ratios}
                finally:
                    await context.close()
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                # 確保 context 被關閉
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {"stock": stock, "error": str(e)}

    async def get_ratios(self, stock, page, retries=3):
        """抓取特定股票的比率資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/ratios'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='load', timeout=50000)

                # 2025/09/23 更新新邏輯
                # await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                # content = await page.content()
                # dfs = pd.read_html(StringIO(content))
                # return dfs

                # 之前的邏輯
                if await page.query_selector(
                        'div.rounded-lg.bg-card.text-card-foreground.shadow-sm.mx-auto.flex.w-\\[500px\\].flex-col.items-center.border.drop-shadow-lg'):
                    return f'{stock}是非美國企業，此頁面須付費！'
                else:
                    await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                    content = await page.content()
                    dfs = pd.read_html(StringIO(content))
                    return dfs

            except Exception as e:
                attempt += 1
                if attempt == retries:
                    return f"Error for {stock}: {e}"

        return f"Failed to retrieve data for {stock}"

    async def run_ratios(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_ratios_data(stock, semaphore) for stock in self.us_stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    # async def fetch_EPS_PE_MarketCap_data(self, stock, semaphore):
    #     """抓取單一股票的數據（EPS_PE_MarketCap）。"""
    #     async with semaphore:
    #         try:
    #             context = await self.browser.new_context(
    #                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    #                 viewport={"width": 800, "height": 600},
    #             )
    #             try:
    #                 page_EPS_PE_MarketCap = await context.new_page()
    #                 EPS_PE_MarketCap = await asyncio.gather(self.get_EPS_PE_MarketCap(stock, page_EPS_PE_MarketCap))
    #                 return {stock: EPS_PE_MarketCap}
    #             finally:
    #                 await context.close()
    #         except Exception as e:
    #             return {"stock": stock, "error": str(e)}

    # async def get_EPS_PE_MarketCap(self, stock, page, retries=3):
    #     """抓取特定股票的EPS/PE/MarketCap數據 - 2025新版HTML結構"""
    #     url = f'https://www.roic.ai/quote/{stock}'
    #     attempt = 0
    #
    #     while attempt < retries:
    #         try:
    #             await asyncio.sleep(random.uniform(1, 3))
    #             await page.goto(url, wait_until='load', timeout=30000)
    #
    #             # 等待關鍵指標容器載入
    #             await page.wait_for_selector('div[data-cy="company_header_ratios"]', timeout=30000)
    #
    #             content = await page.content()
    #             soup = BeautifulSoup(content, 'html.parser')
    #
    #             # 🔥 修正：使用新的HTML結構
    #             ratios_container = soup.find('div', {'data-cy': 'company_header_ratios'})
    #
    #             if ratios_container:
    #                 print(f"找到 {stock} 的指標容器")
    #
    #                 # 提取所有指標項目
    #                 metric_items = ratios_container.find_all('div', class_='shrink-0 flex-col')
    #
    #                 if len(metric_items) >= 3:
    #                     dic_data = {}
    #
    #                     for item in metric_items:
    #                         # 🔥 關鍵修正：適應新舊兩種class順序
    #                         # 新版: class="text-foreground flex text-lg"
    #                         # 舊版: class="flex text-lg text-foreground"
    #                         value_span = item.find('span', class_='text-foreground')
    #
    #                         # 確保是大字（text-lg）
    #                         if value_span and 'text-lg' in value_span.get('class', []):
    #                             label_span = item.find('span', class_='text-muted-foreground')
    #
    #                             # 確保是小字（text-sm uppercase）
    #                             if label_span and 'text-sm' in label_span.get('class',
    #                                                                           []) and 'uppercase' in label_span.get(
    #                                     'class', []):
    #                                 label = label_span.get_text(strip=True)
    #                                 value_text = value_span.get_text(strip=True)
    #
    #                                 # 根據標籤類型進行不同處理
    #                                 if label in ['EPS', 'P/E']:
    #                                     try:
    #                                         dic_data[label] = float(value_text)
    #                                     except ValueError:
    #                                         dic_data[label] = value_text
    #                                 else:
    #                                     # Market Cap, Next Earn等保持字串
    #                                     dic_data[label] = value_text
    #
    #                     if dic_data:
    #                         print(f"成功提取 {stock} 的指標數據: {dic_data}")
    #                         return dic_data
    #                     else:
    #                         print(f"⚠️ 解析後沒有有效數據")
    #                 else:
    #                     print(f"⚠️ 指標項目數量不足: 找到 {len(metric_items)} 個項目")
    #
    #             # 🔥 備用方案 2：直接搜尋所有符合條件的 span
    #             if not ratios_container or not dic_data:
    #                 print(f"嘗試備用方案抓取 {stock} 的指標...")
    #
    #                 # 找出所有可能的數值 span
    #                 value_spans = soup.find_all('span', class_='text-foreground')
    #                 label_spans = soup.find_all('span', class_='text-muted-foreground')
    #
    #                 # 過濾出正確的元素（必須包含 text-lg 和 text-sm）
    #                 filtered_values = [s for s in value_spans if 'text-lg' in s.get('class', [])]
    #                 filtered_labels = [s for s in label_spans if
    #                                    'text-sm' in s.get('class', []) and 'uppercase' in s.get('class', [])]
    #
    #                 if len(filtered_values) >= 3 and len(filtered_labels) >= 3:
    #                     dic_data = {}
    #
    #                     for i in range(min(len(filtered_values), len(filtered_labels))):
    #                         label = filtered_labels[i].get_text(strip=True)
    #                         value_text = filtered_values[i].get_text(strip=True)
    #
    #                         # 只處理我們關心的指標
    #                         if label in ['EPS', 'P/E', 'MARKET CAP', 'Market Cap', 'NEXT EARN', 'Next Earn']:
    #                             if label in ['EPS', 'P/E']:
    #                                 try:
    #                                     dic_data[label] = float(value_text)
    #                                 except ValueError:
    #                                     dic_data[label] = value_text
    #                             else:
    #                                 dic_data[label] = value_text
    #
    #                     if dic_data:
    #                         print(f"備用方案成功提取 {stock} 的指標數據: {dic_data}")
    #                         return dic_data
    #
    #             # 如果所有方法都失敗
    #             return {'error': f'無法找到 {stock} 的指標數據'}
    #
    #         except Exception as e:
    #             attempt += 1
    #             print(f"第 {attempt} 次嘗試失敗: {e}")
    #             if attempt < retries:
    #                 await asyncio.sleep(random.uniform(2, 5))
    #             else:
    #                 return {'error': f'抓取 {stock} 數據時發生錯誤: {e}'}
    #
    #     return {'error': f'Failed to retrieve data for {stock}'}

    async def fetch_combined_summary_and_metrics_data(self, stock, semaphore):
        """同時抓取Summary表格數據和EPS/PE/MarketCap指標數據"""
        async with semaphore:
            context = None  # 🔥 初始化
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                # 🔥 追蹤 context
                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page = await context.new_page()

                    # 一次性獲取兩種數據
                    summary_data, metrics_data = await self.get_combined_data(stock, page)

                    return {
                        stock: {
                            'summary': summary_data,
                            'metrics': metrics_data
                        }
                    }
                finally:
                    await context.close()
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                # 確保 context 被關閉
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {"stock": stock, "error": str(e)}

    async def get_combined_data(self, stock, page, retries=3):
        """從同一頁面同時獲取Summary表格和指標數據 - 2025新版"""
        URL = f'https://www.roic.ai/quote/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='load', timeout=50000)

                # 等待兩種關鍵元素載入完成
                await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                await page.wait_for_selector('div[data-cy="company_header_ratios"]', timeout=30000)

                # 獲取頁面內容
                content = await page.content()

                # ===== 1. 解析 Summary 表格數據 =====
                summary_data = None
                try:
                    dfs = pd.read_html(StringIO(content))
                    summary_data = dfs
                    print(f"成功解析 {stock} 的表格數據，共 {len(dfs)} 個表格")
                except Exception as e:
                    print(f"解析 {stock} 表格數據失敗: {e}")
                    summary_data = []

                # ===== 2. 解析指標數據（EPS/PE/Market Cap）=====
                metrics_data = None
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    ratios_container = soup.find('div', {'data-cy': 'company_header_ratios'})

                    if ratios_container:
                        metric_items = ratios_container.find_all('div', class_='shrink-0 flex-col')

                        if len(metric_items) >= 3:
                            metrics_data = {}

                            for item in metric_items:
                                # 🔥 關鍵修正：適應新class順序
                                value_span = item.find('span', class_='text-foreground')

                                if value_span and 'text-lg' in value_span.get('class', []):
                                    label_span = item.find('span', class_='text-muted-foreground')

                                    if label_span and 'text-sm' in label_span.get('class', []):
                                        label = label_span.get_text(strip=True)
                                        value_text = value_span.get_text(strip=True)

                                        if label in ['EPS', 'P/E']:
                                            try:
                                                metrics_data[label] = float(value_text)
                                            except ValueError:
                                                metrics_data[label] = value_text
                                        else:
                                            metrics_data[label] = value_text

                            print(f"成功解析 {stock} 的指標數據: {metrics_data}")
                        else:
                            metrics_data = {}
                    else:
                        # 🔥 備用方案
                        value_spans = [s for s in soup.find_all('span', class_='text-foreground')
                                       if 'text-lg' in s.get('class', [])]
                        label_spans = [s for s in soup.find_all('span', class_='text-muted-foreground')
                                       if 'text-sm' in s.get('class', []) and 'uppercase' in s.get('class', [])]

                        if len(value_spans) >= 3 and len(label_spans) >= 3:
                            metrics_data = {}
                            for i in range(min(len(value_spans), len(label_spans))):
                                label = label_spans[i].get_text(strip=True)
                                value_text = value_spans[i].get_text(strip=True)

                                if label in ['EPS', 'P/E', 'MARKET CAP', 'Market Cap', 'NEXT EARN', 'Next Earn']:
                                    if label in ['EPS', 'P/E']:
                                        try:
                                            metrics_data[label] = float(value_text)
                                        except ValueError:
                                            metrics_data[label] = value_text
                                    else:
                                        metrics_data[label] = value_text

                            if metrics_data:
                                print(f"備用方案成功: {metrics_data}")
                        else:
                            metrics_data = {}

                except Exception as e:
                    print(f"解析 {stock} 指標數據失敗: {e}")
                    metrics_data = {}

                return summary_data, metrics_data

            except Exception as e:
                attempt += 1
                print(f"第 {attempt} 次嘗試失敗: {e}")
                if attempt == retries:
                    return [], {}
                await asyncio.sleep(random.uniform(2, 5))

        return [], {}

    async def run_combined_summary_and_metrics(self):
        """執行合併的Summary和指標數據抓取"""
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_combined_summary_and_metrics_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)

            # 分離結果以保持與現有代碼的兼容性
            summary_results = []
            metrics_results = []

            for item in result:
                for stock, data in item.items():
                    if stock != "stock" and "error" not in item:  # 排除錯誤項目
                        summary_results.append({stock: data['summary']})
                        metrics_results.append({stock: data['metrics']})
                    else:
                        # 處理錯誤情況
                        summary_results.append(item)
                        metrics_results.append(item)

            return summary_results, metrics_results

        finally:
            await self.cleanup()


    # async def EPS_Growth_Rate_and_write_to_excel(self, stock, excel_base64):
    #     """抓取EPS成長率並寫入Excel"""
    #     if '-' in stock:
    #         stock = ''.join(['.' if char == '-' else char for char in stock])
    #
    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(f'https://api.stockboss.io/api/symbol?symbol={stock}') as response:
    #             content = await response.text()
    #             dic = json.loads(content)
    #             # print(dic['symbol']['guru_summary']['summary']['summary']['company_data']['wacc'])
    #             # wacc = float(dic['symbol']['guru_summary']['summary']['summary']['company_data']['wacc'])/100
    #             l_eps_growth5y = []
    #             try:
    #                 EPS_Growth_Rate_3_Year = \
    #                     dic['symbol']['keyratio']['keyratio']['annuals']['3-Year EPS Growth Rate %'][-1]
    #                 EPS_Growth_Rate_5_Year = \
    #                     dic['symbol']['keyratio']['keyratio']['annuals']['5-Year EPS Growth Rate %'][-1]
    #                 EPS_Growth_Rate_10_Year = \
    #                     dic['symbol']['keyratio']['keyratio']['annuals']['10-Year EPS Growth Rate %'][-1]
    #
    #                 EPS_Growth_Rate_3_Year = 0 if EPS_Growth_Rate_3_Year == '-' else EPS_Growth_Rate_3_Year
    #                 EPS_Growth_Rate_5_Year = 0 if EPS_Growth_Rate_5_Year == '-' else EPS_Growth_Rate_5_Year
    #                 EPS_Growth_Rate_10_Year = 0 if EPS_Growth_Rate_10_Year == '-' else EPS_Growth_Rate_10_Year
    #
    #                 l_eps_growth5y = l_eps_growth5y + [EPS_Growth_Rate_3_Year, EPS_Growth_Rate_5_Year,
    #                                                    EPS_Growth_Rate_10_Year]
    #
    #             except KeyError as e:
    #                 return f"EPS_Growth_Rate的dictionary錯誤：{stock}", excel_base64
    #
    #             # 選擇成長率：如果最小值大於 0，則取最小值，否則取最大值
    #             selected_growth_rate = min(l_eps_growth5y) / 100 if min(l_eps_growth5y) > 0 else max(
    #                 l_eps_growth5y) / 100
    #             # print(selected_growth_rate)
    #             # print(wacc)
    #             # 寫入 Excel
    #             try:
    #                 excel_binary = base64.b64decode(excel_base64)
    #                 excel_buffer = io.BytesIO(excel_binary)
    #                 wb = load_workbook(excel_buffer)
    #                 ws = wb.worksheets[3]  # 假設需要寫入的工作表是第四個
    #
    #                 ws['C3'] = None
    #                 ws['C3'] = selected_growth_rate
    #                 # ws['C6'] = wacc
    #
    #                 output_buffer = io.BytesIO()
    #                 wb.save(output_buffer)
    #                 output_buffer.seek(0)
    #                 modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')
    #
    #                 return f"{stock}的EPS成長率及WACC成功寫入", modified_base64
    #
    #             except Exception as e:
    #                 return f"寫入Excel時發生錯誤：{e}", excel_base64

    # async def fetch_seekingalpha_data(self, stock, semaphore):
    #     async with semaphore:
    #         try:
    #             context = await self.browser.new_context(
    #                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    #                 viewport={"width": 800, "height": 600},  # 增加視窗大小
    #                 java_script_enabled=True,  # 確保JavaScript啟用
    #             )
    #             try:
    #                 page_summary = await context.new_page()
    #                 summary = await self.get_seekingalpha_html(stock, page_summary)
    #                 return {stock: summary}
    #             finally:
    #                 await context.close()
    #         except Exception as e:
    #             return {"stock": stock, "error": str(e)}

    async def get_seekingalpha_html(self, stock, page, retries=3):
        """抓取特定股票的摘要資料 - PerimeterX CAPTCHA 檢測版"""
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        URL = f'https://seekingalpha.com/symbol/{stock}/growth'
        attempt = 0

        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的資料 (第 {attempt + 1} 次)...")

                # 隨機等待
                await asyncio.sleep(random.uniform(3, 7))

                # 前往頁面
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

                # 等待頁面渲染
                await asyncio.sleep(random.uniform(2, 4))

                # 模擬人類瀏覽行為
                for _ in range(random.randint(2, 4)):
                    x = random.randint(100, 800)
                    y = random.randint(100, 600)
                    await page.mouse.move(x, y)
                    await asyncio.sleep(random.uniform(0.3, 0.8))

                # 滾動頁面
                scroll_positions = [200, 400, 600, 400, 200]
                for pos in scroll_positions:
                    await page.evaluate(f'window.scrollTo(0, {pos})')
                    await asyncio.sleep(random.uniform(0.5, 1.2))

                # 🔥 方法 1: 檢測 PerimeterX CAPTCHA（精準檢測）
                px_captcha = await page.query_selector('#px-captcha-wrapper, #px-captcha, .px-captcha-container')

                if px_captcha:
                    # 確認是否可見
                    is_visible = await px_captcha.is_visible()
                    if is_visible:
                        print(f"\n{'🔴' * 30}")
                        print(f"⚠️  {stock} 偵測到 PerimeterX 驗證！")
                        print("⚠️  請在瀏覽器中完成「按壓不放」驗證")
                        print("⚠️  驗證完成後程式將自動繼續...")
                        print(f"{'🔴' * 30}\n")

                        # 無限等待直到 CAPTCHA 消失
                        await self._wait_for_px_captcha_resolution(stock, page)

                # 🔥 方法 2: 反向檢測（備用方案）
                target_section = await page.query_selector('section[data-test-id="card-container-growth-rates"]')

                if not target_section:
                    print(f"\n{'🟡' * 30}")
                    print(f"⚠️  {stock} 目標數據未出現")
                    print("⚠️  可能需要驗證或頁面載入延遲")
                    print("⚠️  等待中...")
                    print(f"{'🟡' * 30}\n")

                    # 無限等待直到目標出現
                    await self._wait_for_target_element(stock, page)

                # 🔥 確認目標元素已載入
                await page.wait_for_selector(
                    'section[data-test-id="card-container-growth-rates"] table[data-test-id="table"]',
                    timeout=10000
                )
                await page.wait_for_selector(
                    'section[data-test-id="card-container-growth-rates"] th:has-text("Revenue")',
                    timeout=10000
                )

                await asyncio.sleep(2)

                # ===== 開始解析數據 =====
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                growth_section = soup.find('section', {'data-test-id': 'card-container-growth-rates'})

                if not growth_section:
                    raise Exception("未找到 Growth Rates section")

                target_table = growth_section.find('table', {'data-test-id': 'table'})

                if target_table:
                    print("找到正確的 Growth Rates 表格，開始解析...")

                    # 解析表頭
                    header_row = target_table.find('thead').find('tr') if target_table.find('thead') else None
                    headers = []
                    if header_row:
                        header_cells = header_row.find_all('th')
                        for cell in header_cells:
                            div_text = cell.find('div')
                            if div_text:
                                header_text = div_text.get_text(strip=True)
                            else:
                                header_text = cell.get_text(strip=True)
                            headers.append(header_text)

                    print(f"表頭: {headers}")

                    # 驗證表頭結構
                    expected_headers = ['YoY', '3Y', '5Y', '10Y']
                    if not all(h in headers for h in expected_headers):
                        raise Exception("表頭結構不正確")

                    # 找到 5Y 和 10Y 的位置
                    try:
                        header_5y_index = headers.index('5Y')
                        header_10y_index = headers.index('10Y')
                        print(f"5Y位置: {header_5y_index}, 10Y位置: {header_10y_index}")
                    except ValueError as e:
                        raise Exception(f"找不到5Y或10Y表頭: {e}")

                    # 解析表格內容
                    tbody = target_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')

                        for row in rows:
                            row_data = []

                            # 處理第一個th（行標題）
                            th = row.find('th')
                            if th:
                                div_text = th.find('div')
                                if div_text:
                                    row_name = div_text.get_text(strip=True)
                                else:
                                    row_name = th.get_text(strip=True)
                                row_data.append(row_name)

                            # 處理其他td
                            tds = row.find_all('td')
                            for td in tds:
                                div_text = td.find('div')
                                if div_text:
                                    cell_value = div_text.get_text(strip=True)
                                else:
                                    cell_value = td.get_text(strip=True)
                                row_data.append(cell_value)

                            # 檢查是否為Revenue行
                            if 'Revenue' in row_data[0] and 'Revenue per Share' not in row_data[0]:
                                print(f"找到Revenue行: {row_data}")

                                if len(row_data) > max(header_5y_index, header_10y_index):
                                    result = {
                                        "5Y": row_data[header_5y_index],
                                        "10Y": row_data[header_10y_index]
                                    }
                                    print(f"提取結果: {result}")
                                    return result
                                else:
                                    return {"error": f"Revenue行數據不足: {row_data}"}

                        return {"error": "未找到Revenue行"}
                    else:
                        return {"error": "未找到tbody"}
                else:
                    return {"error": "未找到Growth Rates表格"}

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    wait_time = random.uniform(20, 40)
                    print(f"等待 {wait_time:.1f} 秒後重試...")
                    await asyncio.sleep(wait_time)

        return {"error": f"Failed to retrieve data for {stock} after {retries} attempts"}

    async def _wait_for_px_captcha_resolution(self, stock, page):
        """等待 PerimeterX CAPTCHA 被解決（無限等待）"""

        check_count = 0

        while True:
            await asyncio.sleep(5)  # 每 5 秒檢查一次
            check_count += 1

            # 檢查 CAPTCHA 是否還在
            px_captcha = await page.query_selector('#px-captcha-wrapper, #px-captcha')

            if px_captcha:
                is_visible = await px_captcha.is_visible()
                if not is_visible:
                    # CAPTCHA 元素還在但不可見了
                    print(f"✅ {stock} PerimeterX 驗證已通過！")
                    break
            else:
                # CAPTCHA 元素完全消失
                print(f"✅ {stock} PerimeterX 驗證已通過！")
                break

            # 每 20 秒提示一次
            if check_count % 4 == 0:
                elapsed = check_count * 5
                print(f"   {stock} 等待 PerimeterX 驗證... (已等待 {elapsed} 秒)")

        # 驗證通過後再等待一下
        await asyncio.sleep(random.uniform(2, 4))

    async def _wait_for_target_element(self, stock, page):
        """等待目標元素出現（無限等待）"""

        check_count = 0

        while True:
            await asyncio.sleep(5)  # 每 5 秒檢查一次
            check_count += 1

            # 檢查目標元素是否出現
            target = await page.query_selector('section[data-test-id="card-container-growth-rates"]')

            if target:
                print(f"✅ {stock} 目標數據已出現！")
                break

            # 每 20 秒提示一次
            if check_count % 4 == 0:
                elapsed = check_count * 5
                print(f"   {stock} 等待目標數據... (已等待 {elapsed} 秒)")

        # 數據出現後再等待一下
        await asyncio.sleep(2)

    async def run_seekingalpha(self):
        """執行 SeekingAlpha 數據抓取 - 強制有頭模式處理 Cloudflare"""

        # 🔥 臨時保存原始 headless 設定
        original_headless = self.headless

        # 🔥 強制使用有頭模式（顯示瀏覽器）
        self.headless = False

        try:
            await self.setup_browser()

            context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
            )

            try:
                page = await context.new_page()
                result = []

                # 依序處理每個股票
                for i, stock in enumerate(self.stocks):
                    print(f"\n{'=' * 50}")
                    print(f"正在處理 {stock} ({i + 1}/{len(self.stocks)})...")
                    print(f"{'=' * 50}")

                    stock_data = await self.get_seekingalpha_html(stock, page)
                    result.append({stock: stock_data})

                    # 🔥 強化: 增加延遲變化幅度
                    if i < len(self.stocks) - 1:
                        base_delay = 3 + (i * 2)
                        wait_time = random.uniform(base_delay, base_delay + 10)
                        print(f"\n⏳ 等待 {wait_time:.1f} 秒後處理下一個股票...")
                        await asyncio.sleep(wait_time)

                return result

            finally:
                await context.close()

        finally:
            # 🔥 恢復原始設定
            self.headless = original_headless
            await self.cleanup()

    async def fetch_wacc_data(self, stock, semaphore):
        async with semaphore:
            context = None
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                    locale='en-US',
                    timezone_id='America/New_York',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Cache-Control': 'max-age=0',
                    }
                )

                # 注入反偵測腳本
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });

                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };

                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );

                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });

                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                """)

                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page = await context.new_page()
                    wacc_value = await self.get_wacc_html(stock, page)
                    return {stock: wacc_value}
                finally:
                    await context.close()
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                print(f"❌ {stock} 發生錯誤: {e}")
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {stock: None}

    async def get_wacc_html(self, stock, page, retries=3):
        """抓取特定股票的WACC資料並回傳int數值。"""
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        URL = f'https://www.gurufocus.com/term/wacc/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的WACC資料 (第 {attempt + 1} 次)...")

                # 隨機等待時間
                await asyncio.sleep(random.uniform(3, 6))

                # 前往頁面
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

                # 模擬人類瀏覽行為
                await asyncio.sleep(random.uniform(1, 2))
                await page.evaluate('window.scrollTo(0, 200)')
                await asyncio.sleep(random.uniform(0.5, 1))

                # 等待關鍵內容載入
                try:
                    await page.wait_for_selector('h1', timeout=30000)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"等待頁面載入時發生錯誤: {e}")

                # 獲取頁面內容
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # 尋找包含WACC數值的特定元素
                wacc_value = None

                # 方法1: 尋找包含":X.X% (As of"模式的font標籤
                font_elements = soup.find_all('font', style=True)
                for font in font_elements:
                    text = font.get_text(strip=True)
                    if '% (As of' in text and text.startswith(':'):
                        # 提取百分比數值
                        match = re.search(r':(\d+\.?\d*)%', text)
                        if match:
                            wacc_value = float(match.group(1)) / 100
                            print(f"✓ 找到 {stock} 的WACC值: {wacc_value}")
                            break

                if wacc_value is not None:
                    return wacc_value
                else:
                    print(f"⚠️ 未能找到 {stock} 的WACC數值")
                    return None

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    wait_time = random.uniform(8, 15)
                    print(f"等待 {wait_time:.1f} 秒後重試...")
                    await asyncio.sleep(wait_time)

        print(f"❌ Failed to retrieve WACC data for {stock} after {retries} attempts")
        return None

    async def run_wacc(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_wacc_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result


    async def fetch_TradingView_data(self, stock, semaphore):
        async with semaphore:
            context = None
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                    locale='zh-TW',
                    timezone_id='Asia/Taipei',
                    # 添加更多真實瀏覽器特徵
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Cache-Control': 'max-age=0',
                    }
                )

                # 注入反偵測腳本
                await context.add_init_script("""
                    // 覆蓋 webdriver 檢測
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });

                    // 覆蓋 Chrome 特徵
                    window.chrome = {
                        runtime: {}
                    };

                    // 覆蓋 permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );

                    // 覆蓋 plugins
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });

                    // 覆蓋 languages
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-TW', 'zh', 'en-US', 'en']
                    });
                """)

                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page = await context.new_page()
                    beta_value = await self.get_TradingView_html(stock, page)
                    return {stock: beta_value}
                finally:
                    await context.close()
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                # 確保 context 被關閉
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {stock: None}  # 如果出錯返回None

    async def get_TradingView_html(self, stock, page, retries=3):
        """抓取特定股票的trading-view資料 - 使用 Schwab API 的 exchangeName"""

        # 🔥 移除 yfinance，改用 stock_exchanges
        exchange_name = self.stock_exchanges.get(stock, 'NYSE')  # 預設 NYSE

        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        # 🔥 直接使用 exchangeName，不需要再做對應
        URL = f'https://www.tradingview.com/symbols/{exchange_name}-{stock}/financials-earnings/?earnings-period=FY&revenues-period=FY'

        attempt = 0

        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的trading-view資料 (第 {attempt + 1} 次)...")

                # 🔥 強化: 更長的隨機等待
                await asyncio.sleep(random.uniform(3, 7))

                # 前往頁面
                await page.goto(URL, wait_until='networkidle', timeout=60000)

                # 🔥 強化: 更真實的瀏覽行為
                await asyncio.sleep(random.uniform(2, 4))

                # 模擬滑鼠移動軌跡
                for _ in range(random.randint(2, 4)):
                    x = random.randint(100, 800)
                    y = random.randint(100, 600)
                    await page.mouse.move(x, y)
                    await asyncio.sleep(random.uniform(0.3, 0.8))

                # 滾動頁面
                scroll_positions = [200, 400, 600, 400, 200]
                for pos in scroll_positions:
                    await page.evaluate(f'window.scrollTo(0, {pos})')
                    await asyncio.sleep(random.uniform(0.5, 1.2))

                # 等待關鍵內容載入
                try:
                    await page.wait_for_selector('h1', timeout=30000)
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"等待頁面載入時發生錯誤: {e}")

                # 獲取頁面內容
                content = await page.content()

                # 使用BeautifulSoup解析trading-view數值
                soup = BeautifulSoup(content, 'html.parser')

                # 解析年份
                years = []
                year_elements = soup.find_all('div', class_='value-OxVAcLqi')
                for element in year_elements:
                    text = element.get_text(strip=True)
                    if text.isdigit() and len(text) == 4:
                        years.append(int(text))

                # 如果沒找到年份，嘗試另一種方式
                if not years:
                    values_container = soup.find('div', class_='values-AtxjAQkN')
                    if values_container:
                        year_divs = values_container.find_all('div', class_='value-OxVAcLqi')
                        for div in year_divs:
                            text = div.get_text(strip=True)
                            if text.isdigit() and len(text) == 4:
                                years.append(int(text))

                if not years:
                    print(f"無法找到年份資料對於 {stock}")
                    return None

                # 初始化資料字典
                data = {
                    'Year': years,
                    'Reported': [None] * len(years),
                    'Estimate': [None] * len(years),
                    'Surprise': [None] * len(years)
                }

                # 解析三種類型的資料
                data_types = ['Reported', 'Estimate', 'Surprise']

                for data_type in data_types:
                    container = soup.find('div', {'data-name': data_type})
                    if not container:
                        print(f"找不到 {data_type} 資料容器")
                        continue

                    values_section = container.find('div', class_='values-C9MdAMrq')
                    if not values_section:
                        print(f"找不到 {data_type} 的數值區域")
                        continue

                    value_containers = values_section.find_all('div', class_='container-OxVAcLqi')

                    for i, value_container in enumerate(value_containers):
                        if i >= len(years):
                            break

                        lock_button = value_container.find('button', class_='lockButton-N_j3rnsK')
                        if lock_button:
                            continue

                        value_div = value_container.find('div', class_='value-OxVAcLqi')
                        if value_div:
                            value = value_div.get_text(strip=True)
                            if value == '—' or value == '-':
                                value = None
                            elif value.startswith('‪') and value.endswith('‬'):
                                value = value.strip('‪‬')

                            data[data_type][i] = value

                # 建立DataFrame
                df_original = pd.DataFrame(data)

                # 只保留有資料的行
                mask = df_original[['Reported', 'Estimate', 'Surprise']].notna().any(axis=1)
                df_filtered = df_original[mask].reset_index(drop=True)

                # 轉換成橫向格式
                if len(df_filtered) > 0:
                    years_list = df_filtered['Year'].tolist()

                    transposed_data = {
                        'Year': years_list,
                        'Reported': df_filtered['Reported'].tolist(),
                        'Estimate': df_filtered['Estimate'].tolist(),
                        'Surprise': df_filtered['Surprise'].tolist()
                    }

                    result_dict = {}

                    for i, year in enumerate(years_list):
                        result_dict[str(year)] = [
                            transposed_data['Reported'][i],
                            transposed_data['Estimate'][i],
                            transposed_data['Surprise'][i]
                        ]

                    df_final = pd.DataFrame(result_dict, index=['Reported', 'Estimate', 'Surprise'])

                    print(f"成功解析 {stock} 的資料，格式為 {df_final.shape[1]} 年份 x {df_final.shape[0]} 指標")
                    return df_final
                else:
                    print(f"未找到 {stock} 的有效資料")
                    return None

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    wait_time = random.uniform(20, 40)  # 🔥 增加重試等待時間
                    print(f"等待 {wait_time:.1f} 秒後重試...")
                    await asyncio.sleep(wait_time)

        print(f"Failed to retrieve TradingView data for {stock} after {retries} attempts")
        return None

    async def run_TradingView(self):
        """批次執行 TradingView 數據抓取 - 先集中處理 CAPTCHA，再批次爬蟲"""

        # 🔥 臨時保存原始 headless 設定
        original_headless = self.headless

        # 🔥 強制使用有頭模式（顯示瀏覽器）
        self.headless = False

        try:
            await self.setup_browser()

            print("\n" + "=" * 60)
            print("🚀 階段 1: 集中處理 TradingView CAPTCHA 驗證")
            print("⚠️  即將打開所有股票的頁面")
            print("⚠️  請依序完成所有 CAPTCHA 驗證")
            print("⚠️  完成所有驗證後，程式將自動開始抓取數據")
            print("=" * 60 + "\n")

            # 🔥 階段 1: 打開所有頁面並處理 CAPTCHA
            pages_and_contexts = await self._open_all_tradingview_pages()

            if not pages_and_contexts:
                print("❌ 無法打開任何頁面")
                return []

            print("\n" + "=" * 60)
            print("✅ 所有 CAPTCHA 已通過！")
            print("🚀 階段 2: 開始批次抓取 TradingView 數據")
            print("=" * 60 + "\n")

            # 🔥 階段 2: 批次抓取數據
            result = []
            for i, (stock, page, context) in enumerate(pages_and_contexts):
                print(f"\n{'=' * 50}")
                print(f"抓取 {stock} 的 TradingView 數據 ({i + 1}/{len(pages_and_contexts)})")
                print(f"{'=' * 50}")

                try:
                    tradingview_data = await self._extract_tradingview_from_page(stock, page)
                    result.append({stock: tradingview_data})

                    if tradingview_data is not None:
                        print(f"✓ {stock}: 成功抓取 {tradingview_data.shape[1]} 年份數據")
                    else:
                        print(f"⚠️ {stock}: 無數據")
                except Exception as e:
                    print(f"❌ {stock} 抓取失敗: {e}")
                    result.append({stock: None})

                # 延遲（最後一個不延遲）
                if i < len(pages_and_contexts) - 1:
                    await asyncio.sleep(random.uniform(0.5, 1.5))

            # 🔥 關閉所有頁面和 context
            print("\n🧹 清理資源...")
            for stock, page, context in pages_and_contexts:
                try:
                    await context.close()
                except:
                    pass

            return result

        finally:
            # 🔥 恢復原始設定
            self.headless = original_headless
            await self.cleanup()

    async def _open_all_tradingview_pages(self):
        """打開所有股票的 TradingView 頁面 - 使用 Schwab API 的 exchangeName"""
        pages_and_contexts = []

        for i, stock in enumerate(self.stocks):
            print(f"\n{'=' * 50}")
            print(f"打開 {stock} 的 TradingView 頁面 ({i + 1}/{len(self.stocks)})")
            print(f"{'=' * 50}")

            try:
                # 創建新的 context
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                    locale='zh-TW',
                    timezone_id='Asia/Taipei',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    }
                )

                # 注入反偵測腳本
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-TW', 'zh', 'en-US', 'en']
                    });
                """)

                page = await context.new_page()

                # 🔥 移除 yfinance，改用 stock_exchanges
                exchange_name = self.stock_exchanges.get(stock, 'NYSE')  # 預設 NYSE

                stock_symbol = ''.join(['.' if char == '-' else char for char in stock]) if '-' in stock else stock

                # 🔥 直接使用 exchangeName
                URL = f'https://www.tradingview.com/symbols/{exchange_name}-{stock_symbol}/financials-earnings/?earnings-period=FY&revenues-period=FY'

                # 訪問頁面
                await asyncio.sleep(random.uniform(2, 4))
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)
                await asyncio.sleep(random.uniform(2, 3))

                # 🔥 檢查 CAPTCHA（無限等待）
                await self._wait_for_captcha_resolution(stock, page)

                # 保存頁面和 context
                pages_and_contexts.append((stock, page, context))
                print(f"✓ {stock} 頁面已就緒")

                # 每個頁面之間延遲
                if i < len(self.stocks) - 1:
                    await asyncio.sleep(random.uniform(1, 2))

            except Exception as e:
                print(f"❌ {stock} 頁面打開失敗: {e}")
                if context:
                    try:
                        await context.close()
                    except:
                        pass

        return pages_and_contexts

    async def _extract_tradingview_from_page(self, stock, page):
        """從已載入的頁面中提取 TradingView 數據"""
        try:
            # 模擬人類瀏覽行為（與 get_TradingView_html 相同的邏輯）
            scroll_positions = [200, 400, 600, 400, 200]
            for pos in scroll_positions:
                await page.evaluate(f'window.scrollTo(0, {pos})')
                await asyncio.sleep(random.uniform(0.3, 0.6))

            # 等待關鍵內容載入
            try:
                await page.wait_for_selector('h1', timeout=30000)
                await asyncio.sleep(3)
            except Exception as e:
                print(f"等待頁面載入時發生錯誤: {e}")

            # 獲取頁面內容
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')

            # === 以下是原有的解析邏輯 ===

            # 解析年份
            years = []
            year_elements = soup.find_all('div', class_='value-OxVAcLqi')
            for element in year_elements:
                text = element.get_text(strip=True)
                if text.isdigit() and len(text) == 4:
                    years.append(int(text))

            if not years:
                values_container = soup.find('div', class_='values-AtxjAQkN')
                if values_container:
                    year_divs = values_container.find_all('div', class_='value-OxVAcLqi')
                    for div in year_divs:
                        text = div.get_text(strip=True)
                        if text.isdigit() and len(text) == 4:
                            years.append(int(text))

            if not years:
                print(f"無法找到年份資料對於 {stock}")
                return None

            # 初始化資料字典
            data = {
                'Year': years,
                'Reported': [None] * len(years),
                'Estimate': [None] * len(years),
                'Surprise': [None] * len(years)
            }

            # 解析三種類型的資料
            data_types = ['Reported', 'Estimate', 'Surprise']

            for data_type in data_types:
                container = soup.find('div', {'data-name': data_type})
                if not container:
                    continue

                values_section = container.find('div', class_='values-C9MdAMrq')
                if not values_section:
                    continue

                value_containers = values_section.find_all('div', class_='container-OxVAcLqi')

                for i, value_container in enumerate(value_containers):
                    if i >= len(years):
                        break

                    lock_button = value_container.find('button', class_='lockButton-N_j3rnsK')
                    if lock_button:
                        continue

                    value_div = value_container.find('div', class_='value-OxVAcLqi')
                    if value_div:
                        value = value_div.get_text(strip=True)
                        if value == '—' or value == '-':
                            value = None
                        elif value.startswith('‪') and value.endswith('‬'):
                            value = value.strip('‪‬')
                        data[data_type][i] = value

            # 建立DataFrame
            df_original = pd.DataFrame(data)
            mask = df_original[['Reported', 'Estimate', 'Surprise']].notna().any(axis=1)
            df_filtered = df_original[mask].reset_index(drop=True)

            # 轉換成橫向格式
            if len(df_filtered) > 0:
                years_list = df_filtered['Year'].tolist()
                transposed_data = {
                    'Year': years_list,
                    'Reported': df_filtered['Reported'].tolist(),
                    'Estimate': df_filtered['Estimate'].tolist(),
                    'Surprise': df_filtered['Surprise'].tolist()
                }

                result_dict = {}
                for i, year in enumerate(years_list):
                    result_dict[str(year)] = [
                        transposed_data['Reported'][i],
                        transposed_data['Estimate'][i],
                        transposed_data['Surprise'][i]
                    ]

                df_final = pd.DataFrame(result_dict, index=['Reported', 'Estimate', 'Surprise'])
                print(f"成功解析 {stock} 的資料")
                return df_final
            else:
                return None

        except Exception as e:
            print(f"提取 TradingView 數據失敗: {e}")
            return None

    async def run_beta(self):
        """批次執行 Beta 值抓取 - 先集中處理 CAPTCHA，再批次爬蟲"""

        # 🔥 臨時保存原始 headless 設定
        original_headless = self.headless

        # 🔥 強制使用有頭模式（顯示瀏覽器）
        self.headless = False

        try:
            await self.setup_browser()

            print("\n" + "=" * 60)
            print("🚀 階段 1: 集中處理 CAPTCHA 驗證")
            print("⚠️  即將打開所有股票的頁面")
            print("⚠️  請依序完成所有 CAPTCHA 驗證")
            print("⚠️  完成所有驗證後，程式將自動開始抓取數據")
            print("=" * 60 + "\n")

            # 🔥 階段 1: 打開所有頁面並處理 CAPTCHA
            pages_and_contexts = await self._open_all_beta_pages()

            if not pages_and_contexts:
                print("❌ 無法打開任何頁面")
                return []

            print("\n" + "=" * 60)
            print("✅ 所有 CAPTCHA 已通過！")
            print("🚀 階段 2: 開始批次抓取 Beta 值")
            print("=" * 60 + "\n")

            # 🔥 階段 2: 批次抓取數據
            result = []
            for i, (stock, page, context) in enumerate(pages_and_contexts):
                print(f"\n{'=' * 50}")
                print(f"抓取 {stock} 的 Beta 值 ({i + 1}/{len(pages_and_contexts)})")
                print(f"{'=' * 50}")

                try:
                    beta_value = await self._extract_beta_from_page(stock, page)
                    result.append({stock: beta_value})
                    print(f"✓ {stock}: {beta_value}")
                except Exception as e:
                    print(f"❌ {stock} 抓取失敗: {e}")
                    result.append({stock: None})

                # 延遲（最後一個不延遲）
                if i < len(pages_and_contexts) - 1:
                    await asyncio.sleep(random.uniform(0.5, 1.5))

            # 🔥 關閉所有頁面和 context
            print("\n🧹 清理資源...")
            for stock, page, context in pages_and_contexts:
                try:
                    await context.close()
                except:
                    pass

            return result

        finally:
            # 🔥 恢復原始設定
            self.headless = original_headless
            await self.cleanup()

    async def _open_all_beta_pages(self):
        """打開所有股票的 Beta 頁面並等待 CAPTCHA 通過（無時間限制）"""
        pages_and_contexts = []

        for i, stock in enumerate(self.stocks):
            print(f"\n{'=' * 50}")
            print(f"打開 {stock} 的頁面 ({i + 1}/{len(self.stocks)})")
            print(f"{'=' * 50}")

            try:
                # 創建新的 context
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 960},
                    java_script_enabled=True,
                    locale='zh-TW',
                    timezone_id='Asia/Taipei',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    }
                )

                # 注入反偵測腳本
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-TW', 'zh', 'en-US', 'en']
                    });
                """)

                page = await context.new_page()

                # 🔥 移除 yfinance，改用 stock_exchanges
                exchange_name = self.stock_exchanges.get(stock, 'NYSE')  # 預設 NYSE

                stock_symbol = ''.join(['.' if char == '-' else char for char in stock]) if '-' in stock else stock

                # 🔥 直接使用 exchangeName
                URL = f'https://tw.tradingview.com/symbols/{exchange_name}-{stock_symbol}/'

                # 訪問頁面
                await asyncio.sleep(random.uniform(2, 4))
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)
                await asyncio.sleep(random.uniform(2, 3))

                # 🔥 檢查 CAPTCHA（無限等待）
                await self._wait_for_captcha_resolution(stock, page)

                # 保存頁面和 context
                pages_and_contexts.append((stock, page, context))
                print(f"✓ {stock} 頁面已就緒")

                # 每個頁面之間延遲
                if i < len(self.stocks) - 1:
                    await asyncio.sleep(random.uniform(1, 2))

            except Exception as e:
                print(f"❌ {stock} 頁面打開失敗: {e}")
                if context:
                    try:
                        await context.close()
                    except:
                        pass

        return pages_and_contexts

    async def _wait_for_captcha_resolution(self, stock, page):
        """等待 CAPTCHA 被解決（無時間限制）+ 強制置中 reCAPTCHA"""

        # 🔥 方案 1：注入 CSS 強制置中 reCAPTCHA 的所有元素
        try:
            await page.add_style_tag(content="""
                /* 置中 reCAPTCHA 的主容器 */
                .g-recaptcha {
                    display: flex !important;
                    justify-content: center !important;
                    align-items: center !important;
                    position: fixed !important;
                    top: 50% !important;
                    left: 50% !important;
                    transform: translate(-50%, -50%) !important;
                    z-index: 999999 !important;
                }

                /* 置中所有 reCAPTCHA 的 iframe */
                iframe[src*="recaptcha"],
                iframe[src*="google.com/recaptcha"],
                iframe[title*="reCAPTCHA"] {
                    position: fixed !important;
                    top: 50% !important;
                    left: 50% !important;
                    transform: translate(-50%, -50%) !important;
                    z-index: 999999 !important;
                }

                /* 隱藏背景的干擾元素 */
                .tv-captcha-page__message-wrap {
                    position: relative !important;
                }

                /* 確保表單不會影響 CAPTCHA 位置 */
                #frmCaptcha {
                    display: flex !important;
                    flex-direction: column !important;
                    align-items: center !important;
                    justify-content: center !important;
                }

                /* 隱藏或調整其他內容 */
                .tv-text h1,
                .tv-text p {
                    position: relative !important;
                    z-index: 1 !important;
                }
            """)
            print(f"   ✓ 已注入 reCAPTCHA 置中 CSS")
        except Exception as e:
            print(f"   ⚠️ 注入 CSS 失敗: {e}")

        # 🔥 方案 2：等待 reCAPTCHA 載入後，用 JavaScript 強制移動
        try:
            await page.evaluate("""
                async () => {
                    // 等待 reCAPTCHA iframe 完全載入
                    const waitForRecaptcha = () => {
                        return new Promise((resolve) => {
                            const checkInterval = setInterval(() => {
                                // 尋找所有 reCAPTCHA iframe
                                const recaptchaIframes = document.querySelectorAll('iframe[src*="recaptcha"]');

                                if (recaptchaIframes.length > 0) {
                                    clearInterval(checkInterval);
                                    resolve(recaptchaIframes);
                                }
                            }, 100);

                            // 10 秒後超時
                            setTimeout(() => {
                                clearInterval(checkInterval);
                                resolve([]);
                            }, 10000);
                        });
                    };

                    const iframes = await waitForRecaptcha();

                    if (iframes.length > 0) {
                        console.log('找到', iframes.length, '個 reCAPTCHA iframe');

                        iframes.forEach((iframe, index) => {
                            // 強制設定 iframe 位置
                            iframe.style.cssText = `
                                position: fixed !important;
                                top: 50% !important;
                                left: 50% !important;
                                transform: translate(-50%, -50%) !important;
                                z-index: ${999999 + index} !important;
                                margin: 0 !important;
                            `;

                            console.log('✓ iframe', index, '已置中');
                        });

                        // 也處理 .g-recaptcha 容器
                        const recaptchaDiv = document.querySelector('.g-recaptcha');
                        if (recaptchaDiv) {
                            recaptchaDiv.style.cssText = `
                                position: fixed !important;
                                top: 50% !important;
                                left: 50% !important;
                                transform: translate(-50%, -50%) !important;
                                z-index: 999998 !important;
                                display: flex !important;
                                justify-content: center !important;
                                align-items: center !important;
                            `;
                            console.log('✓ .g-recaptcha 容器已置中');
                        }

                        // 調整頁面背景，讓 CAPTCHA 更明顯
                        const messageWrap = document.querySelector('.tv-captcha-page__message-wrap');
                        if (messageWrap) {
                            messageWrap.style.opacity = '0.3';
                        }
                    }
                }
            """)
            print(f"   ✓ 已執行 reCAPTCHA 置中腳本")
        except Exception as e:
            print(f"   ⚠️ 執行置中腳本失敗: {e}")

        # 🔥 方案 3：持續監控並調整（防止 reCAPTCHA 重新載入後位置跑掉）
        try:
            await page.evaluate("""
                () => {
                    // 建立 MutationObserver 監控 DOM 變化
                    const observer = new MutationObserver(() => {
                        const iframes = document.querySelectorAll('iframe[src*="recaptcha"]');
                        iframes.forEach((iframe) => {
                            if (iframe.style.position !== 'fixed') {
                                iframe.style.cssText = `
                                    position: fixed !important;
                                    top: 50% !important;
                                    left: 50% !important;
                                    transform: translate(-50%, -50%) !important;
                                    z-index: 999999 !important;
                                `;
                            }
                        });
                    });

                    observer.observe(document.body, {
                        childList: true,
                        subtree: true
                    });

                    // 30 秒後停止監控
                    setTimeout(() => observer.disconnect(), 30000);
                }
            """)
            print(f"   ✓ 已啟動 reCAPTCHA 持續監控")
        except Exception as e:
            print(f"   ⚠️ 啟動監控失敗: {e}")

        # 等待一下讓腳本執行
        await asyncio.sleep(2)

        captcha_visible = await self._check_captcha_visible(page)

        if captcha_visible:
            print("\n" + "🔴" * 30)
            print(f"⚠️  {stock} 偵測到 CAPTCHA 驗證！")
            print("⚠️  reCAPTCHA 應該已經移到畫面正中間")
            print("⚠️  請手動完成驗證")
            print("⚠️  完成後將自動繼續...")
            print("🔴" * 30 + "\n")

            # 🔥 無限等待直到 CAPTCHA 消失
            check_count = 0
            while True:
                await asyncio.sleep(5)
                check_count += 1

                still_visible = await self._check_captcha_visible(page)

                if not still_visible:
                    print(f"✅ {stock} CAPTCHA 已通過！")
                    break

                # 每 20 秒提示一次
                if check_count % 4 == 0:
                    print(f"   {stock} 等待中... (已等待 {check_count * 5} 秒)")

                    # 🔥 每 20 秒重新檢查並調整位置（以防萬一）
                    try:
                        await page.evaluate("""
                            () => {
                                const iframes = document.querySelectorAll('iframe[src*="recaptcha"]');
                                iframes.forEach((iframe) => {
                                    iframe.style.cssText = `
                                        position: fixed !important;
                                        top: 50% !important;
                                        left: 50% !important;
                                        transform: translate(-50%, -50%) !important;
                                        z-index: 999999 !important;
                                    `;
                                });
                            }
                        """)
                    except:
                        pass

            # CAPTCHA 通過後額外等待
            await asyncio.sleep(random.uniform(2, 4))

    async def _check_captcha_visible(self, page):
        """檢查 CAPTCHA 是否可見"""
        try:
            # 檢查 iframe
            captcha_frame = await page.query_selector('iframe[src*="captcha"], iframe[title*="reCAPTCHA"]')
            if captcha_frame:
                is_visible = await captcha_frame.is_visible()
                if is_visible:
                    return True

            # 檢查其他元素
            captcha_element = await page.query_selector('[class*="captcha"], [id*="captcha"], .g-recaptcha')
            if captcha_element:
                is_visible = await captcha_element.is_visible()
                if is_visible:
                    return True

            return False
        except:
            return False

    async def _extract_beta_from_page(self, stock, page):
        """從已載入的頁面中提取 Beta 值"""
        try:
            # 模擬人類瀏覽行為
            scroll_positions = [300, 600, 400, 100, 0]
            for pos in scroll_positions:
                await page.evaluate(f'window.scrollTo(0, {pos})')
                await asyncio.sleep(random.uniform(0.3, 0.6))

            # 獲取內容
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')

            # 解析 Beta 值（使用原有邏輯）
            beta_section = None
            all_wrappers = soup.find_all('div', class_='wrapper-QCJM7wcY')

            for wrapper in all_wrappers:
                parent = wrapper.find_parent()
                if parent and 'beta' in parent.get_text().lower():
                    beta_section = wrapper
                    break

            if beta_section:
                value_div = beta_section.find('div', class_='value-QCJM7wcY')
                if value_div:
                    beta_text = value_div.get_text(strip=True)
                    try:
                        return float(beta_text)
                    except ValueError:
                        pass

            # 備用方案
            for wrapper in all_wrappers:
                value_div = wrapper.find('div', class_='value-QCJM7wcY')
                if value_div:
                    value_text = value_div.get_text(strip=True)
                    try:
                        value_float = float(value_text)
                        if 0.1 <= value_float <= 5.0:
                            nearby_text = wrapper.find_parent().get_text().lower()
                            if 'beta' in nearby_text:
                                return value_float
                    except ValueError:
                        continue

            return None

        except Exception as e:
            print(f"提取 Beta 值失敗: {e}")
            return None

    async def fetch_barchart_data(self, stock, semaphore):
        """抓取單一股票的數據（Barchart Volatility）"""
        async with semaphore:
            context = None  # 🔥 初始化
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                # 🔥 追蹤 context
                async with self.contexts_lock:
                    self.contexts.append(context)
                try:
                    page = await context.new_page()
                    html_content = await self.get_barchart_html(stock, page)
                    return {stock: html_content}
                finally:
                    await context.close()
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
            except Exception as e:
                # 確保 context 被關閉
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    # 🔥 移除追蹤
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {stock: {"error": str(e)}}

    async def get_barchart_html(self, stock, page, retries=3):
        """抓取特定股票的Barchart頁面並回傳完整HTML"""
        URL = f'https://www.barchart.com/stocks/quotes/{stock}/volatility-charts'
        attempt = 0

        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的Barchart頁面 (第 {attempt + 1} 次)...")

                await asyncio.sleep(random.uniform(2, 5))
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

                # 等待頁面載入
                await asyncio.sleep(3)

                # 獲取完整HTML內容
                content = await page.content()

                # print(f"✓ 成功獲取 {stock} 的HTML，長度: {len(content)}")
                bs = BeautifulSoup(content, 'html.parser')

                div = bs.find('div', {'class':'bc-datatable-toolbar bc-options-toolbar volatility'})
                # print(div)
                return div.text.replace('\xa0', ' ')
                # return content

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(random.uniform(5, 10))

        return None

    async def run_barchart(self):
        """執行Barchart數據抓取"""
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_barchart_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
            return result
        finally:
            await self.cleanup()

    # 在 StockScraper 類別中，加在 run_barchart() 方法之後

    async def fetch_earnings_date_data(self, stock, semaphore):
        """抓取單一股票的財報日期（earningshub）"""
        async with semaphore:
            context = None
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                    locale='zh-TW',
                    timezone_id='Asia/Taipei',
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    }
                )

                # 注入反偵測腳本
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });

                    window.chrome = {
                        runtime: {},
                        loadTimes: function() {},
                        csi: function() {},
                        app: {}
                    };

                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });

                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-TW', 'zh', 'en-US', 'en']
                    });
                """)

                async with self.contexts_lock:
                    self.contexts.append(context)

                try:
                    page = await context.new_page()
                    earnings_data = await self.get_earnings_date_earningshub(stock, page)
                    return {stock: earnings_data}
                finally:
                    await context.close()
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)

            except Exception as e:
                if context:
                    try:
                        await context.close()
                    except:
                        pass
                    async with self.contexts_lock:
                        if context in self.contexts:
                            self.contexts.remove(context)
                return {stock: None}

    async def get_earnings_date_earningshub(self, stock, page, retries=3):
        """
        從 earningshub.com 爬取財報日期 - 改進版

        策略：
        1. 找到所有包含 "Earnings" 的區塊
        2. 提取所有日期
        3. 過濾出未來日期
        4. 選擇最近的一個

        Returns:
            dict: {'earnings_date': '2026年2月19日 週四 上午5:00', 'status': 'ESTIMATE'}
            None: 找不到未來財報
        """
        from bs4 import BeautifulSoup
        import random
        from datetime import datetime
        import re

        # 股票代碼轉換
        original_stock = stock
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])
            print(f"   股票代碼轉換: {original_stock} → {stock}")

        URL = f'https://earningshub.com/quote/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                print(f"正在抓取 {original_stock} 的財報日期 (第 {attempt + 1} 次)...")

                # 隨機延遲
                await asyncio.sleep(random.uniform(2, 4))

                # 前往頁面
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

                # 模擬人類瀏覽行為
                await asyncio.sleep(random.uniform(1, 2))
                await page.evaluate('window.scrollTo(0, 200)')
                await asyncio.sleep(random.uniform(0.5, 1))

                # 等待關鍵元素載入
                try:
                    await page.wait_for_selector('div.MuiAlert-root', timeout=10000)
                    await asyncio.sleep(2)
                except Exception:
                    print(f"   等待元素超時，繼續嘗試解析...")

                # 獲取頁面內容
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # ===== 步驟 1: 找到所有 MuiAlert 區塊 =====
                all_alerts = soup.find_all('div', class_='MuiAlert-root')
                print(f"   找到 {len(all_alerts)} 個 Alert 區塊")

                all_earnings_data = []  # 儲存所有找到的財報資訊

                # ===== 步驟 2: 遍歷所有區塊，提取日期 =====
                for alert_index, alert in enumerate(all_alerts, 1):
                    alert_text = alert.get_text()

                    # 🔥 關鍵過濾：必須包含 "Earnings" 和季度標記
                    if 'Earnings' not in alert_text:
                        continue

                    has_quarter = any(q in alert_text for q in ['Q1 ', 'Q2 ', 'Q3 ', 'Q4 '])
                    if not has_quarter:
                        continue

                    print(f"   Alert {alert_index}: 找到 Earnings 區塊")

                    # 尋找日期 span
                    date_span = alert.find('span', class_='MuiTypography-caption')

                    if not date_span:
                        print(f"      ⚠️ 未找到日期 span")
                        continue

                    # 提取日期文字
                    date_text = date_span.get_text(strip=True)

                    # 移除標籤（ESTIMATE / CONFIRMED）
                    status = None
                    inner_box = date_span.find('span', class_='MuiBox-root')
                    if inner_box:
                        status = inner_box.get_text(strip=True)
                        date_text = date_text.replace(status, '').strip()

                    # 驗證日期格式（必須包含「年月日」）
                    if '年' not in date_text or '月' not in date_text or '日' not in date_text:
                        print(f"      ⚠️ 日期格式不正確: {date_text}")
                        continue

                    print(f"      ✓ 原始日期: {date_text}")
                    if status:
                        print(f"      ✓ 狀態: {status}")

                    # 解析日期
                    try:
                        parsed_date = self._parse_chinese_date(date_text)
                        print(f"      ✓ 解析後: {parsed_date}")

                        # 儲存資訊
                        all_earnings_data.append({
                            'date': parsed_date,
                            'date_text': date_text,
                            'status': status or 'CONFIRMED',
                            'alert_type': self._get_alert_color(alert),
                            'raw_text': alert_text[:100]  # 前 100 字符供調試
                        })

                    except Exception as parse_error:
                        print(f"      ❌ 日期解析失敗: {parse_error}")
                        continue

                # ===== 步驟 3: 過濾未來日期 =====
                if not all_earnings_data:
                    print(f"   ⚠️ 未找到任何有效的財報日期")
                    attempt += 1
                    if attempt >= retries:
                        return None
                    continue

                print(f"\n   📊 找到 {len(all_earnings_data)} 個財報日期：")
                for i, data in enumerate(all_earnings_data, 1):
                    print(f"      {i}. {data['date_text']} ({data['status']})")

                # 取得當前時間（台北時區）
                from datetime import timezone, timedelta
                taipei_tz = timezone(timedelta(hours=8))
                now = datetime.now(taipei_tz)

                # 過濾未來日期
                future_dates = [
                    d for d in all_earnings_data
                    if d['date'] > now
                ]

                print(f"\n   🔮 未來財報: {len(future_dates)} 個")

                if not future_dates:
                    print(f"   ⚠️ 沒有找到未來的財報日期")
                    return None

                # ===== 步驟 4: 選擇最近的未來日期 =====
                next_earnings = min(future_dates, key=lambda x: x['date'])

                print(f"\n   ✅ 最近的未來財報:")
                print(f"      日期: {next_earnings['date_text']}")
                print(f"      狀態: {next_earnings['status']}")
                print(f"      距今: {(next_earnings['date'] - now).days} 天")

                return {
                    'earnings_date': next_earnings['date_text'],
                    'status': next_earnings['status'],
                    'source': 'earningshub'
                }

            except Exception as e:
                attempt += 1
                print(f"   ❌ 第 {attempt} 次嘗試失敗: {e}")

                if attempt >= retries:
                    print(f"   ❌ {original_stock} 在 {retries} 次嘗試後仍無法獲取財報日期")
                    return None

                wait_time = random.uniform(5, 10)
                print(f"   等待 {wait_time:.1f} 秒後重試...")
                await asyncio.sleep(wait_time)

        return None

    def _parse_chinese_date(self, date_str):
        """
        解析中文日期格式

        範例：
        - "2026年2月19日 週四 上午5:00"
        - "2026年2月19日 週四 下午9:00"

        Returns:
            datetime: 帶時區的 datetime 物件（台北時區）
        """
        from datetime import datetime, timezone, timedelta
        import re

        # 正則表達式：2026年2月19日 週四 上午5:00
        pattern = r'(\d{4})年(\d{1,2})月(\d{1,2})日.*?(上午|下午)(\d{1,2}):(\d{2})'
        match = re.search(pattern, date_str)

        if not match:
            raise ValueError(f"無法解析日期格式: {date_str}")

        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        am_pm = match.group(4)
        hour = int(match.group(5))
        minute = int(match.group(6))

        # 轉換為 24 小時制
        if am_pm == '下午' and hour != 12:
            hour += 12
        elif am_pm == '上午' and hour == 12:
            hour = 0

        # 建立帶時區的 datetime（台北時區 UTC+8）
        taipei_tz = timezone(timedelta(hours=8))
        dt = datetime(year, month, day, hour, minute, tzinfo=taipei_tz)

        return dt

    def _get_alert_color(self, alert):
        """
        判斷 Alert 的顏色類型

        Returns:
            str: 'info' (藍色), 'warning' (黃色), 'error' (紅色)
        """
        class_str = alert.get('class', [])

        if 'MuiAlert-colorInfo' in class_str:
            return 'info'
        elif 'MuiAlert-colorWarning' in class_str:
            return 'warning'
        elif 'MuiAlert-colorError' in class_str:
            return 'error'
        else:
            return 'unknown'

    async def run_earnings_dates(self):
        """批次執行財報日期抓取"""
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_earnings_date_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
            return result
        finally:
            await self.cleanup()

    async def fetch_option_chain_data(self, stock, semaphore):
        """抓取單一股票的選擇權鏈數據"""
        async with semaphore:
            try:
                # 檢查 Schwab API 是否可用
                if not self.schwab_available:
                    return {stock: {"error": "Schwab API 配置未完整設定"}}

                # 使用 schwabdev 客戶端
                option_data = await asyncio.to_thread(
                    self._get_option_chain_sync, stock
                )
                return {stock: option_data}
            except Exception as e:
                return {stock: {"error": str(e)}}

    def _get_option_chain_sync(self, stock):
        """同步獲取選擇權鏈數據 - 使用重用的 Client"""

        # 🔥 確保 Client 已初始化
        if self.schwab_client is None:
            self.initialize_schwab_client()

        try:
            # 🔥 使用重用的 Client
            response = self.schwab_client.option_chains(stock)

            # 嘗試解析 JSON
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                response_text = response.text if hasattr(response, 'text') else str(response)
                raise ValueError(f"無法解析 API 回應: {response_text[:200]}")

            # 檢查是否有 Token 錯誤
            if isinstance(data, dict):
                if 'error' in data:
                    error_type = data.get('error', '')
                    error_desc = data.get('error_description', '')

                    if 'refresh_token_authentication_error' in error_desc or \
                            'refresh_token_authentication_error' in error_type or \
                            'unsupported_token_type' in error_type:

                        print(f"❌ Token 認證失敗: {error_desc}")
                        raise TokenExpiredException(
                            f"Refresh Token 已失效或過期\n"
                            f"錯誤類型: {error_type}\n"
                            f"錯誤描述: {error_desc}\n\n"
                            f"請重新啟動程式完成認證流程。"
                        )
                    else:
                        raise ValueError(f"API 錯誤: {error_type} - {error_desc}")

            return data

        except TokenExpiredException:
            raise

        except Exception as e:
            error_str = str(e).lower()
            if 'refresh_token' in error_str or ('token' in error_str and 'authentication' in error_str):
                raise TokenExpiredException(
                    f"Token 認證失敗: {str(e)}\n\n"
                    f"請重新啟動程式完成認證流程。"
                )
            else:
                raise e

    async def run_option_chains(self):
        """批次執行選擇權鏈抓取 - 使用 Schwab API（優化版）"""

        # 🔥 初始化 Client（只執行一次）
        try:
            self.initialize_schwab_client()
        except Exception as e:
            print(f"❌ Schwab Client 初始化失敗: {e}")
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent)

        try:
            tasks = [
                self.fetch_option_chain_data(stock, semaphore)
                for stock in self.stocks
            ]
            result = await asyncio.gather(*tasks)
            return result

        except Exception as e:
            print(f"❌ 選擇權鏈抓取失敗: {e}")
            return []
