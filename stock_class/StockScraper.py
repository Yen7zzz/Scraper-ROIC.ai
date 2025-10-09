# 在您的 stock_analyzer_gui.py 檔案最上方添加這段程式碼

import os
import sys


def setup_playwright_path():
    """設定 Playwright 瀏覽器路徑"""

    # 如果是打包後的執行檔
    if hasattr(sys, '_MEIPASS'):
        # PyInstaller 打包後的臨時資料夾
        base_path = sys._MEIPASS

        # 設定瀏覽器路徑到打包後的位置
        browser_path = os.path.join(base_path, 'ms-playwright')

        if os.path.exists(browser_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = browser_path
            print(f"設定瀏覽器路徑: {browser_path}")
        else:
            # 嘗試原始路徑
            original_path = r'C:\Users\2993\AppData\Local\ms-playwright'
            if os.path.exists(original_path):
                os.environ['PLAYWRIGHT_BROWSERS_PATH'] = original_path
                print(f"使用原始瀏覽器路徑: {original_path}")
    else:
        # 開發環境，使用原始路徑
        original_path = r'C:\Users\2993\AppData\Local\ms-playwright'
        if os.path.exists(original_path):
            os.environ['PLAYWRIGHT_BROWSERS_PATH'] = original_path


# 在導入 playwright 之前呼叫這個函數
setup_playwright_path()

# 然後才導入 playwright
from playwright.sync_api import sync_playwright

import asyncio
import base64
import io
from playwright.async_api import async_playwright
import pandas as pd
import random
from io import StringIO
from openpyxl import load_workbook
from bs4 import BeautifulSoup
import aiohttp
import json
import re
import yfinance as yf

class StockScraper:
    def __init__(self, stocks, headless=True, max_concurrent=5):
        """
        初始化爬蟲類別。
        stocks: 股票代碼的列表
        headless: 是否使用無頭模式
        max_concurrent: 同時執行的股票數量（控制併發數）
        """
        self.stocks = stocks.get('final_stocks')
        self.us_stocks = stocks.get('us_stocks')
        self.non_us_stocks = stocks.get('non_us_stocks')
        self.headless = headless
        self.max_concurrent = max_concurrent
        self.browser = None
        self.playwright = None

    async def setup_browser(self):
        """設定瀏覽器環境。"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-background-timer-throttling",
                "--disable-blink-features=AutomationControlled",  # 新增：隱藏自動化標記
                "--exclude-switches=enable-automation",  # 新增：移除automation開關
            ],
        )

    async def cleanup(self):
        """清理資源。"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    # async def fetch_summary_data(self, stock, semaphore):
    #     """抓取單一股票的數據（summary）。"""
    #     async with semaphore:
    #         try:
    #             context = await self.browser.new_context(
    #                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    #                 viewport={"width": 800, "height": 600},
    #             )
    #             try:
    #                 page_summary = await context.new_page()
    #                 summary = await asyncio.gather(self.get_summary(stock, page_summary))
    #                 return {stock: summary}
    #             finally:
    #                 await context.close()
    #         except Exception as e:
    #             return {"stock": stock, "error": str(e)}

    # async def get_summary(self, stock, page, retries=3):
    #     """抓取特定股票的摘要資料並回傳 DataFrame。"""
    #     URL = f'https://www.roic.ai/quote/{stock}'
    #     attempt = 0
    #
    #     while attempt < retries:
    #         try:
    #             await asyncio.sleep(random.uniform(1, 3))
    #             await page.goto(URL, wait_until='load', timeout=50000)
    #             await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
    #             content = await page.content()
    #             dfs = pd.read_html(StringIO(content))
    #             return dfs
    #         except Exception as e:
    #             attempt += 1
    #             if attempt == retries:
    #                 return f"Error for {stock}: {e}"
    #
    #     return f"Failed to retrieve data for {stock}"

    # async def run_summary(self):
    #     await self.setup_browser()
    #     semaphore = asyncio.Semaphore(self.max_concurrent)
    #     try:
    #         tasks = [self.fetch_summary_data(stock, semaphore) for stock in self.stocks]
    #         result = await asyncio.gather(*tasks)
    #     finally:
    #         await self.cleanup()
    #     return result

    async def fetch_financials_data(self, stock, semaphore):
        """抓取單一股票的數據（financials）。"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                    java_script_enabled=True
                )
                try:
                    page_financials = await context.new_page()
                    financials = await asyncio.gather(self.get_financials(stock, page_financials))
                    return {stock: financials}
                finally:
                    await context.close()
            except Exception as e:
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

                await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                content = await page.content()
                dfs = pd.read_html(StringIO(content))
                return dfs

                # 之前的邏輯
                # if await page.query_selector(
                #         'div.rounded-lg.bg-card.text-card-foreground.shadow-sm.mx-auto.flex.w-\\[500px\\].flex-col.items-center.border.drop-shadow-lg'):
                #     return f'{stock}是非美國企業，此頁面須付費！'
                # else:
                #     await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                #     content = await page.content()
                #     dfs = pd.read_html(StringIO(content))
                #     return dfs

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
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                    java_script_enabled=True
                )
                try:
                    page_ratios = await context.new_page()
                    ratios = await asyncio.gather(self.get_ratios(stock, page_ratios))
                    # print({stock: ratios})
                    return {stock: ratios}
                finally:
                    await context.close()
            except Exception as e:
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
                await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                content = await page.content()
                dfs = pd.read_html(StringIO(content))
                return dfs

                # 之前的邏輯
                # if await page.query_selector(
                #         'div.rounded-lg.bg-card.text-card-foreground.shadow-sm.mx-auto.flex.w-\\[500px\\].flex-col.items-center.border.drop-shadow-lg'):
                #     return f'{stock}是非美國企業，此頁面須付費！'
                # else:
                #     await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                #     content = await page.content()
                #     dfs = pd.read_html(StringIO(content))
                #     return dfs

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

    async def get_EPS_PE_MarketCap(self, stock, page, retries=3):
        """抓取特定股票的EPS/PE/MarketCap數據 - 更新版本適應新的HTML結構"""
        url = f'https://www.roic.ai/quote/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(url, wait_until='load', timeout=30000)

                # 等待關鍵指標容器載入
                await page.wait_for_selector('div[data-cy="company_header_ratios"]', timeout=30000)

                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # 方法1：使用新的data-cy屬性定位
                ratios_container = soup.find('div', {'data-cy': 'company_header_ratios'})

                if ratios_container:
                    print(f"找到 {stock} 的指標容器")

                    # 提取所有指標項目
                    metric_items = ratios_container.find_all('div', class_='shrink-0 flex-col')

                    if len(metric_items) >= 3:  # 至少需要EPS, P/E, Market Cap
                        dic_data = {}

                        for item in metric_items:
                            # 提取數值（大字）
                            value_span = item.find('span', class_='flex text-lg text-foreground')
                            # 提取標籤（小字）
                            label_span = item.find('span', class_='flex text-sm uppercase text-muted-foreground')

                            if value_span and label_span:
                                label = label_span.get_text(strip=True)
                                value_text = value_span.get_text(strip=True)

                                # 根據標籤類型進行不同處理
                                if label in ['EPS', 'P/E']:
                                    try:
                                        dic_data[label] = float(value_text)
                                    except ValueError:
                                        dic_data[label] = value_text  # 如果無法轉換為數字，保持原字串
                                else:
                                    dic_data[label] = value_text  # Market Cap, Next Earn等保持字串

                        print(f"成功提取 {stock} 的指標數據: {dic_data}")
                        return dic_data

                    else:
                        print(f"指標項目數量不足: 找到 {len(metric_items)} 個項目")

                # 方法2：備用方案 - 使用類別選擇器
                if not ratios_container:
                    print(f"嘗試備用方案抓取 {stock} 的指標...")

                    # 直接尋找所有符合新結構的span元素
                    value_spans = soup.find_all('span', class_='flex text-lg text-foreground')
                    label_spans = soup.find_all('span', class_='flex text-sm uppercase text-muted-foreground')

                    if len(value_spans) >= 3 and len(label_spans) >= 3:
                        dic_data = {}

                        # 假設前幾個就是我們要的指標
                        for i in range(min(len(value_spans), len(label_spans))):
                            label = label_spans[i].get_text(strip=True)
                            value_text = value_spans[i].get_text(strip=True)

                            # 只處理我們關心的指標
                            if label in ['EPS', 'P/E', 'MARKET CAP', 'Market Cap', 'NEXT EARN', 'Next Earn']:
                                if label in ['EPS', 'P/E']:
                                    try:
                                        dic_data[label] = float(value_text)
                                    except ValueError:
                                        dic_data[label] = value_text
                                else:
                                    dic_data[label] = value_text

                        if dic_data:
                            print(f"備用方案成功提取 {stock} 的指標數據: {dic_data}")
                            return dic_data

                # 如果所有方法都失敗
                return {'error': f'無法找到 {stock} 的指標數據'}

            except Exception as e:
                attempt += 1
                print(f"第 {attempt} 次嘗試失敗: {e}")
                if attempt < retries:
                    await asyncio.sleep(random.uniform(2, 5))
                else:
                    return {'error': f'抓取 {stock} 數據時發生錯誤: {e}'}

        return {'error': f'Failed to retrieve data for {stock}'}

    async def fetch_combined_summary_and_metrics_data(self, stock, semaphore):
        """同時抓取Summary表格數據和EPS/PE/MarketCap指標數據"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
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
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_combined_data(self, stock, page, retries=3):
        """從同一頁面同時獲取Summary表格和指標數據 - 更新版本"""
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

                # 1. 使用 pandas 解析表格數據（Summary部分）
                summary_data = None
                try:
                    dfs = pd.read_html(StringIO(content))
                    summary_data = dfs
                    print(f"成功解析 {stock} 的表格數據，共 {len(dfs)} 個表格")
                except Exception as e:
                    print(f"解析 {stock} 表格數據失敗: {e}")
                    summary_data = []

                # 2. 使用 BeautifulSoup 解析指標數據（更新版本）
                metrics_data = None
                try:
                    soup = BeautifulSoup(content, 'html.parser')

                    # 使用新的選擇器
                    ratios_container = soup.find('div', {'data-cy': 'company_header_ratios'})

                    if ratios_container:
                        metric_items = ratios_container.find_all('div', class_='shrink-0 flex-col')

                        if len(metric_items) >= 3:
                            metrics_data = {}

                            for item in metric_items:
                                value_span = item.find('span', class_='flex text-lg text-foreground')
                                label_span = item.find('span', class_='flex text-sm uppercase text-muted-foreground')

                                if value_span and label_span:
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
                        print(f"未找到 {stock} 的指標容器")
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
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        URL = f'https://seekingalpha.com/symbol/{stock}/growth'
        attempt = 0
        # print(URL)
        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的資料 (第 {attempt + 1} 次)...")

                # 隨機等待時間
                await asyncio.sleep(random.uniform(2, 5))

                # 前往頁面 - 改用 domcontentloaded
                await page.goto(URL, wait_until='domcontentloaded', timeout=60000)

                # # === 極簡版模擬人類行為 ===
                # # 最短停頓
                # await asyncio.sleep(random.uniform(0.1, 0.4))
                #
                # # 只移動一次滑鼠
                # await page.mouse.move(
                #     random.randint(300, 500),
                #     random.randint(200, 300)
                # )
                #
                # # 極短等待
                # await asyncio.sleep(random.uniform(0.1, 0.3))
                # # === 極簡版結束 ===

                # 使用更精確的選擇器組合
                try:
                    # 先等待特定的 Growth Rates section
                    await page.wait_for_selector('section[data-test-id="card-container-growth-rates"]', timeout=15000)

                    # 再等待該 section 內的表格
                    await page.wait_for_selector(
                        'section[data-test-id="card-container-growth-rates"] table[data-test-id="table"]',
                        timeout=10000)

                    # 等待 Revenue 行出現（確保內容已載入）
                    await page.wait_for_selector(
                        'section[data-test-id="card-container-growth-rates"] th:has-text("Revenue")', timeout=10000)

                    # 短暫等待確保數據渲染完成
                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"等待關鍵元素時發生錯誤: {e}")
                    raise e

                # 獲取頁面內容
                content = await page.content()

                # 使用BeautifulSoup解析目標表格
                soup = BeautifulSoup(content, 'html.parser')

                # 先找到 Growth Rates section，再在其內找表格
                growth_section = soup.find('section', {'data-test-id': 'card-container-growth-rates'})

                if not growth_section:
                    print("未找到 Growth Rates section")
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

                    # 驗證表頭結構是否正確
                    expected_headers = ['YoY', '3Y', '5Y', '10Y']
                    if not all(h in headers for h in expected_headers):
                        print(f"表頭結構不符合預期，期望包含: {expected_headers}")
                        raise Exception("表頭結構不正確")

                    # 找到 5Y 和 10Y 在表頭中的位置
                    try:
                        header_5y_index = headers.index('5Y')
                        header_10y_index = headers.index('10Y')
                        print(f"5Y位置: {header_5y_index}, 10Y位置: {header_10y_index}")
                    except ValueError as e:
                        print(f"找不到5Y或10Y表頭: {e}")
                        raise Exception("找不到5Y或10Y表頭")

                    # 解析表格內容
                    tbody = target_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')

                        # 只處理Revenue行並返回5Y和10Y數據
                        for i, row in enumerate(rows):
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

                                # 根據表頭位置精確提取5Y和10Y數據
                                if len(row_data) > max(header_5y_index, header_10y_index):
                                    result = {
                                        "5Y": row_data[header_5y_index],  # 直接用表頭位置
                                        "10Y": row_data[header_10y_index]  # 直接用表頭位置
                                    }
                                    print(f"提取結果: {result}")
                                    return result
                                else:
                                    return {"error": f"Revenue行數據不足: {row_data}"}

                        return {"error": "未找到Revenue行"}
                    else:
                        print("未找到tbody")
                        return {"error": "未找到tbody"}

                else:
                    print("未找到Growth Rates表格")
                    return {"error": "未找到Growth Rates表格"}

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(random.uniform(10, 20))

        return {"error": f"Failed to retrieve data for {stock} after {retries} attempts"}

    async def run_seekingalpha(self):
        await self.setup_browser()

        # 創建一個持久的context
        context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            viewport={"width": 800, "height": 600},
            java_script_enabled=True,
        )

        try:
            page = await context.new_page()
            result = []

            # 依序處理每個股票
            for stock in self.stocks:
                print(f"正在處理 {stock}...")
                stock_data = await self.get_seekingalpha_html(stock, page)
                result.append({stock: stock_data})

                # 每個股票之間的延遲
                await asyncio.sleep(random.uniform(10, 20))

        finally:
            await context.close()
            await self.cleanup()

        return result

    async def fetch_wacc_data(self, stock, semaphore):
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                try:
                    page_summary = await context.new_page()
                    wacc_value = await self.get_wacc_html(stock, page_summary)
                    return {stock: wacc_value}
                finally:
                    await context.close()
            except Exception as e:
                return {stock: None}  # 如果出錯返回None

    async def get_wacc_html(self, stock, page, retries=3):
        """抓取特定股票的WACC資料並回傳int數值。"""
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        URL = f'https://www.gurufocus.com/term/wacc/{stock}'
        attempt = 0
        # print(URL)
        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的WACC資料 (第 {attempt + 1} 次)...")

                # 隨機等待時間
                await asyncio.sleep(random.uniform(2, 5))

                # 前往頁面
                await page.goto(URL, wait_until='networkidle', timeout=60000)

                # 等待頁面載入完成
                await page.wait_for_load_state('networkidle')

                # 等待關鍵內容載入
                try:
                    await page.wait_for_selector('h1', timeout=30000)
                    await asyncio.sleep(3)
                except Exception as e:
                    print(f"等待頁面載入時發生錯誤: {e}")

                # 獲取頁面內容
                content = await page.content()

                # 使用BeautifulSoup解析WACC數值
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
                            wacc_value = float(match.group(1))/100
                            # print(f"找到WACC值: {wacc_value}%")
                            break

                if wacc_value is not None:
                    return wacc_value
                else:
                    print(f"未能找到 {stock} 的WACC數值")
                    return None

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(random.uniform(5, 10))

        print(f"Failed to retrieve WACC data for {stock} after {retries} attempts")
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
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                try:
                    page_summary = await context.new_page()
                    wacc_value = await self.get_TradingView_html(stock, page_summary)
                    return {stock: wacc_value}
                finally:
                    await context.close()
            except Exception as e:
                return {stock: None}  # 如果出錯返回None

    async def get_TradingView_html(self, stock, page, retries=3):
        """抓取特定股票的trading-view資料並處理網址證券交易所問題。"""
        url_stock_exchange = yf.Ticker(stock).info.get('fullExchangeName', None)
        if url_stock_exchange in ['NasdaqGS', 'NasdaqGM', 'NasdaqCM']:
            url_stock_exchange = 'NASDAQ'

        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        URL = f'https://www.tradingview.com/symbols/{url_stock_exchange}-{stock}/financials-earnings/?earnings-period=FY&revenues-period=FY'
        attempt = 0
        # print(URL)
        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的trading-view資料 (第 {attempt + 1} 次)...")

                # 隨機等待時間
                await asyncio.sleep(random.uniform(2, 5))

                # 前往頁面
                await page.goto(URL, wait_until='networkidle', timeout=60000)

                # 等待頁面載入完成
                await page.wait_for_load_state('networkidle')

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

                # 解析年份 - 找到標題行中的年份
                years = []
                year_elements = soup.find_all('div', class_='value-OxVAcLqi')
                for element in year_elements:
                    text = element.get_text(strip=True)
                    if text.isdigit() and len(text) == 4:  # 年份是4位數字
                        years.append(int(text))

                # 如果沒找到年份，嘗試另一種方式
                if not years:
                    # 查找包含年份的容器
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
                    # 找到對應的資料容器
                    container = soup.find('div', {'data-name': data_type})
                    if not container:
                        print(f"找不到 {data_type} 資料容器")
                        continue

                    # 找到該容器中的數值區域
                    values_section = container.find('div', class_='values-C9MdAMrq')
                    if not values_section:
                        print(f"找不到 {data_type} 的數值區域")
                        continue

                    # 獲取所有數值容器
                    value_containers = values_section.find_all('div', class_='container-OxVAcLqi')

                    for i, value_container in enumerate(value_containers):
                        if i >= len(years):  # 防止索引超出範圍
                            break

                        # 檢查是否為鎖定資料（跳過付費內容）
                        lock_button = value_container.find('button', class_='lockButton-N_j3rnsK')
                        if lock_button:
                            continue  # 跳過鎖定的資料

                        # 提取數值
                        value_div = value_container.find('div', class_='value-OxVAcLqi')
                        if value_div:
                            value = value_div.get_text(strip=True)
                            # 處理特殊符號
                            if value == '—' or value == '-':
                                value = None
                            elif value.startswith('‪') and value.endswith('‬'):
                                # 移除Unicode控制字符
                                value = value.strip('‪‬')

                            # 儲存數值
                            data[data_type][i] = value

                # 建立DataFrame（原始格式）
                df_original = pd.DataFrame(data)

                # 只保留有資料的行（至少有一個非None值）
                mask = df_original[['Reported', 'Estimate', 'Surprise']].notna().any(axis=1)
                df_filtered = df_original[mask].reset_index(drop=True)

                # 轉換成橫向格式
                if len(df_filtered) > 0:
                    # 創建新的DataFrame，年份作為列標題
                    years_list = df_filtered['Year'].tolist()

                    # 創建橫向格式的數據
                    transposed_data = {
                        'Year': years_list,
                        'Reported': df_filtered['Reported'].tolist(),
                        'Estimate': df_filtered['Estimate'].tolist(),
                        'Surprise': df_filtered['Surprise'].tolist()
                    }

                    # 轉置數據：年份作為列標題，指標作為行
                    result_dict = {}

                    # 第一行：年份
                    for i, year in enumerate(years_list):
                        result_dict[str(year)] = [
                            transposed_data['Reported'][i],
                            transposed_data['Estimate'][i],
                            transposed_data['Surprise'][i]
                        ]

                    # 創建最終的DataFrame，以指標名稱作為索引
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
                    await asyncio.sleep(random.uniform(5, 10))

        print(f"Failed to retrieve TradingView data for {stock} after {retries} attempts")
        return None

    async def run_TradingView(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_TradingView_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    async def fetch_barchart_data(self, stock, semaphore):
        """抓取單一股票的數據（Barchart Volatility）"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                try:
                    page = await context.new_page()
                    html_content = await self.get_barchart_html(stock, page)
                    return {stock: html_content}
                finally:
                    await context.close()
            except Exception as e:
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
        finally:
            await self.cleanup()
        return result

    async def fetch_option_chain_data(self, stock, semaphore):
        """抓取單一股票的選擇權鏈數據"""
        async with semaphore:
            try:
                # 使用 schwabdev 客戶端
                option_data = await asyncio.to_thread(
                    self._get_option_chain_sync, stock
                )
                return {stock: option_data}
            except Exception as e:
                return {stock: {"error": str(e)}}

    def _get_option_chain_sync(self, stock):
        """同步獲取選擇權鏈數據"""
        import os
        import dotenv
        import schwabdev

        # 載入環境變數
        dotenv.load_dotenv()

        # 創建客戶端
        client = schwabdev.Client(
            os.getenv('app_key'),
            os.getenv('app_secret'),
            os.getenv('callback_url')
        )

        # 獲取選擇權數據
        response = client.option_chains(stock)
        return response.json()

    async def run_option_chains(self):
        """批次執行選擇權鏈抓取"""
        await self.setup_browser()  # 如果需要的話
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [
                self.fetch_option_chain_data(stock, semaphore)
                for stock in self.stocks
            ]
            result = await asyncio.gather(*tasks)
        finally:
            pass  # 選擇權API不需要清理瀏覽器
        return result