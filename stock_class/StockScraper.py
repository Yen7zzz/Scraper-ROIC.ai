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

class StockScraper:
    def __init__(self, stocks, headless=True, max_concurrent=1):
        """
        初始化爬蟲類別。
        stocks: 股票代碼的列表
        headless: 是否使用無頭模式
        max_concurrent: 同時執行的股票數量（控制併發數）
        """
        self.stocks = stocks
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

    async def fetch_summary_data(self, stock, semaphore):
        """抓取單一股票的數據（summary）。"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                try:
                    page_summary = await context.new_page()
                    summary = await asyncio.gather(self.get_summary(stock, page_summary))
                    return {stock: summary}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_summary(self, stock, page, retries=1):
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='load', timeout=50000)
                await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                content = await page.content()
                dfs = pd.read_html(StringIO(content))
                return dfs
            except Exception as e:
                attempt += 1
                if attempt == retries:
                    return f"Error for {stock}: {e}"

        return f"Failed to retrieve data for {stock}"

    async def run_summary(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_summary_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    async def fetch_financials_data(self, stock, semaphore):
        """抓取單一股票的數據（financials）。"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                try:
                    page_financials = await context.new_page()
                    financials = await asyncio.gather(self.get_financials(stock, page_financials))
                    return {stock: financials}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_financials(self, stock, page, retries=1):
        """抓取特定股票的財務資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/financials'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='load', timeout=50000)

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
            tasks = [self.fetch_financials_data(stock, semaphore) for stock in self.stocks]
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
                )
                try:
                    page_ratios = await context.new_page()
                    ratios = await asyncio.gather(self.get_ratios(stock, page_ratios))
                    print({stock: ratios})
                    return {stock: ratios}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_ratios(self, stock, page, retries=1):
        """抓取特定股票的比率資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/ratios'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(URL, wait_until='load', timeout=50000)

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
            tasks = [self.fetch_ratios_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    async def fetch_EPS_PE_MarketCap_data(self, stock, semaphore):
        """抓取單一股票的數據（EPS_PE_MarketCap）。"""
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                try:
                    page_EPS_PE_MarketCap = await context.new_page()
                    EPS_PE_MarketCap = await asyncio.gather(self.get_EPS_PE_MarketCap(stock, page_EPS_PE_MarketCap))
                    return {stock: EPS_PE_MarketCap}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_EPS_PE_MarketCap(self, stock, page, retries=3):
        url = f'https://www.roic.ai/quote/{stock}'
        attempt = 0
        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))
                await page.goto(url, wait_until='load', timeout=30000)
                await page.wait_for_selector('table', timeout=30000)
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                span_string = soup.find_all('span', attrs={'stock_class': 'flex text-sm uppercase text-muted-foreground'})
                span_int_value = soup.find_all('span', attrs={'stock_class': 'flex text-lg text-foreground'})

                dic_data = {
                    span_string[0].text: float(span_int_value[0].text),
                    span_string[1].text: float(span_int_value[1].text),
                    span_string[2].text: span_int_value[2].text,
                    span_string[3].text: span_int_value[3].text
                }
                return dic_data
            except Exception as e:
                return f'error message:{e}'

    async def run_EPS_PE_MarketCap(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        try:
            tasks = [self.fetch_EPS_PE_MarketCap_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

    async def EPS_Growth_Rate_and_write_to_excel(self, stock, excel_base64):
        """抓取EPS成長率並寫入Excel"""
        if '-' in stock:
            stock = ''.join(['.' if char == '-' else char for char in stock])

        async with aiohttp.ClientSession() as session:
            async with session.get(f'https://api.stockboss.io/api/symbol?symbol={stock}') as response:
                content = await response.text()
                dic = json.loads(content)
                # print(dic['symbol']['guru_summary']['summary']['summary']['company_data']['wacc'])
                # wacc = float(dic['symbol']['guru_summary']['summary']['summary']['company_data']['wacc'])/100
                l_eps_growth5y = []
                try:
                    EPS_Growth_Rate_3_Year = \
                        dic['symbol']['keyratio']['keyratio']['annuals']['3-Year EPS Growth Rate %'][-1]
                    EPS_Growth_Rate_5_Year = \
                        dic['symbol']['keyratio']['keyratio']['annuals']['5-Year EPS Growth Rate %'][-1]
                    EPS_Growth_Rate_10_Year = \
                        dic['symbol']['keyratio']['keyratio']['annuals']['10-Year EPS Growth Rate %'][-1]

                    EPS_Growth_Rate_3_Year = 0 if EPS_Growth_Rate_3_Year == '-' else EPS_Growth_Rate_3_Year
                    EPS_Growth_Rate_5_Year = 0 if EPS_Growth_Rate_5_Year == '-' else EPS_Growth_Rate_5_Year
                    EPS_Growth_Rate_10_Year = 0 if EPS_Growth_Rate_10_Year == '-' else EPS_Growth_Rate_10_Year

                    l_eps_growth5y = l_eps_growth5y + [EPS_Growth_Rate_3_Year, EPS_Growth_Rate_5_Year,
                                                       EPS_Growth_Rate_10_Year]

                except KeyError as e:
                    return f"EPS_Growth_Rate的dictionary錯誤：{stock}", excel_base64

                # 選擇成長率：如果最小值大於 0，則取最小值，否則取最大值
                selected_growth_rate = min(l_eps_growth5y) / 100 if min(l_eps_growth5y) > 0 else max(
                    l_eps_growth5y) / 100
                # print(selected_growth_rate)
                # print(wacc)
                # 寫入 Excel
                try:
                    excel_binary = base64.b64decode(excel_base64)
                    excel_buffer = io.BytesIO(excel_binary)
                    wb = load_workbook(excel_buffer)
                    ws = wb.worksheets[3]  # 假設需要寫入的工作表是第四個
                    ws['C3'] = selected_growth_rate
                    # ws['C6'] = wacc

                    output_buffer = io.BytesIO()
                    wb.save(output_buffer)
                    output_buffer.seek(0)
                    modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

                    return f"{stock}的EPS成長率及WACC成功寫入", modified_base64

                except Exception as e:
                    return f"寫入Excel時發生錯誤：{e}", excel_base64

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
        URL = f'https://seekingalpha.com/symbol/{stock}/growth'
        attempt = 0

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
        URL = f'https://www.gurufocus.com/term/wacc/{stock}'
        attempt = 0

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