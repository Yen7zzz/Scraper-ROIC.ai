import time
from playwright.async_api import async_playwright
import asyncio
import pandas as pd
import random
from io import StringIO
import re
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
from bs4 import BeautifulSoup
import yfinance as yf
import aiohttp
import json
class StockScraper:
    def __init__(self, stocks, template_path, headless=True, max_concurrent=3):
        """
        初始化爬蟲類別。
        stocks: 股票代碼的列表
        headless: 是否使用無頭模式
        max_concurrent: 同時執行的股票數量（控制併發數）
        """
        self.stocks = stocks
        self.template_path = template_path
        self.headless = headless
        self.max_concurrent = max_concurrent  # 限制同時執行的併發數量
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
            ],
        )

    async def cleanup(self):
        """清理資源。"""
        if self.browser:
            await self.browser.close()  # 確保瀏覽器關閉
        if self.playwright:
            await self.playwright.stop()  # 停止 Playwright

    async def get_summary(self, stock, page, retries=1):
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))  # 模擬人工操作
                '''
                依狀況選擇
                'load'：等到整個頁面載入完成（包括圖片、JS等）。
                'domcontentloaded'：只等到 HTML 的基本結構載入完成。
                'networkidle'：等到沒有額外的網路請求在進行時。
                'timeout'：等多久（以毫秒為單位）才判定超時，預設是 30 秒（30000 毫秒）
                '''
                await page.goto(URL, wait_until='load', timeout=50000)
                # await page.wait_for_selector('table[style="width: 1199.8px;"]', timeout=50000)
                await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
                content = await page.content()

                dfs = pd.read_html(StringIO(content))
                desired_df = [df for df in dfs if df.columns[0] == 'Customize view']
                # 抓取最上方 EPS_PE_MarketCap
                soup = BeautifulSoup(content, 'html.parser')
                span_string = soup.find_all('span', attrs={'class': 'flex text-sm uppercase text-muted-foreground'})
                span_int_value = soup.find_all('span', attrs={'class': 'flex text-lg text-foreground'})

                dic_data = {span_string[0].text: float(span_int_value[0].text),
                            span_string[1].text: float(span_int_value[1].text),
                            span_string[2].text: span_int_value[2].text, span_string[3].text: span_int_value[3].text}
                return *desired_df, dic_data

            except Exception as e:
                attempt += 1
                if attempt == retries:
                    return f"Error for {stock}: {e}"

        return f"Failed to retrieve data for {stock}"

    async def get_financials(self, stock, page, retries=1):
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/financials'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))  # 模擬人工操作
                '''
                依狀況選擇
                'load'：等到整個頁面載入完成（包括圖片、JS等）。
                'domcontentloaded'：只等到 HTML 的基本結構載入完成。
                'networkidle'：等到沒有額外的網路請求在進行時。
                'timeout'：等多久（以毫秒為單位）才判定超時，預設是 30 秒（30000 毫秒）
                '''
                await page.goto(URL, wait_until='load', timeout=50000)
                # await page.wait_for_selector('table[style="width: 1199.8px;"]', timeout=50000)
                # await page.wait_for_selector('table.w-full.caption-bottom.text-sm.table-fixed', timeout=100000)
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

    async def get_ratios(self, stock, page, retries=1):
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        URL = f'https://www.roic.ai/quote/{stock}/ratios'
        attempt = 0

        while attempt < retries:
            try:
                await asyncio.sleep(random.uniform(1, 3))  # 模擬人工操作
                '''
                依狀況選擇
                'load'：等到整個頁面載入完成（包括圖片、JS等）。
                'domcontentloaded'：只等到 HTML 的基本結構載入完成。
                'networkidle'：等到沒有額外的網路請求在進行時。
                'timeout'：等多久（以毫秒為單位）才判定超時，預設是 30 秒（30000 毫秒）
                '''
                await page.goto(URL, wait_until='load', timeout=50000)
                # await page.wait_for_selector('table[style="width: 1199.8px;"]', timeout=50000)
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

    async def fetch_SFR_data(self, stock, semaphore):
        async with semaphore:  # 控制併發數量
            try:
                # 建立新的瀏覽器上下文
                context_summary = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                context_financial = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                context_ratios = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},
                )
                try:
                    # 為每個數據抓取建立頁面
                    page_summary = await context_summary.new_page()
                    page_financial = await context_financial.new_page()
                    page_ratios = await context_ratios.new_page()
                    # 使用 asyncio.gather 同時執行抓取任務
                    summary = await asyncio.gather(self.get_summary(stock, page_summary))
                    financial = await asyncio.gather(self.get_financials(stock, page_financial))
                    ratios = await asyncio.gather(self.get_ratios(stock, page_ratios))
                    return {'summary':summary, 'financial':financial, 'ratios':ratios}
                    # return {'summary': summary}

                finally:
                    await context_summary.close()
                    await context_financial.close()
                    await context_ratios.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def run_SFR_data(self):
        await self.setup_browser()
        semaphore = asyncio.Semaphore(self.max_concurrent)  # 限制同時執行的任務數量
        try:
            tasks = [self.fetch_SFR_data(stock, semaphore) for stock in self.stocks]
            result = await asyncio.gather(*tasks)
        finally:
            await self.cleanup()
        return result

if __name__ == '__main__':
    start = time.time()
    stocks = ['NVDA']
    # stocks = ['MSFT', 'DELL', 'NVDA', 'META', 'PLTR', 'AAPL', 'MMM', 'TSLA', 'AMAT', 'GOOG', 'AMD', 'ADBE', 'SBUX', 'LOW', 'V',
    #  'MA', 'MU', 'ORCL']
    template_path = r'C:\tmp\stock_template'
    scraper = StockScraper(stocks=stocks, template_path=template_path)
    data_list = asyncio.run(scraper.run_SFR_data())
    for x in data_list:
        print(x)
    print(time.time()-start)
