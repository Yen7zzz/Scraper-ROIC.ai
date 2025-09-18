# 匯入您原本的模組
import base64
import io
from playwright.async_api import async_playwright
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
import asyncio
from concurrent.futures import ThreadPoolExecutor


class StockScraper:
    def __init__(self, stocks, headless=True, max_concurrent=3):
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
                "--disable-web-security",  # 新增
                "--disable-features=VizDisplayCompositor",  # 新增
            ],
        )

    async def fetch_seekingalpha_data(self, stock, semaphore):
        async with semaphore:
            try:
                context = await self.browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                    viewport={"width": 800, "height": 600},  # 增加視窗大小
                    java_script_enabled=True,  # 確保JavaScript啟用
                )
                try:
                    page_summary = await context.new_page()
                    summary = await self.get_seekingalpha_html(stock, page_summary)
                    return {stock: summary}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_seekingalpha_html(self, stock, page, retries=3):
        """抓取特定股票的摘要資料並回傳 DataFrame。"""
        URL = f'https://seekingalpha.com/symbol/{stock}/growth'
        attempt = 0

        while attempt < retries:
            try:
                print(f"正在嘗試抓取 {stock} 的資料 (第 {attempt + 1} 次)...")

                # 隨機等待時間
                await asyncio.sleep(random.uniform(2, 5))

                # 前往頁面
                await page.goto(URL, wait_until='networkidle', timeout=60000)

                # 等待頁面載入完成
                await page.wait_for_load_state('networkidle')

                # 嘗試等待表格出現 - 修改選擇器
                try:
                    # 等待表格數據載入
                    await page.wait_for_selector('table', timeout=30000)

                    # 額外等待確保數據完全載入
                    await asyncio.sleep(3)

                    # 嘗試滾動到表格位置
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight/2)')
                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"等待表格時發生錯誤: {e}")

                # 獲取頁面內容
                content = await page.content()

                # 使用BeautifulSoup直接解析目標表格
                soup = BeautifulSoup(content, 'html.parser')

                # 方法1: 尋找包含growth-rates的section
                target_table = None
                growth_section = soup.find('section', {'data-test-id': 'card-container-growth-rates'})

                if growth_section:
                    target_table = growth_section.find('table', {'data-test-id': 'table'})
                    if target_table:
                        print("在Growth Rates section中找到目標表格")

                if target_table:
                    print("開始解析目標表格...")

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

                    # 解析表格內容
                    tbody = target_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        # print(f"找到 {len(rows)} 個數據行")

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
                                print(f"找到關鍵行 {i + 1}: {row_data}")

                                # 根據你的例子：['Revenue', '16.10%', '25.24%', '27.89%', '18.73%']
                                # 提取5Y和10Y數據 (假設是倒數第二和最後一個)
                                if len(row_data) >= 5:
                                    result = {
                                        "5Y": row_data[-2],  # 倒數第二個 '27.89%'
                                        "10Y": row_data[-1]  # 最後一個 '18.73%'
                                    }
                                    print(f"提取結果: {result}")
                                    return result
                                else:
                                    return {"error": f"Revenue行數據不足: {row_data}"}

                    else:
                        print("未找到tbody")

                    return {"error": "未找到Revenue行"}
                else:
                    print("未找到包含Growth Rates的目標表格")
                    return {"error": "未找到目標表格"}

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(random.uniform(5, 10))  # 增加等待時間

        return {"error": f"Failed to retrieve data for {stock} after {retries} attempts"}

    async def cleanup(self):
        """清理資源。"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()


async def main():
    stocks = ['O']
    scraper = StockScraper(stocks, headless=False)  # 設為False以便調試

    try:
        await scraper.setup_browser()
        semaphore = asyncio.Semaphore(scraper.max_concurrent)
        result = await scraper.fetch_seekingalpha_data('O', semaphore)

        print("=" * 50)
        print("最終結果:")
        print(result)

    except Exception as e:
        print(f"執行錯誤: {e}")
    finally:
        await scraper.cleanup()


if __name__ == "__main__":
    asyncio.run(main())