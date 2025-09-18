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
    def __init__(self, stocks, headless=False, max_concurrent=3):
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
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
            ],
        )

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
                    summary = await self.get_wacc_html(stock, page_summary)
                    return {stock: summary}
                finally:
                    await context.close()
            except Exception as e:
                return {"stock": stock, "error": str(e)}

    async def get_wacc_html(self, stock, page, retries=3):
        """抓取特定股票的WACC資料並回傳數值。"""
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

                # 儲存HTML以供調試
                with open(f'{stock}_wacc_debug.html', 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"已儲存 {stock} 的HTML內容到 {stock}_wacc_debug.html")

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
                            wacc_value = float(match.group(1))
                            print(f"找到WACC值: {wacc_value}%")
                            break

                # 方法2: 如果方法1失敗，尋找h1標籤中的WACC信息
                if wacc_value is None:
                    h1_elements = soup.find_all('h1')
                    for h1 in h1_elements:
                        if 'WACC %' in h1.get_text():
                            # 在h1後尋找相關的數值
                            font = h1.find('font', style=True)
                            if font:
                                text = font.get_text(strip=True)
                                match = re.search(r':(\d+\.?\d*)%', text)
                                if match:
                                    wacc_value = float(match.group(1))
                                    print(f"從h1標籤找到WACC值: {wacc_value}%")
                                    break

                # 方法3: 廣泛搜索所有可能包含WACC數值的文字
                if wacc_value is None:
                    text_content = soup.get_text()
                    matches = re.findall(r'WACC.*?(\d+\.?\d*)%.*?\(As of', text_content)
                    if matches:
                        wacc_value = float(matches[0])
                        print(f"通過文字搜索找到WACC值: {wacc_value}%")

                if wacc_value is not None:
                    result_data = {
                        'stock': stock,
                        'wacc': wacc_value,
                        'wacc_percentage': f"{wacc_value}%",
                        'source': 'GuruFocus',
                        'timestamp': 'Sep. 17, 2025'
                    }
                    print(f"成功解析 {stock} 的WACC數據:")
                    print(f"WACC: {wacc_value}% (float型別)")
                    return result_data
                else:
                    print("未能找到WACC數值")
                    return f"無法解析WACC數值 for {stock}"

            except Exception as e:
                print(f"第 {attempt + 1} 次嘗試失敗: {e}")
                attempt += 1
                if attempt < retries:
                    await asyncio.sleep(random.uniform(5, 10))

        return f"Failed to retrieve WACC data for {stock} after {retries} attempts"

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
        result = await scraper.fetch_wacc_data('O', semaphore)

        print("=== 最終結果 ===")
        if 'O' in result:
            data = result['O']
            if isinstance(data, dict) and 'wacc' in data:
                print(f"股票: {data['stock']}")
                print(f"WACC: {data['wacc']} (型別: {type(data['wacc'])})")
                print(f"WACC百分比: {data['wacc_percentage']}")
                print(f"數據來源: {data['source']}")
            else:
                print(f"錯誤或無數據: {data}")
        else:
            print(f"結果: {result}")

    except Exception as e:
        print(f"執行錯誤: {e}")
    finally:
        await scraper.cleanup()


if __name__ == "__main__":
    asyncio.run(main())