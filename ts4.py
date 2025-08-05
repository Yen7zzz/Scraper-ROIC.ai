from playwright.sync_api import sync_playwright
import re

def fetch_wacc(url: str) -> str:
    with sync_playwright() as p:
        # 1. 啟動無頭 Chromium
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # 2. 前往目標頁，等待網路空閒（大概所有資料載完了）
        page.goto(url, wait_until="networkidle")

        # 3. 定位那段「The WACC of Apple Inc...」的文字
        #    我們猜它是在 <p> 裡，所以用 XPath 抓第一個符合的 <p>
        para = page.locator("xpath=//p[contains(text(), 'The WACC of Apple Inc')]").inner_text().strip()

        # 4. 用正規表達式抽出百分比（e.g. "9.0%")
        m = re.search(r"(\d+(\.\d+)?%)", para)
        wacc = m.group(1) if m else "未知"

        browser.close()
        return wacc

if __name__ == "__main__":
    url = "https://valueinvesting.io/AAPL/valuation/wacc"
    wacc = fetch_wacc(url)
    print(f"抓到的 Apple WACC：{wacc}")
