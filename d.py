import pandas as pd
import requests
import time
from urllib.request import Request, urlopen
import urllib.request


def get_yahoo_options_with_headers(symbol):
    """
    使用自訂標頭獲取 Yahoo Finance 選擇權數據
    """
    url = f'https://finance.yahoo.com/quote/{symbol}/options/?straddle=true'

    # 設定 User-Agent 模擬瀏覽器
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }

    try:
        print(f"正在獲取 {symbol} 的選擇權數據...")

        # 創建請求
        req = Request(url, headers=headers)

        # 添加延遲避免被封鎖
        time.sleep(2)

        # 使用 pandas 讀取，並傳入自訂 headers
        df_list = pd.read_html(req)

        print(f"成功獲取到 {len(df_list)} 個表格")

        # 通常第一個表格是 Calls，第二個是 Puts
        if len(df_list) >= 2:
            calls_df = df_list[0]
            puts_df = df_list[1]

            calls_df['Type'] = 'Call'
            puts_df['Type'] = 'Put'

            # 合併數據
            combined_df = pd.concat([calls_df, puts_df], ignore_index=True)

            print(f"買權合約數: {len(calls_df)}")
            print(f"賣權合約數: {len(puts_df)}")
            print(f"總合約數: {len(combined_df)}")

            return combined_df
        else:
            print("獲取的表格數量不足")
            return df_list[0] if df_list else None

    except Exception as e:
        print(f"獲取數據失敗: {e}")
        return None


def get_yahoo_options_with_requests(symbol):
    """
    使用 requests 庫獲取數據（更靈活的方法）
    """
    url = f'https://finance.yahoo.com/quote/{symbol}/options/'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        print(f"使用 requests 獲取 {symbol} 選擇權數據...")

        # 創建 session 保持連接
        session = requests.Session()
        session.headers.update(headers)

        # 添加延遲
        time.sleep(3)

        response = session.get(url, timeout=30)
        response.raise_for_status()

        # 使用 pandas 解析 HTML 內容
        df_list = pd.read_html(response.content)

        print(f"成功獲取到 {len(df_list)} 個表格")

        if len(df_list) >= 2:
            calls_df = df_list[0]
            puts_df = df_list[1]

            calls_df['Type'] = 'Call'
            puts_df['Type'] = 'Put'

            combined_df = pd.concat([calls_df, puts_df], ignore_index=True)

            print(f"買權合約數: {len(calls_df)}")
            print(f"賣權合約數: {len(puts_df)}")

            return combined_df
        else:
            return df_list[0] if df_list else None

    except requests.RequestException as e:
        print(f"網路請求失敗: {e}")
        return None
    except Exception as e:
        print(f"解析數據失敗: {e}")
        return None


def retry_with_backoff(symbol, max_retries=3):
    """
    使用指數退避重試機制
    """
    for attempt in range(max_retries):
        try:
            wait_time = (2 ** attempt) * 2  # 2, 4, 8 秒
            if attempt > 0:
                print(f"第 {attempt + 1} 次嘗試，等待 {wait_time} 秒...")
                time.sleep(wait_time)

            # 首先嘗試 requests 方法
            result = get_yahoo_options_with_requests(symbol)
            if result is not None:
                return result

            # 如果失敗，嘗試 headers 方法
            result = get_yahoo_options_with_headers(symbol)
            if result is not None:
                return result

        except Exception as e:
            print(f"嘗試 {attempt + 1} 失敗: {e}")
            if attempt == max_retries - 1:
                print("所有嘗試都失敗了")
                return None

    return None


# 主程式
if __name__ == "__main__":
    symbol = "GRAB"  # 你要查詢的股票代號

    print("開始獲取選擇權數據...")
    print("注意：Yahoo Finance 有反爬蟲機制，可能需要等待...")

    # 使用重試機制獲取數據
    options_df = retry_with_backoff(symbol)

    if options_df is not None:
        print("\n=== 數據獲取成功 ===")
        print(f"總計 {len(options_df)} 個選擇權合約")

        # 顯示前幾行數據
        print("\n前10行數據:")
        print(options_df.head(10))

        # 保存數據
        filename = f"{symbol}_options_pandas.csv"
        options_df.to_csv(filename, index=False)
        print(f"\n數據已保存到: {filename}")

        # 基本統計
        if 'Type' in options_df.columns:
            print(f"\n統計:")
            print(options_df['Type'].value_counts())

    else:
        print("\n❌ 無法獲取數據")
        print("\n可能的原因:")
        print("1. Yahoo Finance 反爬蟲機制")
        print("2. 網路連接問題")
        print("3. 股票代號不存在選擇權")
        print("\n建議:")
        print("1. 等待幾分鐘後重試")
        print("2. 使用 VPN 更換 IP")
        print("3. 使用 yfinance 套件（推薦）")

        print("\n使用 yfinance 的替代方案:")
        print("pip install yfinance")
        print("import yfinance as yf")
        print(f"ticker = yf.Ticker('{symbol}')")
        print("options = ticker.option_chain()")