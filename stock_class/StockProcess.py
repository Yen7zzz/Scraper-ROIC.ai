import asyncio
import time
import base64
import io
import pandas as pd
import random
import re
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
import yfinance as yf
from excel_template.excel_template import EXCEL_TEMPLATE_BASE64

class StockProcess:
    def __init__(self, max_concurrent=2, request_delay=2.0):
        # 將 semaphore 移到類別層級，確保全域限制
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.request_delay = request_delay  # 請求之間的延遲（秒）
        self.last_request_time = {}  # 記錄每個API的上次請求時間

    def create_excel_from_base64(self, stock):
        """從base64模板創建Excel文件的base64"""
        try:
            if EXCEL_TEMPLATE_BASE64.strip() == "" or "請將您從轉換工具得到的" in EXCEL_TEMPLATE_BASE64:
                return "", "❌ 錯誤：請先設定 EXCEL_TEMPLATE_BASE64 變數"

            excel_binary = base64.b64decode(EXCEL_TEMPLATE_BASE64.strip())
            excel_buffer = io.BytesIO(excel_binary)
            workbook = load_workbook(excel_buffer)

            # 儲存修改後的檔案到記憶體
            output_buffer = io.BytesIO()
            workbook.save(output_buffer)
            output_buffer.seek(0)
            excel_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

            return excel_base64, f"成功為 {stock} 創建Excel檔案"

        except Exception as e:
            return "", f"創建Excel檔案時發生錯誤: {e}"

    # def write_wacc_to_excel_base64(self, stock_code: str, wacc_value: str, excel_base64: str) -> tuple:
    #     """將WACC值寫入Excel的base64"""
    #     try:
    #         excel_binary = base64.b64decode(excel_base64)
    #         excel_buffer = io.BytesIO(excel_binary)
    #         workbook = load_workbook(excel_buffer)
    #
    #         if "現金流折現法" in workbook.sheetnames:
    #             worksheet = workbook["現金流折現法"]
    #         else:
    #             worksheet = workbook.active
    #
    #         # 寫入 WACC 值到 C6 儲存格
    #         if wacc_value != "未知":
    #             if wacc_value.endswith('%'):
    #                 numeric_value = float(wacc_value.rstrip('%')) / 100
    #                 worksheet['C6'] = numeric_value
    #                 worksheet['C6'].number_format = '0.00%'
    #             else:
    #                 worksheet['C6'] = wacc_value
    #         else:
    #             worksheet['C6'] = "無法取得"
    #
    #         # 儲存修改後的檔案到記憶體
    #         output_buffer = io.BytesIO()
    #         workbook.save(output_buffer)
    #         output_buffer.seek(0)
    #         modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')
    #
    #         return modified_base64, f"已將 {stock_code} 的 WACC 值 ({wacc_value}) 寫入 C6 儲存格"
    #
    #     except Exception as e:
    #         return excel_base64, f"操作 Excel 檔案時發生錯誤: {e}"

    async def process_df_summary(self, raw_df_summary, stock, excel_base64):
        """處理summary數據並寫入Excel base64"""
        if raw_df_summary:
            try:
                excel_binary = base64.b64decode(excel_base64)
                excel_buffer = io.BytesIO(excel_binary)
                wb = load_workbook(excel_buffer)
                ws = wb.worksheets[0]

                # 清除舊資料
                for row in ws.iter_rows(min_row=1, min_col=1, max_row=30, max_col=12):
                    for cell in row:
                        cell.value = None

                # 修正：直接使用 raw_df_summary，因為它已經是該股票的數據
                # 原本的程式碼: d_1_raw_df_summary = [y for x in raw_df_summary.get(stock, pd.DataFrame({})) for y in x]
                # 新的程式碼：檢查 raw_df_summary 的類型
                if isinstance(raw_df_summary, list):
                    # 如果是 list，直接使用
                    d_1_raw_df_summary = raw_df_summary
                elif isinstance(raw_df_summary, dict) and stock in raw_df_summary:
                    # 如果是 dict，取出該股票的數據
                    stock_data = raw_df_summary[stock]
                    if isinstance(stock_data, list):
                        d_1_raw_df_summary = stock_data
                    else:
                        d_1_raw_df_summary = [stock_data] if stock_data is not None else []
                else:
                    # 其他情況，嘗試直接使用
                    d_1_raw_df_summary = [raw_df_summary] if raw_df_summary is not None else []

                for df_amount, df in enumerate(d_1_raw_df_summary):
                    df_column_list = df.columns.tolist()

                    # 篩選需要的年份
                    years_list = []
                    pattern = r'\d{4}\sY'
                    for years in df_column_list:
                        if re.match(pattern, years):
                            years_list.append(int(years.split()[0]))

                    if years_list:
                        end_year = max(years_list)
                        start_year = end_year - 10
                        drop_column = [
                            x for x in df_column_list
                            if not (re.match(pattern, x) and start_year <= int(x.split()[0]) <= end_year)
                        ]
                        drop_column.pop(0)  # 移除列名的第一欄
                        df = df.drop(columns=drop_column)

                        # 資料轉型為數值型
                        years_data = df.columns[1:]
                        df[years_data] = df[years_data].apply(pd.to_numeric, errors='coerce')

                        # *** 關鍵修改：反轉年份欄位的順序 ***
                        # 保留第一欄（通常是項目名稱），但反轉年份欄位
                        first_col = df.columns[0]  # 第一欄
                        year_cols = df.columns[1:]  # 年份欄位
                        reversed_year_cols = year_cols[::-1]  # 反轉年份欄位順序

                        # 重新組合欄位順序：第一欄 + 反轉的年份欄位
                        new_column_order = [first_col] + list(reversed_year_cols)
                        df_reordered = df[new_column_order]

                        # 將資料寫入 Excel，並設置欄位格式
                        start_row = 1

                        for r_idx, row in enumerate(dataframe_to_rows(df_reordered, index=False, header=True),
                                                    start=start_row):
                            row_data = list(row)

                            # 第一欄（項目名稱）寫入 A 欄
                            cell = ws.cell(row=r_idx, column=1, value=row_data[0])
                            cell.font = Font(name='新細明體', size=12, bold=(r_idx == start_row))

                            # 年份數據從右邊開始寫入（從 L 欄開始往左）
                            year_data = row_data[1:]  # 除了第一欄以外的年份數據

                            # L欄是第12欄，從L欄開始往左寫
                            for year_idx, value in enumerate(year_data):
                                column_position = 12 - year_idx  # L=12, K=11, J=10...
                                cell = ws.cell(row=r_idx, column=column_position, value=value)
                                cell.font = Font(name='新細明體', size=12, bold=(r_idx == start_row))

                        # 自動調整欄寬
                        for col in ws.columns:
                            max_length = max(len(str(cell.value or '')) for cell in col)
                            ws.column_dimensions[col[0].column_letter].width = max_length + 2

                # 儲存到base64
                output_buffer = io.BytesIO()
                wb.save(output_buffer)
                output_buffer.seek(0)
                modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

                return modified_base64, f"{stock}的Summary成功寫入"

            except Exception as e:
                return excel_base64, f"處理Summary資料時發生錯誤: {e}"
        else:
            return excel_base64, '無原始資料'

    async def process_df_financial(self, raw_df_financial, stock, excel_base64):
        """處理financial數據並寫入Excel base64"""
        if raw_df_financial:
            try:
                excel_binary = base64.b64decode(excel_base64)
                excel_buffer = io.BytesIO(excel_binary)
                wb = load_workbook(excel_buffer)
                ws = wb.worksheets[0]

                # 定義各類財務數據的起始位置
                starting_cell = [("IncomeStatement", 1, 14),  # N1
                                 ("BalanceSheet", 1, 27),  # AA1
                                 ("CashFlowStatement", 1, 40)]  # AN1

                # 清除舊資料
                for row in ws.iter_rows(min_row=1, min_col=14, max_row=100, max_col=25):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=27, max_row=100, max_col=38):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=40, max_row=100, max_col=51):
                    for cell in row:
                        cell.value = None

                if raw_df_financial.get(stock) == [f'{stock}是非美國企業，此頁面須付費！']:
                    return excel_base64, f'{stock}是非美國企業，此頁面須付費！'

                else:
                    d_1_raw_df_financial = [y for x in raw_df_financial.get(stock, pd.DataFrame({})) for y in x]

                    for df_amount, df in enumerate(d_1_raw_df_financial):
                        df_column_list = df.columns.tolist()

                        # 篩選需要的年份
                        years_list = []
                        pattern = r'\d{4}\sY'
                        for years in df_column_list:
                            if re.match(pattern, years):
                                years_list.append(int(years.split()[0]))

                        if years_list:
                            end_year = max(years_list)
                            start_year = end_year - 10
                            drop_column = [
                                x for x in df_column_list
                                if not (re.match(pattern, x) and start_year <= int(x.split()[0]) <= end_year)
                            ]
                            drop_column.pop(0)
                            df = df.drop(columns=drop_column)

                            # 資料轉型為數值型
                            years_data = df.columns[1:]
                            df[years_data] = df[years_data].apply(pd.to_numeric, errors='coerce')

                            # *** 關鍵修改：反轉年份欄位的順序 ***
                            first_col = df.columns[0]  # 第一欄
                            year_cols = df.columns[1:]  # 年份欄位
                            reversed_year_cols = year_cols[::-1]  # 反轉年份欄位順序

                            # 重新組合欄位順序：第一欄 + 反轉的年份欄位
                            new_column_order = [first_col] + list(reversed_year_cols)
                            df_reordered = df[new_column_order]

                            # 將表格資料寫入指定位置並調整格式
                            start_row = starting_cell[df_amount][1]
                            start_col = starting_cell[df_amount][2]

                            for r_idx, row in enumerate(dataframe_to_rows(df_reordered, index=False, header=True),
                                                        start=start_row):
                                row_data = list(row)

                                # 第一欄（項目名稱）寫入起始欄位
                                cell = ws.cell(row=r_idx, column=start_col, value=row_data[0])
                                cell.font = Font(size=12, bold=(r_idx == start_row))

                                # 年份數據從右邊開始寫入（從起始欄位+11開始往左）
                                year_data = row_data[1:]  # 除了第一欄以外的年份數據

                                # 從起始欄位+11開始往左寫（假設有12欄的空間）
                                for year_idx, value in enumerate(year_data):
                                    column_position = (start_col + 11) - year_idx  # 從右邊往左寫
                                    cell = ws.cell(row=r_idx, column=column_position, value=value)
                                    cell.font = Font(size=12, bold=(r_idx == start_row))

                            # 自動調整欄寬
                            for col in ws.columns:
                                max_length = max(len(str(cell.value or '')) for cell in col)
                                ws.column_dimensions[col[0].column_letter].width = max_length + 2

                # 儲存到base64
                output_buffer = io.BytesIO()
                wb.save(output_buffer)
                output_buffer.seek(0)
                modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

                return modified_base64, f"{stock}的Financial成功寫入"

            except Exception as e:
                return excel_base64, f"處理Financial資料時發生錯誤: {e}"
        else:
            return excel_base64, '無原始資料'

    async def process_df_ratios(self, raw_df_ratios, stock, excel_base64):
        """處理ratios數據並寫入Excel base64"""
        if raw_df_ratios:
            try:
                excel_binary = base64.b64decode(excel_base64)
                excel_buffer = io.BytesIO(excel_binary)
                wb = load_workbook(excel_buffer)
                ws = wb.worksheets[0]  # 使用第一個工作表

                # 定義各類財務數據的起始位置（對應A.py的7個類別）
                starting_cell = [
                    ('Profitability', 1, 53), ('Credit', 1, 66),
                    ('Liquidity', 1, 79), ('Working Capital', 1, 92),
                    ('Enterprise Value', 1, 105), ('Multiples', 1, 118),
                    ('Per Share Data Items', 1, 131)
                ]

                # 清除舊資料（對應A.py的7個區域）
                for row in ws.iter_rows(min_row=1, min_col=53, max_row=100, max_col=64):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=66, max_row=100, max_col=77):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=79, max_row=100, max_col=90):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=92, max_row=100, max_col=103):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=105, max_row=100, max_col=116):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=118, max_row=100, max_col=129):
                    for cell in row:
                        cell.value = None
                for row in ws.iter_rows(min_row=1, min_col=131, max_row=100, max_col=142):
                    for cell in row:
                        cell.value = None

                if raw_df_ratios.get(stock) == [f'{stock}是非美國企業，此頁面須付費！']:
                    return excel_base64, f'{stock}是非美國企業，此頁面須付費！'
                else:
                    d_1_raw_df_ratios = [y for x in raw_df_ratios.get(stock, []) for y in x]

                    for df_amount, df in enumerate(d_1_raw_df_ratios):
                        df_column_list = df.columns.tolist()

                        # 篩選需要的年份
                        years_list = []
                        pattern = r'\d{4}\sY'
                        for years in df_column_list:
                            if re.match(pattern, years):
                                years_list.append(int(years.split()[0]))

                        if years_list:
                            end_year = max(years_list)
                            start_year = end_year - 10
                            drop_column = [
                                x for x in df_column_list
                                if not (re.match(pattern, x) and start_year <= int(x.split()[0]) <= end_year)
                            ]
                            drop_column.pop(0)
                            df = df.drop(columns=drop_column)

                            # 資料轉型為數值型
                            years_data = df.columns[1:]
                            df[years_data] = df[years_data].apply(pd.to_numeric, errors='coerce')

                            # *** 關鍵修改：反轉年份欄位的順序 ***
                            first_col = df.columns[0]  # 第一欄
                            year_cols = df.columns[1:]  # 年份欄位
                            reversed_year_cols = year_cols[::-1]  # 反轉年份欄位順序

                            # 重新組合欄位順序：第一欄 + 反轉的年份欄位
                            new_column_order = [first_col] + list(reversed_year_cols)
                            df_reordered = df[new_column_order]

                            # 將表格資料寫入指定位置並調整格式
                            start_row = starting_cell[df_amount][1]
                            start_col = starting_cell[df_amount][2]

                            for r_idx, row in enumerate(dataframe_to_rows(df_reordered, index=False, header=True),
                                                        start=start_row):
                                row_data = list(row)

                                # 第一欄（項目名稱）寫入起始欄位
                                cell = ws.cell(row=r_idx, column=start_col, value=row_data[0])
                                cell.font = Font(size=12, bold=(r_idx == start_row))

                                # 年份數據從右邊開始寫入（從起始欄位+11開始往左）
                                year_data = row_data[1:]  # 除了第一欄以外的年份數據

                                # 從起始欄位+11開始往左寫（假設有12欄的空間）
                                for year_idx, value in enumerate(year_data):
                                    column_position = (start_col + 11) - year_idx  # 從右邊往左寫
                                    cell = ws.cell(row=r_idx, column=column_position, value=value)
                                    cell.font = Font(size=12, bold=(r_idx == start_row))

                            # 自動調整欄寬
                            for col in ws.columns:
                                max_length = max(len(str(cell.value or '')) for cell in col)
                                ws.column_dimensions[col[0].column_letter].width = max_length + 2

                # 儲存到base64
                output_buffer = io.BytesIO()
                wb.save(output_buffer)
                output_buffer.seek(0)
                modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

                return modified_base64, f"{stock}的Ratios成功寫入"

            except Exception as e:
                return excel_base64, f"處理Ratios資料時發生錯誤: {e}"
        else:
            return excel_base64, '無原始資料'

    async def EPS_PE_MarketCap_data_write_to_excel(self, EPS_PE_MarketCap_content, stock, excel_base64):
        """將 EPS_PE_MarketCap 數據寫入 Excel base64 - 完整格式化處理"""
        try:
            excel_binary = base64.b64decode(excel_base64)
            excel_buffer = io.BytesIO(excel_binary)
            wb = load_workbook(excel_buffer)
            ws = wb.worksheets[0]

            # 清除舊資料 - 清空 EN1 到 EO1
            for i in range(1, 5):
                ws[f'EN{i}'].value = None
                ws[f'EO{i}'].value = None

            # 處理資料
            for data in EPS_PE_MarketCap_content.get(stock, {}):
                start_cell = "EN1"
                start_row = int(start_cell[2:])  # 提取行號，例如 "1"

                for i, (key, value) in enumerate(data.items()):
                    row = start_row + i  # 從起始行開始逐行寫入

                    # 寫入鍵到 EY 列
                    key_cell = ws[f"EN{row}"]
                    key_cell.value = key
                    key_cell.font = Font(size=12, bold=False)  # 統一字體設定

                    # 寫入值到 EZ 列
                    value_cell = ws[f"EO{row}"]
                    value_cell.value = value
                    value_cell.font = Font(size=12, bold=False)  # 統一字體設定

                # 自動調整欄寬
                for col in ws.columns:
                    max_length = max(len(str(cell.value or '')) for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = max_length + 2

            # 儲存到 base64
            output_buffer = io.BytesIO()
            wb.save(output_buffer)
            output_buffer.seek(0)
            modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

            return modified_base64, f'{stock}的EPS_PE_MarketCap成功寫入並儲存成功'

        except Exception as e:
            return excel_base64, f"處理 EPS_PE_MarketCap 時發生錯誤: {e}"

    async def _rate_limit(self, api_key="yfinance"):
        """實施速率限制"""
        current_time = time.time()

        if api_key not in self.last_request_time:
            self.last_request_time[api_key] = 0

        time_since_last_request = current_time - self.last_request_time[api_key]

        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            # 添加隨機延遲，避免所有請求同時發送
            sleep_time += random.uniform(0.5, 1.5)
            print(f"⏳ 等待 {sleep_time:.1f} 秒以避免API限制...")
            await asyncio.sleep(sleep_time)

        self.last_request_time[api_key] = time.time()

    async def _fetch_stock_data_with_retry(self, stock, max_retries=3):
        """帶重試機制的數據獲取"""
        for attempt in range(max_retries):
            try:
                return await asyncio.to_thread(self._fetch_stock_data, stock)
            except Exception as e:
                if attempt == max_retries - 1:  # 最後一次嘗試
                    raise e

                # 指數退避：每次重試等待時間加倍
                wait_time = (2 ** attempt) * 3 + random.uniform(2, 5)
                print(f"⚠️ 獲取 {stock} 資料失敗，{wait_time:.1f}秒後重試... (嘗試 {attempt + 1}/{max_retries})")
                await asyncio.sleep(wait_time)

    def _fetch_stock_data(self, stock):
        """同步獲取股票數據"""
        # 查詢 10 年期美國國債收益率
        # tnx = yf.Ticker("^TNX")
        # rf_rate = tnx.info['previousClose'] / 100

        # 獲取股票資料
        Stock = yf.Ticker(stock)
        # beta = Stock.info['beta']
        currentPrice = Stock.info['currentPrice']
        symbol = Stock.info['symbol']

        return {
            'Stock': symbol,
            'CurrentPrice': currentPrice,
            # 'beta': beta,
            # 'rf_rate': rf_rate
        }

    async def others_data(self, stock, excel_base64):
        """抓取其他數據並寫入Excel base64"""
        async with self.semaphore:  # 限制併發數量
            try:
                # 添加請求延遲，避免頻率過高
                await self._rate_limit("yfinance")

                # 使用重試機制獲取數據
                dic_data = await self._fetch_stock_data_with_retry(stock)

                print(f'{stock}: {dic_data}')

                # 寫入 Excel（移到線程中執行避免阻塞）
                modified_base64 = await self._write_to_excel(excel_base64, dic_data)

                return modified_base64, f'{stock}的其他資料成功寫入'

            except Exception as e:
                return excel_base64, f"獲取 {stock} 資料時發生錯誤：{str(e)}"

    def write_wacc_data_to_excel(self, stock, wacc_value, excel_base64):
        """將WACC數據寫入Excel"""
        try:
            print(f"正在處理 {stock} 的WACC值: {wacc_value}")

            # 解碼Excel
            excel_binary = base64.b64decode(excel_base64)
            excel_buffer = io.BytesIO(excel_binary)
            wb = load_workbook(excel_buffer)
            ws = wb.worksheets[3]  # 使用第四個工作表

            # 假設WACC值要寫入特定位置，這裡需要根據你的Excel模板調整
            # 清除舊資料
            ws['C5'] = None  # 你需要根據實際Excel模板調整位置
            # 例如：假設WACC值寫入B2儲存格
            ws['C5'] = wacc_value  # 你需要根據實際Excel模板調整位置

            # 可以添加一些格式設定
            # ws['B2'].font = Font(bold=False)

            # 保存修改後的Excel
            output_buffer = io.BytesIO()
            wb.save(output_buffer)
            output_buffer.seek(0)
            modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

            return modified_base64, f"成功將 {stock} 的WACC值 {wacc_value} 寫入Excel"

        except Exception as e:
            return None, f"寫入 {stock} 的WACC數據時發生錯誤: {e}"

    def write_TradeingView_data_to_excel(self, stock, tradingview_data, excel_base64):
        """將TradingView數據寫入Excel"""
        try:
            print(f"正在處理 {stock} 的TradingView數據")

            # 解碼Excel
            excel_binary = base64.b64decode(excel_base64)
            excel_buffer = io.BytesIO(excel_binary)
            wb = load_workbook(excel_buffer)
            ws = wb.worksheets[0]  # 使用第一個工作表

            # 檢查數據類型
            if tradingview_data is None:
                print(f"{stock} 的數據為空")
                return None, f"{stock} 的數據為空"

            # 從 DataFrame 獲取數據
            if hasattr(tradingview_data, 'columns') and hasattr(tradingview_data, 'index'):
                # 確認這是一個 DataFrame

                # 清除舊數據範圍 (可選，根據需要調整範圍)
                for row in range(6, 10):  # 清除前10行
                    for col in range(144, 154):  # 清除EN到EW列 (大概10列)
                        ws.cell(row=row, column=col).value = None

                # 指標名稱從EN開始，年份從EO開始 (EN = 第144列, EO = 第145列)
                start_row = 6
                label_col = 144  # EN列 - 指標名稱列
                start_col = 145  # EO列 - 數據開始列

                # 寫入年份標題 (第一行，從EO1開始)
                for col_idx, year in enumerate(tradingview_data.columns):
                    ws.cell(row=start_row, column=start_col + col_idx).value = int(year)

                # 寫入數據行 (從第二行開始)
                for row_idx, index_name in enumerate(tradingview_data.index):
                    # 寫入行標題 (指標名稱) 到EN列
                    ws.cell(row=start_row + row_idx + 1, column=label_col).value = index_name

                    # 寫入該行的所有數據
                    for col_idx, year in enumerate(tradingview_data.columns):
                        value = tradingview_data.loc[index_name, year]

                        # 處理不同類型的數據
                        if value is None or (isinstance(value, str) and value.lower() == 'none'):
                            cell_value = None
                        elif isinstance(value, str) and ('%' in value):
                            # 保持百分比格式
                            cell_value = value
                        else:
                            try:
                                # 嘗試轉換為數字
                                cell_value = float(value)
                            except (ValueError, TypeError):
                                # 如果無法轉換，保持原始字符串
                                cell_value = value

                        ws.cell(row=start_row + row_idx + 1, column=start_col + col_idx).value = cell_value

                print(f"成功將 {stock} 的數據寫入Excel，範圍: EN{start_row}:EO{start_row + len(tradingview_data.index)}")

                # *** 新增：自動調整欄寬 ***
                # 計算受影響的欄位範圍
                affected_columns = list(range(label_col, start_col + len(tradingview_data.columns)))

                for col_num in affected_columns:
                    try:
                        # 取得該欄位的所有儲存格
                        column_cells = [ws.cell(row=r, column=col_num) for r in
                                        range(1, start_row + len(tradingview_data.index) + 2)]

                        # 計算最大寬度
                        max_length = 0
                        for cell in column_cells:
                            if cell.value is not None:
                                cell_length = len(str(cell.value))
                                if cell_length > max_length:
                                    max_length = cell_length

                        # 設定欄寬 (最小寬度為8，最大寬度為50)
                        column_width = min(max(max_length + 2, 8), 50)

                        # 取得欄位字母
                        from openpyxl.utils import get_column_letter
                        column_letter = get_column_letter(col_num)
                        ws.column_dimensions[column_letter].width = column_width

                    except Exception as col_error:
                        print(f"調整欄位 {col_num} 寬度時發生錯誤: {col_error}")
                        continue

            else:
                return None, f"{stock} 的數據格式不正確，預期為DataFrame"

            # 保存修改後的Excel
            output_buffer = io.BytesIO()
            wb.save(output_buffer)
            output_buffer.seek(0)
            modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

            return modified_base64, f"成功將 {stock} 的TradingView數據寫入Excel"

        except Exception as e:
            return None, f"寫入 {stock} 的TradingView數據時發生錯誤: {e}"

    def write_seekingalpha_data_to_excel(self, stock, raw_revenue_growth, excel_base64):
        """將revenue_growth數據寫入Excel"""
        try:
            print(f"正在處理 {stock} 的revenue_growth值: {raw_revenue_growth}")

            # 解碼Excel
            excel_binary = base64.b64decode(excel_base64)
            excel_buffer = io.BytesIO(excel_binary)
            wb = load_workbook(excel_buffer)
            ws = wb.worksheets[3]  # 假設需要寫入的工作表是第四個

            # 提取5Y和10Y的數值（去掉%符號，轉換為float）
            if "5Y" in raw_revenue_growth and "10Y" in raw_revenue_growth:
                # 從 '27.89%' 提取 27.89
                revenue_5y_str = raw_revenue_growth["5Y"].replace('%', '')
                revenue_10y_str = raw_revenue_growth["10Y"].replace('%', '')

                try:
                    if revenue_5y_str == '-':
                        revenue_5y = None
                    else:
                        revenue_5y = float(revenue_5y_str) / 100

                    if revenue_10y_str == '-':
                        revenue_10y = None
                    else:
                        revenue_10y = float(revenue_10y_str) / 100

                except ValueError:
                    return None, f"無法轉換 {stock} 的revenue數值為浮點數: 5Y={revenue_5y_str}, 10Y={revenue_10y_str}"

                # 寫入對應的儲存格
                ws['F4'] = None
                ws['F5'] = None
                ws['F2'] = None

                # 寫入對應的儲存格
                ws['F4'] = revenue_5y  # 5Y數值寫入F4
                ws['F5'] = revenue_10y  # 10Y數值寫入F5
                ws['F2'] = revenue_10y  # 10Y數值寫入F5

                # 可以添加一些格式設定
                # ws['F4'].font = Font(bold=True)
                # ws['F5'].font = Font(bold=True)

                # 保存修改後的Excel
                output_buffer = io.BytesIO()
                wb.save(output_buffer)
                output_buffer.seek(0)
                modified_base64 = base64.b64encode(output_buffer.read()).decode('utf-8')

                return modified_base64, f"成功將 {stock} 的revenue growth數據寫入Excel (5Y: {revenue_5y}%, 10Y: {revenue_10y}%)"

            else:
                return None, f"{stock} 的revenue數據缺少5Y或10Y欄位: {raw_revenue_growth}"

        except Exception as e:
            return None, f"寫入 {stock} 的revenue growth數據時發生錯誤: {e}"


    async def _write_to_excel(self, excel_base64, dic_data):
        """寫入Excel文件"""

        def write_excel():
            excel_binary = base64.b64decode(excel_base64)
            excel_buffer = io.BytesIO(excel_binary)
            wb = load_workbook(excel_buffer)

            ws = wb.worksheets[0]  # 選擇第一個工作表

            ws['EQ2'] = dic_data['Stock']
            ws['ER2'] = dic_data['CurrentPrice']

            # ws = wb.worksheets[3]
            # ws['C31'] = dic_data['beta']
            # ws['C36'] = dic_data['rf_rate']

            # 儲存到base64
            output_buffer = io.BytesIO()
            wb.save(output_buffer)
            output_buffer.seek(0)
            return base64.b64encode(output_buffer.read()).decode('utf-8')

        return await asyncio.to_thread(write_excel)

    def save_excel_to_file(self, base64_data: str, output_path: str) -> bool:
        """將 base64 編碼的 Excel 資料保存為實體檔案"""
        try:
            excel_binary = base64.b64decode(base64_data)
            with open(output_path, 'wb') as f:
                f.write(excel_binary)
            print(f"Excel 檔案已保存至：{output_path}")
            return True
        except Exception as e:
            print(f"保存檔案時發生錯誤: {e}")
            return False