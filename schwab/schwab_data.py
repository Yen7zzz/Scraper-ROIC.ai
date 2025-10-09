import pandas as pd
import logging
import os
import dotenv
import schwabdev
from typing import Dict, Optional
from datetime import datetime


class OptionChainProcessor:
    """選擇權鏈數據處理器"""

    def __init__(self, app_key: str = None, app_secret: str = None, callback_url: str = None):
        """
        初始化選擇權處理器

        Args:
            app_key: API Key (如不提供則從環境變數讀取)
            app_secret: API Secret (如不提供則從環境變數讀取)
            callback_url: Callback URL (如不提供則從環境變數讀取)
        """
        # 載入環境變數
        dotenv.load_dotenv()

        # 設定憑證
        self.app_key = app_key or os.getenv('app_key')
        self.app_secret = app_secret or os.getenv('app_secret')
        self.callback_url = callback_url or os.getenv('callback_url')

        # 驗證憑證
        self._validate_credentials()

        # 設定日誌
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # 初始化客戶端
        self.client = schwabdev.Client(self.app_key, self.app_secret, self.callback_url)

    def _validate_credentials(self):
        """驗證 API 憑證格式"""
        if not self.app_key or len(self.app_key) not in (32, 48):
            raise ValueError(
                f"App Key 格式不正確！當前長度: {len(self.app_key) if self.app_key else 0}，"
                "應為 32 或 48 字元。請檢查 .env 檔案。"
            )

        if not self.app_secret or len(self.app_secret) not in (16, 64):
            raise ValueError(
                f"App Secret 格式不正確！當前長度: {len(self.app_secret) if self.app_secret else 0}，"
                "應為 16 或 64 字元。請檢查 .env 檔案。"
            )

    def fetch_and_process(self, symbol: str, output_filename: Optional[str] = None) -> pd.DataFrame:
        """
        獲取選擇權數據並處理成 DataFrame，同時匯出到 Excel

        Args:
            symbol: 股票代碼
            output_filename: 輸出檔案名稱 (預設: {symbol}_option_chain_{timestamp}.xlsx)

        Returns:
            處理後的 DataFrame
        """
        # 獲取數據
        self.logger.info(f"正在獲取 {symbol} 的選擇權鏈數據...")
        option_data = self.client.option_chains(symbol).json()

        # 展平數據
        self.logger.info("開始處理選擇權數據...")
        df = self._flatten_option_data(option_data)

        # 匯出到 Excel
        if output_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"{symbol}_option_chain_{timestamp}.xlsx"

        self._export_to_excel(df, output_filename)

        return df

    def _flatten_option_data(self, option_data: Dict) -> pd.DataFrame:
        """
        將選擇權數據展平為 DataFrame

        Args:
            option_data: 選擇權數據字典

        Returns:
            展平後的 DataFrame
        """
        # 提取基本資訊
        base_info = {
            'symbol': option_data.get('symbol'),
            'status': option_data.get('status'),
            'underlying': option_data.get('underlying'),
            'strategy': option_data.get('strategy'),
            'interval': option_data.get('interval'),
            'isDelayed': option_data.get('isDelayed'),
            'isIndex': option_data.get('isIndex'),
            'interestRate': option_data.get('interestRate'),
            'underlyingPrice': option_data.get('underlyingPrice'),
            'volatility': option_data.get('volatility'),
            'daysToExpiration': option_data.get('daysToExpiration'),
            'dividendYield': option_data.get('dividendYield'),
            'numberOfContracts': option_data.get('numberOfContracts'),
            'assetMainType': option_data.get('assetMainType'),
            'assetSubType': option_data.get('assetSubType'),
            'isChainTruncated': option_data.get('isChainTruncated')
        }

        all_options = []

        # 處理 Call 選擇權
        if 'callExpDateMap' in option_data:
            for exp_date_key, strikes in option_data['callExpDateMap'].items():
                for strike_price, contracts in strikes.items():
                    for contract in contracts:
                        option_record = base_info.copy()
                        option_record.update(contract)
                        option_record['expDateKey'] = exp_date_key
                        option_record['strikeKey'] = strike_price
                        all_options.append(option_record)

        # 處理 Put 選擇權
        if 'putExpDateMap' in option_data:
            for exp_date_key, strikes in option_data['putExpDateMap'].items():
                for strike_price, contracts in strikes.items():
                    for contract in contracts:
                        option_record = base_info.copy()
                        option_record.update(contract)
                        option_record['expDateKey'] = exp_date_key
                        option_record['strikeKey'] = strike_price
                        all_options.append(option_record)

        df = pd.DataFrame(all_options)
        self.logger.info(f"總共處理了 {len(df)} 筆選擇權合約")

        return df

    def _export_to_excel(self, df: pd.DataFrame, output_filename: str):
        """
        將 DataFrame 匯出到 Excel

        Args:
            df: 要匯出的 DataFrame
            output_filename: 輸出檔案名稱
        """
        self.logger.info(f"開始匯出數據到 {output_filename}...")

        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='OptionChain', index=False)

            # 調整欄位寬度
            worksheet = writer.sheets['OptionChain']
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        # 打印摘要
        print(f"\n✅ 數據已寫入 {output_filename}")
        print(f"總共 {len(df)} 筆選擇權合約")
        print(f"欄位數量: {len(df.columns)}")
        print("\n前 20 個欄位名稱:")
        for i, col in enumerate(df.columns[:20], 1):
            print(f"  {i:2d}. {col}")
        if len(df.columns) > 20:
            print(f"  ... 還有 {len(df.columns) - 20} 個欄位\n")


# 使用範例
if __name__ == "__main__":
    # 創建處理器
    processor = OptionChainProcessor()

    # 一行搞定：獲取、處理、匯出
    df = processor.fetch_and_process('CCL')

    # 或指定檔案名稱
    df = processor.fetch_and_process('CCL', 'my_options.xlsx')