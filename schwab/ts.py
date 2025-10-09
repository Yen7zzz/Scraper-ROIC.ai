import pandas as pd
import logging
import os
import dotenv
import schwabdev

# load environment
dotenv.load_dotenv()

# warn user if they have not added their keys to the .env
if len(os.getenv('app_key')) not in (32, 48) or len(os.getenv('app_secret')) not in (16, 64):
    raise Exception("Add you app key and app secret to the .env file.")

# set logging level
logging.basicConfig(level=logging.INFO)

# make a client
client = schwabdev.Client(os.getenv('app_key'), os.getenv('app_secret'), os.getenv('callback_url'))
option = client.option_chains('CCL').json()

def flatten_option_chain_to_excel(option_data, output_filename='option_chain_data.xlsx'):
    """
    將選擇權鏈數據展平並寫入Excel
    """
    all_options = []

    # 提取基本股票資訊，用於每個合約
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

    # 處理 Call 選擇權
    if 'callExpDateMap' in option_data:
        for exp_date_key, strikes in option_data['callExpDateMap'].items():
            for strike_price, contracts in strikes.items():
                for contract in contracts:
                    # 合併基本資訊和合約資訊
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
                    # 合併基本資訊和合約資訊
                    option_record = base_info.copy()
                    option_record.update(contract)
                    option_record['expDateKey'] = exp_date_key
                    option_record['strikeKey'] = strike_price
                    all_options.append(option_record)

    # 轉換為DataFrame
    df = pd.DataFrame(all_options)

    # 寫入Excel
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='OptionChain', index=False)

        # 調整欄位寬度
        worksheet = writer.sheets['OptionChain']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[column_letter].width = adjusted_width

    print(f"數據已寫入 {output_filename}")
    print(f"總共 {len(df)} 筆選擇權合約")
    print(f"欄位數量: {len(df.columns)}")
    print("\n前幾個欄位名稱:")
    for i, col in enumerate(df.columns[:20]):
        print(f"{i + 1}: {col}")
    if len(df.columns) > 20:
        print("...")

    return df

# 使用方式
# 假設你的選擇權數據存在變數 option 中
df = flatten_option_chain_to_excel(option, 'CCL_option_chain.xlsx')