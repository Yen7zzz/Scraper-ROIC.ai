import pandas as pd
df = pd.read_excel(r'C:\tmp\stock_template\STOCK_NVDA.xlsx')
df_Customize_view = df.iloc[:19, :12]
# print(df_Customize_view)
df_Income_Statement = df.iloc[:45,13:25]
# print(df_Income_Statement)
df_Balance_Sheet = df.iloc[:81,26:38]
# print(df_Balance_Sheet)
df_Cash_Flow_Statement = df.iloc[:54,39:51]
print(df_Cash_Flow_Statement)
df_Profitability = df.iloc[:14, 52:62]
# print(df_Profitability)
df_Credit = df.iloc[:23, 65:77]
# print(df_Credit)
df_Liquidity = df.iloc[:14,78:90]
# print(df_Liquidity)
df_Working_Capital = df.iloc[:13,91:103]
# print(df_Working_Capital)
df_Enterprise_Value = df.iloc[:22,104:116]
# print(df_Enterprise_Value)
df_Multiples = df.iloc[:43,117:129]
# print(df_Multiples)
df_Per_Share_Data_Items = df.iloc[:16, 130:142]
# print(df_Per_Share_Data_Items)
# free_cash_flow_row = df_Cash_Flow_Statement['Cash Flow Statement']
# print(free_cash_flow_row)
free_cash_flow_row = df_Cash_Flow_Statement.iloc[51]
free_cash_flow_value = free_cash_flow_row.iloc[1:].astype(float)
print(free_cash_flow_value)

import matplotlib.pyplot as plt
x_labels = free_cash_flow_value.index  # 提取年份
y_values = free_cash_flow_value.values  # 提取數值

# 繪圖
plt.plot(x_labels, y_values, marker='o', linestyle='-', color='b')
plt.title('Free Cash Flow per Basic Share')
plt.xlabel('Year')
plt.ylabel('Free Cash Flow')
plt.xticks(rotation=45)  # 旋轉 x 軸標籤以防擁擠
plt.grid(True)
plt.tight_layout()
plt.show()