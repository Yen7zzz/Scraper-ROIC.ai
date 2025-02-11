import asyncio
import yfinance as yf
async def get_data(stock):
    Stock = yf.Ticker(stock)
    return Stock.info

async def main():
    stocks = ['NVDA','AAPL']
    tasks = [get_data(stock) for stock in stocks]
    l = await asyncio.gather(*tasks)
    return l

if __name__ == '__main__':
    res = asyncio.run(main())
    print(res)