import asyncio
from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess
from stock_class.StockManager import StockManager


async def main():
    """測試選擇權模板的Barchart功能"""

    # StockManager 需要字典格式
    stocks = {
        'final_stocks': ['CCL'],
        'us_stocks': ['CCL'],
        'non_us_stocks': []
    }

    scraper = StockScraper(stocks=stocks)
    process = StockProcess()
    manager = StockManager(scraper=scraper, processor=process, stocks=stocks)

    print("=" * 80)
    print("🚀 開始測試選擇權模板 Barchart 功能")
    print("=" * 80)

    # 步驟 1：初始化選擇權Excel檔案
    print("\n📋 步驟 1：初始化選擇權Excel檔案...")
    success = await manager.initialize_option_excel_files()
    if not success:
        print("❌ 選擇權Excel檔案初始化失敗，程式終止")
        return

    # 步驟 2：處理Barchart數據
    print("\n📊 步驟 2：抓取並處理Barchart數據...")
    await manager.process_barchart_for_options()

    await manager.process_option_chains()
    # 步驟 3：保存選擇權Excel檔案
    print("\n💾 步驟 3：保存選擇權Excel檔案...")
    output_folder = "."  # 當前目錄，你可以改成其他路徑
    saved_files = manager.save_all_option_excel_files(output_folder)

    # 顯示結果
    print("\n" + "=" * 80)
    print("🎉 測試完成！")
    print(f"📁 保存了 {len(saved_files)} 個檔案：")
    for file_path in saved_files:
        print(f"   ✅ {file_path}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())