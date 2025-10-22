"""
自動打包腳本 - 包含 Playwright 瀏覽器
"""
import os
import shutil
import subprocess
import sys


def main():
    print("=" * 60)
    print("🚀 開始打包股票分析程式")
    print("=" * 60)

    # 1. 確認 Playwright 已安裝
    print("\n📦 步驟 1/5: 檢查 Playwright 安裝...")
    playwright_path = shutil.which('playwright')
    if not playwright_path:
        print("❌ 找不到 Playwright，正在安裝...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'playwright'], check=True)
        subprocess.run([sys.executable, '-m', 'playwright', 'install', 'chromium'], check=True)
    else:
        print("✅ Playwright 已安裝")

    # 2. 確認 PyInstaller 已安裝
    print("\n📦 步驟 2/5: 檢查 PyInstaller 安裝...")
    try:
        import PyInstaller
        print("✅ PyInstaller 已安裝")
    except ImportError:
        print("❌ 找不到 PyInstaller，正在安裝...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', 'pyinstaller'], check=True)

    # 3. 清理舊的 build 文件
    print("\n🧹 步驟 3/5: 清理舊的打包文件...")
    folders_to_clean = ['build', 'dist']
    for folder in folders_to_clean:
        if os.path.exists(folder):
            shutil.rmtree(folder)
            print(f"✅ 已刪除 {folder}")

    # 4. 執行 PyInstaller 打包
    print("\n🔨 步驟 4/5: 執行 PyInstaller 打包...")

    # 檢查是否有 .spec 文件
    if os.path.exists('stock_analyzer.spec'):
        print("✅ 找到 stock_analyzer.spec，使用配置文件打包...")
        result = subprocess.run(['pyinstaller', 'stock_analyzer.spec'],
                                capture_output=True, text=True)
    else:
        print("⚠️ 未找到 .spec 文件，使用默認配置打包...")
        result = subprocess.run([
            'pyinstaller',
            '--name=StockAnalyzer',
            '--windowed',  # 隱藏控制台（如果要看 debug 訊息改成 --console）
            '--onedir',
            '--add-data=stock_class;stock_class',
            '--add-data=excel_template;excel_template',
            '--add-data=schwab;schwab',
            '--hidden-import=playwright',
            '--hidden-import=schwabdev',
            '--hidden-import=yfinance',
            'main.py'
        ], capture_output=True, text=True)

    if result.returncode != 0:
        print("❌ 打包失敗！")
        print(result.stderr)
        return False

    print("✅ PyInstaller 打包完成")

    # 5. 複製 Playwright 瀏覽器文件
    print("\n📂 步驟 5/5: 複製 Playwright 瀏覽器...")

    # 找到 Playwright 瀏覽器位置
    playwright_browsers = os.path.join(
        os.path.expanduser('~'),
        'AppData', 'Local', 'ms-playwright'
    )

    if os.path.exists(playwright_browsers):
        # 目標位置
        dist_browsers = os.path.join('dist', 'StockAnalyzer', 'ms-playwright')

        print(f"從: {playwright_browsers}")
        print(f"到: {dist_browsers}")

        # 只複製 chromium（節省空間）
        chromium_src = os.path.join(playwright_browsers, 'chromium-*')

        import glob
        chromium_folders = glob.glob(chromium_src)

        if chromium_folders:
            for folder in chromium_folders:
                folder_name = os.path.basename(folder)
                dest = os.path.join(dist_browsers, folder_name)

                print(f"  正在複製 {folder_name}...")
                shutil.copytree(folder, dest, dirs_exist_ok=True)
                print(f"  ✅ {folder_name} 複製完成")
        else:
            print("⚠️ 找不到 Chromium 瀏覽器，請手動安裝：")
            print("   python -m playwright install chromium")
    else:
        print("❌ 找不到 Playwright 瀏覽器目錄")
        print("請執行：python -m playwright install chromium")

    # 6. 創建使用說明
    print("\n📝 創建使用說明...")
    readme_content = """
# 股票分析程式 - 使用說明

## 首次使用
1. 雙擊 StockAnalyzer.exe 啟動程式
2. 如果提示需要 API 認證，請按照視窗指示完成設定
3. 完成後即可正常使用

## 系統需求
- Windows 10/11 (64位元)
- 需要網路連線

## 注意事項
- 第一次啟動可能需要較長時間
- 請勿刪除 ms-playwright 資料夾（瀏覽器引擎）
- schwab 資料夾存放 API 憑證，請妥善保管

## 問題排解
- 如果程式無法啟動，請檢查防毒軟體是否阻擋
- 如果選擇權功能無法使用，請重新完成 Schwab API 認證

## 聯絡資訊
如有問題請聯繫開發者
"""

    readme_path = os.path.join('dist', 'StockAnalyzer', 'README.txt')
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)

    print("✅ README.txt 已創建")

    # 完成
    print("\n" + "=" * 60)
    print("🎉 打包完成！")
    print("=" * 60)
    print(f"\n📁 執行檔位置：dist/StockAnalyzer/StockAnalyzer.exe")
    print(f"📦 資料夾大小：約 {get_folder_size('dist/StockAnalyzer'):.1f} MB")
    print("\n💡 提示：")
    print("  - 可以將整個 StockAnalyzer 資料夾壓縮後分享")
    print("  - 使用者解壓後直接執行 StockAnalyzer.exe 即可")
    print("  - 不要刪除 ms-playwright 資料夾（瀏覽器引擎）")

    return True


def get_folder_size(folder_path):
    """計算資料夾大小（MB）"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)
    return total_size / (1024 * 1024)


if __name__ == "__main__":
    try:
        success = main()
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️ 用戶中斷打包")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 打包過程發生錯誤：{e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)