"""
main.py - 加入啟動畫面版本
"""

# 🔥 步驟 1: 引入 pyi_splash (只在打包後有效)
try:
    import pyi_splash
    splash_available = True
except ImportError:
    splash_available = False

from stock_class.StockAnalyzerGUI import StockAnalyzerGUI
from schwab.config_manager import check_and_setup_config
from stock_class.StockScraper import TokenExpiredException
import tkinter as tk
from tkinter import messagebox
import sys
import os

def main():
    """主程式入口 - 加入啟動畫面"""
    try:
        # 🔥 關鍵修正：在任何 GUI 視窗出現前就關閉啟動畫面
        print("🔧 初始化系統...")

        # 🔥 步驟 1: 立即關閉啟動畫面（因為我們要顯示認證視窗了）
        if splash_available:
            try:
                pyi_splash.close()
                print("✓ 啟動畫面已關閉")
            except Exception as e:
                print(f"⚠️ 關閉啟動畫面時發生錯誤: {e}")

        # 🔥 步驟 2: 檢查並設定配置（可能會顯示認證視窗）
        config, should_continue = check_and_setup_config()

        if not should_continue:
            return

        # 配置已完成,準備啟動主視窗
        print("🚀 啟動股票分析系統...")

        # 🔥 步驟 3: 啟動主 GUI
        app = StockAnalyzerGUI(config)
        app.run()

    except TokenExpiredException as e:
        # 關閉啟動畫面(如果還沒關)
        if splash_available:
            try:
                pyi_splash.close()
            except Exception:
                pass

        # Token 錯誤處理
        print(f"\n❌ Token 認證失敗")
        print(f"錯誤詳情：{str(e)}")

        temp_root = tk.Tk()
        temp_root.withdraw()

        response = messagebox.askyesno(
            "❌ Token 認證失敗",
            f"{str(e)}\n\n"
            "是否立即重新認證？\n\n"
            "選擇「是」：將關閉程式並啟動認證流程\n"
            "選擇「否」：關閉程式",
            icon='error'
        )

        temp_root.destroy()

        if response:
            from schwab.config_manager import ConfigManager
            config_manager = ConfigManager()
            config_manager.delete_token()

            print("🔄 重新啟動程式...")
            python = sys.executable
            os.execl(python, python, *sys.argv)
        else:
            print("❌ 用戶選擇退出程式")
            return

    except KeyboardInterrupt:
        print("\n⚠️ 用戶中斷程式")
        # 關閉啟動畫面
        if splash_available:
            try:
                pyi_splash.close()
            except Exception:
                pass
        return

    except Exception as e:
        print(f"\n❌ 程式發生未預期的錯誤：{str(e)}")

        # 關閉啟動畫面
        if splash_available:
            try:
                pyi_splash.close()
            except Exception:
                pass

        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()