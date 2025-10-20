from stock_class.StockAnalyzerGUI import StockAnalyzerGUI
from schwab.config_manager import check_and_setup_config
from stock_class.StockScraper import TokenExpiredException
import tkinter as tk
from tkinter import messagebox
import sys
import os


def main():
    """主程式入口"""
    try:
        # 檢查並設定配置
        config, should_continue = check_and_setup_config()

        if not should_continue:
            # 用戶取消設定，退出程式
            return

        # 配置已完成，啟動主程式
        print("🚀 啟動股票分析系統...")
        app = StockAnalyzerGUI(config)
        app.run()

    except TokenExpiredException as e:
        # 捕獲 Token 過期異常
        print(f"\n❌ Token 認證失敗")
        print(f"錯誤詳情：{str(e)}")

        # 創建臨時視窗來顯示對話框
        temp_root = tk.Tk()
        temp_root.withdraw()

        # 顯示詢問對話框
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
            # 用戶選擇重新認證
            print("\n🔄 準備重新認證...")

            # 刪除舊 Token
            from schwab.config_manager import ConfigManager
            config_manager = ConfigManager()
            config_manager.delete_token()

            # 重新啟動程式（呼叫自己）
            print("🔄 重新啟動程式...")
            python = sys.executable
            os.execl(python, python, *sys.argv)
        else:
            print("❌ 用戶選擇退出程式")
            return

    except KeyboardInterrupt:
        print("\n⚠️ 用戶中斷程式")
        return

    except Exception as e:
        print(f"\n❌ 程式發生未預期的錯誤：{str(e)}")
        import traceback
        traceback.print_exc()
        return


if __name__ == "__main__":
    main()