from stock_class.StockAnalyzerGUI import StockAnalyzerGUI
from schwab.config_manager import check_and_setup_config


def main():
    """主程式入口"""
    # 檢查並設定配置
    config, should_continue = check_and_setup_config()

    if not should_continue:
        # 用戶取消設定，退出程式
        return

    # 配置已完成，啟動主程式
    print("🚀 啟動股票分析系統...")
    app = StockAnalyzerGUI()
    app.run()


if __name__ == "__main__":
    main()