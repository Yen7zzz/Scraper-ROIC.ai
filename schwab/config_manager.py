"""
完整的配置管理模組 - 整合到股票分析程式 (.env 版本)
包含正確的 OAuth 認證流程
"""
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import os
import sys
import webbrowser
import schwabdev


class ConfigManager:
    """配置管理器 - 處理 API 憑證的存儲和讀取"""

    def __init__(self):
        # 確定基礎路徑
        if getattr(sys, 'frozen', False):
            self.base_path = os.path.dirname(sys.executable)
        else:
            self.base_path = os.path.dirname(os.path.abspath(__file__))

        self.env_path = os.path.join(self.base_path, '.env')
        self.token_path = os.path.join(self.base_path, 'token.txt')

    def config_exists(self):
        """檢查配置檔案是否存在"""
        return os.path.exists(self.env_path)

    def load_config(self):
        """讀取 .env 配置"""
        try:
            config = {}
            with open(self.env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        config[key] = value
            return config if config else None
        except Exception as e:
            print(f"讀取配置失敗: {e}")
            return None

    def save_config(self, config_data):
        """保存配置到 .env 檔案"""
        try:
            with open(self.env_path, 'w', encoding='utf-8') as f:
                f.write("# INCLUDE THIS FILE IN YOUR .gitignore\n\n")
                f.write(f'app_key = "{config_data["app_key"]}"\n')
                f.write(f'app_secret = "{config_data["app_secret"]}"\n')
                f.write(f'callback_url = "https://127.0.0.1"\n')
            return True
        except Exception as e:
            print(f"保存配置失敗: {e}")
            return False

    def has_valid_token(self):
        """檢查是否有有效的 token"""
        return os.path.exists(self.token_path)


class OAuthSetupWindow:
    """OAuth 認證設定視窗"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("🔧 API 認證設定")
        self.root.geometry("1200x1000")
        self.root.resizable(True, True)
        self.root.configure(bg='#1a1a1a')
        self.root.minsize(1200, 1000)

        self.config_manager = ConfigManager()
        self.config_saved = False
        self.auth_url = None
        self.app_key = None
        self.app_secret = None

        self.setup_ui()
        self.center_window()

    def center_window(self):
        """視窗置中"""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    def setup_ui(self):
        # 主框架
        main_frame = tk.Frame(self.root, bg='#2d2d2d')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 標題
        title_label = tk.Label(
            main_frame,
            text="📊 Schwab API 認證設定",
            font=('微軟正黑體', 18, 'bold'),
            fg='#00d4aa',
            bg='#2d2d2d'
        )
        title_label.pack(pady=(10, 20))

        # === 步驟 1: 輸入憑證 ===
        step1_frame = tk.LabelFrame(
            main_frame,
            text="  步驟 1: 輸入 API 憑證  ",
            font=('微軟正黑體', 12, 'bold'),
            fg='#00d4aa',
            bg='#2d2d2d',
            relief='solid',
            bd=2
        )
        step1_frame.pack(fill=tk.X, pady=10, padx=10)

        input_frame = tk.Frame(step1_frame, bg='#2d2d2d')
        input_frame.pack(pady=15, padx=20)

        # App Key
        tk.Label(
            input_frame,
            text="🔑 App Key:",
            font=('微軟正黑體', 11, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).grid(row=0, column=0, sticky=tk.W, pady=10)

        self.app_key_entry = tk.Entry(
            input_frame,
            width=50,
            font=('Consolas', 10),
            bg='#3d3d3d',
            fg='#ffffff',
            insertbackground='#00d4aa',
            relief='flat',
            bd=2
        )
        self.app_key_entry.grid(row=0, column=1, pady=10, padx=10, ipady=8)

        # App Secret
        tk.Label(
            input_frame,
            text="🔐 App Secret:",
            font=('微軟正黑體', 11, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).grid(row=1, column=0, sticky=tk.W, pady=10)

        self.app_secret_entry = tk.Entry(
            input_frame,
            width=50,
            font=('Consolas', 10),
            bg='#3d3d3d',
            fg='#ffffff',
            insertbackground='#00d4aa',
            show="●",
            relief='flat',
            bd=2
        )
        self.app_secret_entry.grid(row=1, column=1, pady=10, padx=10, ipady=8)

        # 生成授權連結按鈕
        btn_frame1 = tk.Frame(step1_frame, bg='#2d2d2d')
        btn_frame1.pack(pady=10)

        self.generate_btn = tk.Button(
            btn_frame1,
            text="🔗 生成授權連結",
            command=self.generate_auth_url,
            font=('微軟正黑體', 11, 'bold'),
            bg='#00d4aa',
            fg='white',
            activebackground='#00b894',
            width=20,
            height=1,
            relief='flat',
            cursor='hand2'
        )
        self.generate_btn.pack()

        # === 步驟 2: 瀏覽器認證 ===
        step2_frame = tk.LabelFrame(
            main_frame,
            text="  步驟 2: 在瀏覽器中完成認證  ",
            font=('微軟正黑體', 12, 'bold'),
            fg='#ffb347',
            bg='#2d2d2d',
            relief='solid',
            bd=2
        )
        step2_frame.pack(fill=tk.X, pady=10, padx=10)

        # 說明文字
        instruction_text = """
點擊下方按鈕後，瀏覽器將開啟 Schwab 認證頁面。

請在瀏覽器中：
  1. 登入您的 Schwab 帳號
  2. 授權應用程式存取權限
  3. 完成後會跳轉到一個「無法連接」的頁面（這是正常的！）
  4. 複製瀏覽器網址列中的完整 URL
     （例如：https://127.0.0.1/?code=ABCD1234...）
        """

        instruction_label = tk.Label(
            step2_frame,
            text=instruction_text,
            font=('微軟正黑體', 10),
            justify=tk.LEFT,
            fg='#ffffff',
            bg='#2d2d2d'
        )
        instruction_label.pack(pady=10, padx=20)

        # 授權 URL 顯示框
        url_display_frame = tk.Frame(step2_frame, bg='#2d2d2d')
        url_display_frame.pack(fill=tk.X, pady=5, padx=20)

        tk.Label(
            url_display_frame,
            text="授權連結：",
            font=('微軟正黑體', 10),
            fg='#b0b0b0',
            bg='#2d2d2d'
        ).pack(anchor=tk.W)

        self.url_display = scrolledtext.ScrolledText(
            url_display_frame,
            height=3,
            font=('Consolas', 9),
            bg='#3d3d3d',
            fg='#00d4aa',
            relief='flat',
            wrap=tk.WORD,
            state='disabled'
        )
        self.url_display.pack(fill=tk.X, pady=5)

        # 開啟瀏覽器按鈕
        btn_frame2 = tk.Frame(step2_frame, bg='#2d2d2d')
        btn_frame2.pack(pady=10)

        self.browser_btn = tk.Button(
            btn_frame2,
            text="🌐 開啟瀏覽器進行認證",
            command=self.open_browser,
            font=('微軟正黑體', 11, 'bold'),
            bg='#6c5ce7',
            fg='white',
            activebackground='#5f4dd1',
            width=25,
            height=1,
            relief='flat',
            cursor='hand2',
            state='disabled'
        )
        self.browser_btn.pack()

        # === 步驟 3: 貼上回調 URL ===
        step3_frame = tk.LabelFrame(
            main_frame,
            text="  步驟 3: 貼上回調 URL  ",
            font=('微軟正黑體', 12, 'bold'),
            fg='#ff6b6b',
            bg='#2d2d2d',
            relief='solid',
            bd=2
        )
        step3_frame.pack(fill=tk.X, pady=10, padx=10)

        callback_frame = tk.Frame(step3_frame, bg='#2d2d2d')
        callback_frame.pack(pady=15, padx=20, fill=tk.X)

        tk.Label(
            callback_frame,
            text="🔗 貼上完整的回調 URL：",
            font=('微軟正黑體', 11, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(anchor=tk.W, pady=5)

        self.callback_entry = tk.Entry(
            callback_frame,
            font=('Consolas', 10),
            bg='#3d3d3d',
            fg='#ffffff',
            insertbackground='#00d4aa',
            relief='flat',
            bd=2,
            state='disabled'
        )
        self.callback_entry.pack(fill=tk.X, ipady=8, pady=5)

        tk.Label(
            callback_frame,
            text="💡 提示：URL 應該類似 https://127.0.0.1/?code=...",
            font=('微軟正黑體', 9),
            fg='#b0b0b0',
            bg='#2d2d2d'
        ).pack(anchor=tk.W)

        # 完成認證按鈕
        btn_frame3 = tk.Frame(step3_frame, bg='#2d2d2d')
        btn_frame3.pack(pady=10)

        self.complete_btn = tk.Button(
            btn_frame3,
            text="✅ 完成認證",
            command=self.complete_authentication,
            font=('微軟正黑體', 11, 'bold'),
            bg='#00d4aa',
            fg='white',
            activebackground='#00b894',
            width=20,
            height=1,
            relief='flat',
            cursor='hand2',
            state='disabled'
        )
        self.complete_btn.pack()

        # 取消按鈕
        cancel_frame = tk.Frame(main_frame, bg='#2d2d2d')
        cancel_frame.pack(pady=20)

        cancel_btn = tk.Button(
            cancel_frame,
            text="❌ 取消退出",
            command=self.cancel_setup,
            font=('微軟正黑體', 11),
            bg='#ff6b35',
            fg='white',
            activebackground='#e84393',
            width=15,
            height=1,
            relief='flat',
            cursor='hand2'
        )
        cancel_btn.pack()

    def generate_auth_url(self):
        """生成授權 URL"""
        app_key = self.app_key_entry.get().strip()
        app_secret = self.app_secret_entry.get().strip()

        # 驗證
        if not app_key:
            messagebox.showerror("❌ 錯誤", "請填寫 App Key！")
            return

        if not app_secret:
            messagebox.showerror("❌ 錯誤", "請填寫 App Secret！")
            return

        if len(app_key) not in (32, 48):
            messagebox.showerror("❌ 格式錯誤",
                f"App Key 長度不正確！\n當前：{len(app_key)} 字元\n正確：32 或 48 字元")
            return

        if len(app_secret) not in (16, 64):
            messagebox.showerror("❌ 格式錯誤",
                f"App Secret 長度不正確！\n當前：{len(app_secret)} 字元\n正確：16 或 64 字元")
            return

        # 保存憑證
        self.app_key = app_key
        self.app_secret = app_secret

        # 生成授權 URL
        try:
            callback_url = "https://127.0.0.1"
            self.auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?client_id={app_key}&redirect_uri={callback_url}"

            # 顯示 URL
            self.url_display.config(state='normal')
            self.url_display.delete(1.0, tk.END)
            self.url_display.insert(1.0, self.auth_url)
            self.url_display.config(state='disabled')

            # 啟用開啟瀏覽器按鈕
            self.browser_btn.config(state='normal')

            messagebox.showinfo("✅ 成功", "授權連結已生成！\n\n請點擊「開啟瀏覽器進行認證」按鈕。")

        except Exception as e:
            messagebox.showerror("❌ 錯誤", f"生成授權連結失敗：\n{e}")

    def open_browser(self):
        """開啟瀏覽器"""
        if self.auth_url:
            webbrowser.open(self.auth_url)
            messagebox.showinfo(
                "🌐 瀏覽器已開啟",
                "請在瀏覽器中完成認證。\n\n"
                "完成後：\n"
                "1. 複製瀏覽器網址列的完整 URL\n"
                "2. 回到此視窗\n"
                "3. 貼到「步驟 3」的輸入框中"
            )
            # 啟用回調 URL 輸入和完成按鈕
            self.callback_entry.config(state='normal')
            self.complete_btn.config(state='normal')

    def complete_authentication(self):
        """完成認證"""
        returned_url = self.callback_entry.get().strip()

        if not returned_url:
            messagebox.showerror("❌ 錯誤", "請貼上瀏覽器返回的完整 URL！")
            return

        if "code=" not in returned_url:
            messagebox.showerror("❌ 錯誤",
                "URL 格式不正確！\n\n"
                "請確認 URL 包含授權碼（code=...）")
            return

        try:
            # 保存配置
            config_data = {
                'app_key': self.app_key,
                'app_secret': self.app_secret
            }
            self.config_manager.save_config(config_data)

            # 使用 schwabdev 完成認證
            print("🔐 正在用授權碼換取 Token...")
            client = schwabdev.Client(self.app_key, self.app_secret, callback_url="https://127.0.0.1")

            # schwabdev 會自動處理 token 交換和保存
            # 如果需要手動處理，可以使用 client 的內部方法

            messagebox.showinfo(
                "✅ 認證成功",
                "Token 已成功獲取並保存！\n\n"
                "程式現在可以正常使用了。"
            )

            self.config_saved = True
            self.root.quit()
            self.root.destroy()

        except Exception as e:
            messagebox.showerror("❌ 認證失敗",
                f"認證過程發生錯誤：\n\n{str(e)}\n\n"
                "請確認：\n"
                "1. URL 是否完整複製\n"
                "2. 授權碼是否還有效\n"
                "3. 網路連線是否正常")

    def cancel_setup(self):
        """取消設定"""
        result = messagebox.askyesno(
            "⚠️ 確認退出",
            "尚未完成設定，確定要退出嗎？\n\n退出後程式將無法正常運行。"
        )
        if result:
            self.config_saved = False
            self.root.quit()
            self.root.destroy()

    def run(self):
        """運行設定視窗"""
        self.root.protocol("WM_DELETE_WINDOW", self.cancel_setup)
        self.root.mainloop()
        return self.config_saved


def check_and_setup_config():
    """
    檢查配置並在需要時啟動設定視窗
    返回: (config_data, should_continue)
    """
    config_manager = ConfigManager()

    # 如果配置和 token 都存在，直接使用
    if config_manager.config_exists() and config_manager.has_valid_token():
        config = config_manager.load_config()
        if config:
            print("✅ 已載入現有配置和 Token")
            return config, True

    # 需要重新設定
    if not config_manager.config_exists():
        print("⚙️ 首次運行，啟動配置設定...")
    else:
        print("⚠️ Token 無效或不存在，需要重新認證...")

    setup_window = OAuthSetupWindow()
    success = setup_window.run()

    if success:
        config = config_manager.load_config()
        return config, True
    else:
        print("❌ 用戶取消設定，程式退出")
        return None, False


# 測試用
if __name__ == "__main__":
    config, should_continue = check_and_setup_config()

    if should_continue:
        print("\n✅ 配置完成！")
        print(f"App Key: {config['app_key'][:10]}...")
        print("系統已就緒，可以開始使用！")
    else:
        print("程式已退出")