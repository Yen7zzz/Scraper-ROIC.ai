"""
完整的配置管理模組 - 最終正確版
使用 monkey patch 替換 input() 函數
"""
import webbrowser
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import os
import sys
import webbrowser
import schwabdev
import threading
import queue
import builtins
import json


class ConfigManager:
    """配置管理器 - 處理 API 憑證的存儲和讀取"""

    def __init__(self):
        # 確定基礎路徑
        if getattr(sys, 'frozen', False):
            self.base_path = os.path.dirname(sys.executable)
        else:
            self.base_path = os.path.dirname(os.path.abspath(__file__))

        self.env_path = os.path.join(self.base_path, '.env')
        self.tokens_path = os.path.join(self.base_path, 'tokens.json')

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
        return os.path.exists(self.tokens_path)

    def is_token_valid(self, buffer_days=1):
        """檢查 Refresh Token 是否仍然有效"""
        from datetime import datetime, timezone

        try:
            if not os.path.exists(self.tokens_path):
                return False, 0, None

            with open(self.tokens_path, 'r') as f:
                tokens = json.load(f)

            # 取得 refresh_token 發行時間
            refresh_issued = tokens.get('refresh_token_issued')
            if not refresh_issued:
                return False, 0, None

            # 解析 ISO 格式時間
            issued_time = datetime.fromisoformat(refresh_issued.replace('Z', '+00:00'))

            # Schwab Refresh Token 有效期是 7 天
            from datetime import timedelta
            expiry_time = issued_time + timedelta(days=7)

            # 計算剩餘時間
            current_time = datetime.now(timezone.utc)
            remaining_seconds = (expiry_time - current_time).total_seconds()
            remaining_hours = remaining_seconds / 3600
            remaining_days = remaining_seconds / 86400

            # 判斷是否有效（剩餘時間大於緩衝天數）
            is_valid = remaining_days > buffer_days

            return is_valid, remaining_hours, expiry_time

        except Exception as e:
            print(f"❌ 檢查 Token 時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return False, 0, None

    def delete_token(self):
        """安全刪除 Token 檔案"""
        try:
            if os.path.exists(self.tokens_path):
                os.remove(self.tokens_path)
                print(f"🗑️ 已刪除 Token 檔案: {self.tokens_path}")
                return True
            else:
                print(f"⚠️ Token 檔案不存在: {self.tokens_path}")
                return False
        except Exception as e:
            print(f"❌ 刪除 Token 時發生錯誤: {e}")
            return False

    def get_token_expiry_info(self):
        """
        獲取 Token 過期資訊的詳細字串

        返回:
            str: 格式化的過期資訊
        """
        is_valid, remaining_hours, expiry_time = self.is_token_valid(buffer_days=0)

        if expiry_time is None:
            return "Token 不存在或無法讀取"

        from datetime import datetime

        if remaining_hours < 0:
            # 已過期
            hours_ago = abs(remaining_hours)
            return f"Token 已過期（{hours_ago:.1f} 小時前過期）"
        elif remaining_hours < 24:
            # 不到 1 天
            return f"Token 將在 {remaining_hours:.1f} 小時後過期\n過期時間：{expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            # 超過 1 天
            days = remaining_hours / 24
            return f"Token 將在 {days:.1f} 天後過期\n過期時間：{expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"


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

        # 用於執行緒間通訊
        self.callback_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.auth_thread = None

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
            bg='#00d4aa',
            fg='white',
            activebackground='#00b894',
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
        """開啟瀏覽器並啟動背景認證執行緒"""
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

            # 保存配置（提前保存）
            config_data = {
                'app_key': self.app_key,
                'app_secret': self.app_secret
            }
            self.config_manager.save_config(config_data)
            print("✅ 配置已保存到 .env")

            # 啟動背景執行緒來處理 schwabdev 認證
            self.start_auth_thread()

            # 啟用回調 URL 輸入和完成按鈕
            self.callback_entry.config(state='normal')
            self.complete_btn.config(state='normal')

    def start_auth_thread(self):
        """在背景執行緒啟動 schwabdev Client - 使用 monkey patch"""

        def auth_worker():
            try:
                print("🔄 背景執行緒：正在初始化 schwabdev Client...")

                # 保存原始的 input 和 webbrowser.open 函數
                original_input = builtins.input
                original_webbrowser_open = webbrowser.open  # 👈 新增

                # 創建自定義 input 函數
                def custom_input(prompt=""):
                    if prompt:
                        print(prompt, end='', flush=True)
                    url = self.callback_queue.get()
                    print(url)
                    return url

                # 👇 新增：禁用 webbrowser.open（因為已經手動開啟過了）
                def disabled_webbrowser_open(url, new=0, autoraise=True):
                    print(f"🚫 已禁用自動開啟瀏覽器（URL: {url[:50]}...）")
                    return True  # 假裝成功

                # 替換 builtins.input 和 webbrowser.open
                builtins.input = custom_input
                webbrowser.open = disabled_webbrowser_open  # 👈 新增

                try:
                    # 🔥 修改：使用完整路徑指向 schwab/ 資料夾
                    tokens_file_path = os.path.join(
                        self.config_manager.base_path,
                        'tokens.json'
                    )

                    print(f"📁 Token 將保存至: {tokens_file_path}")

                    # 初始化 schwabdev Client
                    client = schwabdev.Client(
                        self.app_key,
                        self.app_secret,
                        tokens_file=tokens_file_path  # 👈 使用完整路徑
                    )

                    print("✅ schwabdev Client 初始化成功！")
                    self.result_queue.put(('success', None))

                finally:
                    # 恢復原始函數
                    builtins.input = original_input
                    webbrowser.open = original_webbrowser_open  # 👈 新增

            except Exception as e:
                print(f"❌ 背景執行緒錯誤: {e}")
                import traceback
                traceback.print_exc()
                self.result_queue.put(('error', str(e)))
                # 確保恢復原始函數
                builtins.input = original_input
                webbrowser.open = original_webbrowser_open  # 👈 新增

        self.auth_thread = threading.Thread(target=auth_worker, daemon=True)
        self.auth_thread.start()

    def complete_authentication(self):
        """完成認證 - 將 URL 傳給背景執行緒"""
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
            print(f"📤 將 callback URL 傳送給背景執行緒...")

            # 把 URL 放入 queue，讓背景執行緒的 schwabdev 使用
            self.callback_queue.put(returned_url)

            # 禁用按鈕，避免重複點擊
            self.complete_btn.config(state='disabled', text="⏳ 處理中...")
            self.callback_entry.config(state='disabled')

            # 啟動檢查結果的定時器
            self.root.after(100, self.check_auth_result)

        except Exception as e:
            messagebox.showerror("❌ 錯誤", f"發生錯誤：\n{str(e)}")
            self.complete_btn.config(state='normal', text="✅ 完成認證")
            self.callback_entry.config(state='normal')

    def check_auth_result(self):
        """定時檢查背景執行緒的認證結果"""
        try:
            # 非阻塞檢查 queue
            result = self.result_queue.get_nowait()

            if result[0] == 'success':
                messagebox.showinfo(
                    "✅ 認證成功",
                    "Token 已成功獲取並保存！\n\n"
                    "程式現在可以正常使用了。"
                )
                self.config_saved = True
                self.root.quit()
                self.root.destroy()
            else:
                messagebox.showerror("❌ 認證失敗",
                    f"認證過程發生錯誤：\n\n{result[1]}\n\n"
                    "請確認：\n"
                    "1. URL 是否完整複製\n"
                    "2. 授權碼是否還有效（30秒內）\n"
                    "3. 網路連線是否正常\n\n"
                    "請點擊「開啟瀏覽器進行認證」重試。")
                self.complete_btn.config(state='normal', text="✅ 完成認證")
                self.callback_entry.config(state='normal')

        except queue.Empty:
            # 還沒有結果，繼續等待
            self.root.after(100, self.check_auth_result)

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
    import tkinter as tk
    from tkinter import messagebox

    config_manager = ConfigManager()

    # 檢查配置檔案是否存在
    if not config_manager.config_exists():
        print("⚙️ 首次運行，啟動配置設定...")
        setup_window = OAuthSetupWindow()
        success = setup_window.run()

        if success:
            config = config_manager.load_config()
            return config, True
        else:
            print("❌ 用戶取消設定，程式退出")
            return None, False

    # 配置存在，檢查 Token
    if not config_manager.has_valid_token():
        print("⚠️ Token 不存在，需要重新認證...")
        setup_window = OAuthSetupWindow()
        success = setup_window.run()

        if success:
            config = config_manager.load_config()
            return config, True
        else:
            print("❌ 用戶取消設定，程式退出")
            return None, False

    # 配置和 Token 都存在，檢查 Token 是否有效
    is_valid, remaining_hours, expiry_time = config_manager.is_token_valid(buffer_days=1)

    if not is_valid:
        # Token 即將過期或已過期
        expiry_info = config_manager.get_token_expiry_info()

        if remaining_hours < 0:
            # 已過期 - 自動刪除並重新認證（不詢問）
            print(f"❌ Token 已過期")
            print(expiry_info)
            config_manager.delete_token()
            print("⚠️ 需要重新認證...")

            setup_window = OAuthSetupWindow()
            success = setup_window.run()

            if success:
                config = config_manager.load_config()
                return config, True
            else:
                print("❌ 用戶取消設定，程式退出")
                return None, False

        else:
            # 即將過期 - 詢問用戶是否重新認證
            print(f"⚠️ Token 即將過期")
            print(expiry_info)

            # 創建臨時視窗來顯示對話框
            temp_root = tk.Tk()
            temp_root.withdraw()  # 隱藏主視窗

            # 顯示詢問對話框
            response = messagebox.askyesno(
                "⚠️ Token 即將過期",
                f"{expiry_info}\n\n"
                "建議現在重新認證以避免後續錯誤。\n\n"
                "是否立即重新認證？",
                icon='warning'
            )

            temp_root.destroy()

            if response:
                # 用戶選擇重新認證
                print("🔄 用戶選擇重新認證...")
                config_manager.delete_token()

                setup_window = OAuthSetupWindow()
                success = setup_window.run()

                if success:
                    config = config_manager.load_config()
                    return config, True
                else:
                    print("❌ 用戶取消設定，程式退出")
                    return None, False
            else:
                # 用戶選擇稍後再說
                print("⚠️ 用戶選擇繼續使用（Token 可能在使用時失效）")
                config = config_manager.load_config()
                return config, True

    # Token 有效
    config = config_manager.load_config()
    if config:
        print("✅ 已載入現有配置和 Token")
        # 顯示剩餘時間（可選）
        days = remaining_hours / 24
        print(f"📅 Token 剩餘有效期：{days:.1f} 天")
        return config, True
    else:
        print("❌ 載入配置失敗")
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