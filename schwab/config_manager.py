"""
完整的配置管理模組 - 支援 Schwab API 3.0.0 的 .db 格式
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
        # 🔥 修正：確定基礎路徑（打包後和開發環境統一處理）
        if getattr(sys, 'frozen', False):
            # 打包後：exe 所在目錄
            self.base_path = os.path.dirname(sys.executable)
            print(f"🔥 [打包模式] Base path: {self.base_path}")
        else:
            # 開發環境：專案根目錄（schwab 資料夾的上一層）
            current_file = os.path.abspath(__file__)
            self.base_path = os.path.dirname(current_file)  # schwab 資料夾
            print(f"🔥 [開發模式] Base path: {self.base_path}")

        self.env_path = os.path.join(self.base_path, '.env')

        # 🔥 關鍵修改：改用 .db 檔案（Schwab 3.0.0 格式）
        self.tokens_path = os.path.join(self.base_path, 'tokens.db')  # ✅ 改成 .db

        # 🔥 新增：Token 驗證快取
        self._last_validation_time = None
        self._last_validation_result = None

        # 🔥 新增：啟動時顯示路徑資訊
        print(f"📁 .env 路徑: {self.env_path}")
        print(f"📁 tokens.db 路徑: {self.tokens_path}")  # ✅ 顯示 .db
        print(f"📁 .env 存在: {os.path.exists(self.env_path)}")
        print(f"📁 tokens.db 存在: {os.path.exists(self.tokens_path)}")  # ✅ 檢查 .db

    # 🔥 新增方法 1：快速本地檢查
    def is_token_valid_fast(self, buffer_hours=24):
        """
        快速檢查 Token 是否有效（讀取 .db 檔案中的時間戳）

        Returns:
            (is_valid, remaining_hours, expiry_time, status)
            status: 'valid' | 'expiring_soon' | 'expired' | 'missing'
        """
        from datetime import datetime, timezone, timedelta
        import sqlite3

        try:
            if not os.path.exists(self.tokens_path):
                return False, 0, None, 'missing'

            # 🔥 關鍵修改：讀取 SQLite DB 而非 JSON
            conn = sqlite3.connect(self.tokens_path)
            cursor = conn.cursor()

            # 🔥 讀取 refresh_token 的發行時間
            # 假設表結構為 tokens(token_type, token_value, issued_at)
            cursor.execute(
                "SELECT issued_at FROM tokens WHERE token_type = 'refresh_token'"
            )
            result = cursor.fetchone()
            conn.close()

            if not result:
                return False, 0, None, 'missing'

            refresh_issued = result[0]

            # 解析時間（假設存儲為 ISO 格式字串）
            issued_time = datetime.fromisoformat(refresh_issued.replace('Z', '+00:00'))

            # Schwab Refresh Token 有效期是 7 天
            expiry_time = issued_time + timedelta(days=7)
            current_time = datetime.now(timezone.utc)
            remaining_seconds = (expiry_time - current_time).total_seconds()
            remaining_hours = remaining_seconds / 3600

            # 判斷狀態
            if remaining_hours <= 0:
                status = 'expired'
                is_valid = False
            elif remaining_hours < buffer_hours:
                status = 'expiring_soon'
                is_valid = True
            else:
                status = 'valid'
                is_valid = True

            return is_valid, remaining_hours, expiry_time, status

        except Exception as e:
            print(f"⚠️ 檢查 Token 時發生錯誤: {e}")
            return False, 0, None, 'error'

    # 🔥 新增方法 2：智慧判斷是否需要 API 驗證
    def should_validate_with_api(self):
        """
        智慧判斷是否需要調用 API 驗證

        Returns:
            (should_validate, cached_result)
        """
        from datetime import datetime

        # 檢查是否在快取時間內（1小時）
        if self._last_validation_time:
            time_since_last = (datetime.now() - self._last_validation_time).total_seconds()
            if time_since_last < 3600:  # 1 小時內
                print(f"✓ 使用快取的驗證結果（{int(time_since_last / 60)} 分鐘前驗證）")
                return False, self._last_validation_result

        # 快速檢查 Token 狀態
        is_valid, remaining_hours, _, status = self.is_token_valid_fast(buffer_hours=24)

        # 決策邏輯
        if status == 'expired' or status == 'missing':
            print(f"⚠️ Token {status}，需要重新認證")
            return True, None

        if status == 'expiring_soon':
            print(f"⚠️ Token 即將過期（剩餘 {remaining_hours:.1f} 小時），執行 API 驗證")
            return True, None

        if remaining_hours > 72:  # > 3 天
            print(f"✓ Token 狀態良好（剩餘 {remaining_hours / 24:.1f} 天），跳過 API 驗證")
            return False, True

        # 預設：執行驗證
        print(f"🔍 Token 剩餘 {remaining_hours / 24:.1f} 天，執行 API 驗證確認")
        return True, None

    # 🔥 新增方法 3：更新快取
    def update_validation_cache(self, result):
        """更新驗證快取"""
        from datetime import datetime
        self._last_validation_time = datetime.now()
        self._last_validation_result = result

    def config_exists(self):
        """檢查配置檔案是否存在"""
        exists = os.path.exists(self.env_path)
        print(f"🔍 檢查 .env 是否存在: {exists}")
        return exists

    def load_config(self):
        """讀取 .env 配置"""
        try:
            if not os.path.exists(self.env_path):
                print(f"❌ .env 檔案不存在: {self.env_path}")
                return None

            config = {}
            with open(self.env_path, 'r', encoding='utf-8') as f:
                content = f.read()
                print(f"📄 .env 內容長度: {len(content)} 字元")

                for line in content.split('\n'):
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

            print(f"✅ 成功載入配置，包含 {len(config)} 個設定")
            print(f"   - app_key: {'已設定' if 'app_key' in config else '❌ 缺失'}")
            print(f"   - app_secret: {'已設定' if 'app_secret' in config else '❌ 缺失'}")

            return config if config else None
        except Exception as e:
            print(f"❌ 讀取配置失敗: {e}")
            import traceback
            traceback.print_exc()
            return None

    def save_config(self, config_data):
        """保存配置到 .env 檔案"""
        try:
            print(f"💾 正在保存配置到: {self.env_path}")

            with open(self.env_path, 'w', encoding='utf-8') as f:
                f.write("# INCLUDE THIS FILE IN YOUR .gitignore\n\n")
                f.write(f'app_key = "{config_data["app_key"]}"\n')
                f.write(f'app_secret = "{config_data["app_secret"]}"\n')
                f.write(f'callback_url = "https://127.0.0.1"\n')

            # 驗證檔案是否真的被寫入
            if os.path.exists(self.env_path):
                file_size = os.path.getsize(self.env_path)
                print(f"✅ 配置已保存，檔案大小: {file_size} bytes")
                return True
            else:
                print(f"❌ 保存後檔案不存在！")
                return False

        except Exception as e:
            print(f"❌ 保存配置失敗: {e}")
            import traceback
            traceback.print_exc()
            return False

    def has_valid_token(self):
        """檢查是否有有效的 token（.db 格式）"""
        exists = os.path.exists(self.tokens_path)
        print(f"🔍 檢查 tokens.db 是否存在: {exists}")  # ✅ 改成 .db
        if exists:
            file_size = os.path.getsize(self.tokens_path)
            print(f"   檔案大小: {file_size} bytes")
        return exists

    def is_token_valid(self, buffer_days=1):
        """檢查 Refresh Token 是否仍然有效（從 .db 讀取）"""
        from datetime import datetime, timezone, timedelta
        import sqlite3

        try:
            if not os.path.exists(self.tokens_path):
                print("❌ tokens.db 不存在")
                return False, 0, None

            # 🔥 關鍵修改：從 SQLite 讀取
            conn = sqlite3.connect(self.tokens_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT issued_at FROM tokens WHERE token_type = 'refresh_token'"
            )
            result = cursor.fetchone()
            conn.close()

            if not result:
                print("❌ 找不到 refresh_token 記錄")
                return False, 0, None

            refresh_issued = result[0]

            # 解析 ISO 格式時間
            issued_time = datetime.fromisoformat(refresh_issued.replace('Z', '+00:00'))

            # Schwab Refresh Token 有效期是 7 天
            expiry_time = issued_time + timedelta(days=7)

            # 計算剩餘時間
            current_time = datetime.now(timezone.utc)
            remaining_seconds = (expiry_time - current_time).total_seconds()
            remaining_hours = remaining_seconds / 3600
            remaining_days = remaining_seconds / 86400

            # 判斷是否有效（剩餘時間大於緩衝天數）
            is_valid = remaining_days > buffer_days

            print(f"📅 Token 狀態:")
            print(f"   發行時間: {issued_time}")
            print(f"   過期時間: {expiry_time}")
            print(f"   剩餘天數: {remaining_days:.1f}")
            print(f"   是否有效: {is_valid}")

            return is_valid, remaining_hours, expiry_time

        except Exception as e:
            print(f"❌ 檢查 Token 時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return False, 0, None

    def delete_token(self):
        """安全刪除 Token 檔案（.db）"""
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
        """在背景執行緒啟動 schwabdev Client - 使用實際參數名稱"""

        def auth_worker():
            original_input = builtins.input
            original_webbrowser_open = webbrowser.open

            try:
                print("🔄 背景執行緒：正在初始化 schwabdev Client...")

                def custom_input(prompt=""):
                    if prompt:
                        print(prompt, end='', flush=True)
                    url = self.callback_queue.get()
                    print(url)
                    return url

                def disabled_webbrowser_open(url, new=0, autoraise=True):
                    print(f"🚫 已禁用自動開啟瀏覽器")
                    return True

                builtins.input = custom_input
                webbrowser.open = disabled_webbrowser_open

                try:
                    tokens_file_path = self.config_manager.tokens_path
                    print(f"📁 Token 將保存至: {tokens_file_path}")

                    # 🔥 使用實際存在的參數名稱
                    client = schwabdev.Client(
                        app_key=self.app_key,
                        app_secret=self.app_secret,
                        callback_url="https://127.0.0.1",
                        tokens_db=tokens_file_path,
                        encryption=None,
                        timeout=30,
                        call_on_auth=None  # ✅ 使用 IDE 提示的實際參數
                    )

                    print("✅ schwabdev Client 初始化成功！")
                    print(f"✅ Token 已保存為 .db 格式: {tokens_file_path}")
                    self.result_queue.put(('success', None))

                finally:
                    builtins.input = original_input
                    webbrowser.open = original_webbrowser_open

            except Exception as e:
                print(f"❌ 背景執行緒錯誤: {e}")
                import traceback
                traceback.print_exc()
                self.result_queue.put(('error', str(e)))

                builtins.input = original_input
                webbrowser.open = original_webbrowser_open

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
                    "Token 已成功獲取並保存為 .db 格式！\n\n"
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
    檢查配置並在需要時啟動設定視窗（修復版）
    Returns: (config_data, should_continue)
    """
    import tkinter as tk
    from tkinter import messagebox

    config_manager = ConfigManager()

    # 步驟 1: 檢查配置檔案
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

    # 步驟 2: 檢查 Token 檔案是否存在
    if not config_manager.has_valid_token():
        print("⚠️ Token 檔案不存在，需要重新認證...")
        setup_window = OAuthSetupWindow()
        success = setup_window.run()

        if success:
            config = config_manager.load_config()
            return config, True
        else:
            print("❌ 用戶取消設定，程式退出")
            return None, False

    # 步驟 3: 載入配置
    config = config_manager.load_config()
    if not config:
        print("❌ 載入配置失敗")
        return None, False

    # 🔥 步驟 4: 檢查 Token DB 是否有正確的表格結構
    if not verify_token_db_structure(config_manager.tokens_path):
        print("⚠️ Token 資料庫結構不正確，需要重新認證...")

        temp_root = tk.Tk()
        temp_root.withdraw()

        response = messagebox.askyesno(
            "⚠️ Token 資料異常",
            "檢測到 Token 資料庫結構不完整。\n\n"
            "這可能是因為：\n"
            "• 上次認證未完成\n"
            "• 檔案已損壞\n\n"
            "需要重新進行認證，是否繼續？",
            icon='warning'
        )

        temp_root.destroy()

        if response:
            # 刪除舊的 token 檔案
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
            print("❌ 用戶拒絕重新認證，程式退出")
            return None, False

    # 🔥 步驟 5: 智慧 Token 驗證（優化核心）
    should_validate, cached_result = config_manager.should_validate_with_api()

    if not should_validate:
        # 使用快取或本地檢查結果
        is_valid, remaining_hours, expiry_time, status = config_manager.is_token_valid_fast()
        if is_valid:
            print(f"✅ Token 有效（剩餘 {remaining_hours / 24:.1f} 天）")
            return config, True

    # 步驟 6: 需要 API 驗證時才執行
    print("🔍 執行 API 驗證...")
    token_works = test_schwab_token(config, config_manager.tokens_path)

    # 更新快取
    config_manager.update_validation_cache(token_works)

    if token_works:
        print("✅ Token 驗證成功")
        return config, True
    else:
        print("❌ Token 驗證失敗")

        temp_root = tk.Tk()
        temp_root.withdraw()

        response = messagebox.askyesno(
            "❌ Token 認證失敗",
            "Schwab 伺服器拒絕了你的 Token。\n\n"
            "可能原因：\n"
            "• Token 已被伺服器撤銷\n"
            "• 帳號在其他地方登入\n"
            "• Schwab 系統維護\n\n"
            "是否立即重新認證？",
            icon='error'
        )

        temp_root.destroy()

        if response:
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
            print("⚠️ 用戶選擇繼續（可能會在使用時失敗）")
            return config, True


def test_schwab_token(config, tokens_path):
    """實際測試 Schwab Token 是否可用"""
    try:
        import schwabdev

        client = schwabdev.Client(
            app_key=config['app_key'],
            app_secret=config['app_secret'],
            callback_url="https://127.0.0.1",
            tokens_db=tokens_path,
            encryption=None,
            timeout=30,
            call_on_auth=None  # ✅ 使用實際參數
        )

        try:
            response = client.market_hours(['equity'])

            if hasattr(response, 'status_code'):
                if response.status_code == 200:
                    print("✓ Token 驗證：API 調用成功")
                    return True
                elif response.status_code == 401:
                    print("✗ Token 驗證：認證失敗 (401)")
                    return False
                else:
                    print(f"✗ Token 驗證：API 返回狀態碼 {response.status_code}")
                    return False
            else:
                print("✓ Token 驗證：API 回應正常")
                return True

        except Exception as api_error:
            error_str = str(api_error).lower()
            if 'refresh_token_authentication_error' in error_str or \
                    'unsupported_token_type' in error_str or \
                    '401' in error_str:
                print(f"✗ Token 驗證失敗：{api_error}")
                return False
            else:
                print(f"⚠️ Token 驗證時發生錯誤（假設有效）：{api_error}")
                return True

    except Exception as e:
        print(f"❌ 無法創建 Schwab 客戶端：{e}")
        return False


def verify_token_db_structure(tokens_path):
    """
    驗證 Token DB 是否有正確的表格結構

    Returns:
        bool: True 表示結構正確，False 表示需要重建
    """
    import sqlite3

    try:
        if not os.path.exists(tokens_path):
            return False

        conn = sqlite3.connect(tokens_path)
        cursor = conn.cursor()

        # 檢查 tokens 表格是否存在
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tokens'"
        )
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            print("⚠️ tokens 表格不存在")
            conn.close()
            return False

        # 檢查是否有 refresh_token 記錄
        cursor.execute(
            "SELECT COUNT(*) FROM tokens WHERE token_type = 'refresh_token'"
        )
        token_count = cursor.fetchone()[0]

        conn.close()

        if token_count == 0:
            print("⚠️ 找不到 refresh_token 記錄")
            return False

        print(f"✓ Token DB 結構正確，包含 {token_count} 筆 refresh_token")
        return True

    except Exception as e:
        print(f"⚠️ 檢查 Token DB 結構時發生錯誤: {e}")
        return False

# 測試用
if __name__ == "__main__":
    config, should_continue = check_and_setup_config()

    if should_continue:
        print("\n✅ 配置完成！")
        print(f"App Key: {config['app_key'][:10]}...")
        print("系統已就緒，可以開始使用！")
    else:
        print("程式已退出")