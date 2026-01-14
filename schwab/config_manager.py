"""
完整的配置管理模組 - 基於實際的 schwabdev 3.0.0 (PyPI 版本)

實際資料庫結構（已確認）：
表名：schwabdev
欄位：
  - access_token_issued (TEXT)
  - refresh_token_issued (TEXT)
  - access_token (TEXT)
  - refresh_token (TEXT)
  - id_token (TEXT)
  - expires_in (INTEGER)
  - token_type (TEXT)
  - scope (TEXT)
"""
import sqlite3
import os
import sys
import webbrowser
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import schwabdev
import threading
import queue
import builtins
from datetime import datetime, timezone, timedelta

# 🔥 導入工具模組（從上層目錄）
# 如果 config_manager.py 在 schwab/ 目錄下
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_resource_path

class ConfigManager:
    """配置管理器 - 處理 API 憑證的存儲和讀取"""

    def __init__(self):
        # 🔥 修改：統一在 schwab 資料夾中存放檔案
        if getattr(sys, 'frozen', False):
            # 打包後：在 .exe 同層創建 schwab 資料夾
            exe_dir = os.path.dirname(sys.executable)
            self.base_path = os.path.join(exe_dir, 'schwab')
        else:
            # 開發環境：config_manager.py 所在目錄（已經是 schwab/）
            current_file = os.path.abspath(__file__)
            self.base_path = os.path.dirname(current_file)

        # 🔥 確保 schwab 資料夾存在
        os.makedirs(self.base_path, exist_ok=True)

        self.env_path = os.path.join(self.base_path, '.env')
        self.tokens_path = os.path.join(self.base_path, 'tokens.db')

        # Token 驗證快取
        self._last_validation_time = None
        self._last_validation_result = None

        print(f"📁 schwab 資料夾: {self.base_path}")
        print(f"📁 .env 路徑: {self.env_path}")
        print(f"📁 tokens.db 路徑: {self.tokens_path}")
        print(f"📁 .env 存在: {os.path.exists(self.env_path)}")
        print(f"📁 tokens.db 存在: {os.path.exists(self.tokens_path)}")


    def is_token_valid_fast(self, buffer_hours=24):
        """
        快速檢查 Token 是否有效

        基於實際結構：
        SELECT refresh_token_issued FROM schwabdev

        Returns:
            (is_valid, remaining_hours, expiry_time, status)
        """
        try:
            if not os.path.exists(self.tokens_path):
                return False, 0, None, 'missing'

            conn = sqlite3.connect(self.tokens_path)
            cursor = conn.cursor()

            # 🔥 正確查詢：基於實際資料庫結構
            cursor.execute("SELECT refresh_token_issued FROM schwabdev LIMIT 1")
            result = cursor.fetchone()
            conn.close()

            if not result:
                print("⚠️ 找不到 token 記錄")
                return False, 0, None, 'missing'

            # refresh_token_issued 是 ISO 格式字串
            issued_time_str = result[0]
            issued_time = datetime.fromisoformat(issued_time_str)

            # Schwab Refresh Token 有效期是 7 天
            expiry_time = issued_time + timedelta(days=7)

            # 計算剩餘時間
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

            print(f"✓ Token 狀態: {status}（剩餘 {remaining_hours / 24:.1f} 天）")
            return is_valid, remaining_hours, expiry_time, status

        except Exception as e:
            print(f"⚠️ 檢查 Token 時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return False, 0, None, 'error'

    def should_validate_with_api(self):
        """智慧判斷是否需要調用 API 驗證"""
        # 檢查快取（1小時內）
        if self._last_validation_time:
            time_since_last = (datetime.now() - self._last_validation_time).total_seconds()
            if time_since_last < 3600:
                print(f"✓ 使用快取的驗證結果（{int(time_since_last / 60)} 分鐘前驗證）")
                return False, self._last_validation_result

        # 快速檢查 Token 狀態
        is_valid, remaining_hours, _, status = self.is_token_valid_fast(buffer_hours=24)

        if status == 'expired' or status == 'missing':
            print(f"⚠️ Token {status}，需要重新認證")
            return True, None

        if status == 'expiring_soon':
            print(f"⚠️ Token 即將過期（剩餘 {remaining_hours:.1f} 小時），執行 API 驗證")
            return True, None

        if remaining_hours > 72:  # > 3 天
            print(f"✓ Token 時間充足（剩餘 {remaining_hours / 24:.1f} 天），但仍需執行 API 驗證確認")
            return True, None  # 👈 改成 True，強制執行 API 驗證

        print(f"🔍 Token 剩餘 {remaining_hours / 24:.1f} 天，執行 API 驗證確認")
        return True, None

    def update_validation_cache(self, result):
        """更新驗證快取"""
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
                print(f"📄 .env 內容長度: {len(content)} 字數")

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
            return config if config else None
        except Exception as e:
            print(f"❌ 讀取配置失敗: {e}")
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

            if os.path.exists(self.env_path):
                file_size = os.path.getsize(self.env_path)
                print(f"✅ 配置已保存，檔案大小: {file_size} bytes")
                return True
            else:
                print(f"❌ 保存後檔案不存在！")
                return False

        except Exception as e:
            print(f"❌ 保存配置失敗: {e}")
            return False

    def has_valid_token(self):
        """檢查是否有有效的 token"""
        exists = os.path.exists(self.tokens_path)
        print(f"🔍 檢查 tokens.db 是否存在: {exists}")
        if exists:
            file_size = os.path.getsize(self.tokens_path)
            print(f"   檔案大小: {file_size} bytes")
        return exists

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
        """獲取 Token 過期資訊"""
        is_valid, remaining_hours, expiry_time, status = self.is_token_valid_fast(buffer_hours=0)

        if expiry_time is None:
            return "Token 不存在或無法讀取"

        if remaining_hours < 0:
            hours_ago = abs(remaining_hours)
            return f"Token 已過期（{hours_ago:.1f} 小時前過期）"
        elif remaining_hours < 24:
            return f"Token 將在 {remaining_hours:.1f} 小時後過期\n過期時間：{expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            days = remaining_hours / 24
            return f"Token 將在 {days:.1f} 天後過期\n過期時間：{expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"


def verify_token_db_structure(tokens_path):
    """
    驗證 Token DB 是否有正確的表格結構

    基於實際的 schwabdev 3.0.0 (PyPI 版本)
    表名：schwabdev
    欄位：access_token_issued, refresh_token_issued, access_token, refresh_token,
          id_token, expires_in, token_type, scope

    Returns:
        bool: True 表示結構正確，False 表示需要重建
    """
    try:
        if not os.path.exists(tokens_path):
            print("⚠️ tokens.db 檔案不存在")
            return False

        # 檢查檔案大小
        file_size = os.path.getsize(tokens_path)
        if file_size == 0:
            print("⚠️ tokens.db 檔案大小為 0")
            return False

        print(f"✓ tokens.db 檔案存在，大小: {file_size} bytes")

        conn = sqlite3.connect(tokens_path)
        cursor = conn.cursor()

        # 步驟 1: 檢查 schwabdev 表格是否存在
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schwabdev'"
        )
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            print("⚠️ schwabdev 表格不存在")
            conn.close()
            return False

        print("✓ schwabdev 表格存在")

        # 步驟 2: 檢查表格結構
        cursor.execute("PRAGMA table_info(schwabdev)")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        print(f"✓ schwabdev 欄位: {column_names}")

        # 步驟 3: 驗證必要欄位
        required_columns = ['access_token', 'refresh_token', 'refresh_token_issued']
        missing_columns = [col for col in required_columns if col not in column_names]

        if missing_columns:
            print(f"⚠️ 缺少必要欄位: {missing_columns}")
            conn.close()
            return False

        # 步驟 4: 檢查是否有資料
        cursor.execute("SELECT COUNT(*) FROM schwabdev")
        count = cursor.fetchone()[0]

        if count == 0:
            print("⚠️ schwabdev 表格是空的")
            conn.close()
            return False

        print(f"✓ 找到 {count} 筆記錄")

        # 步驟 5: 檢查 refresh_token 的值是否為空
        cursor.execute("SELECT refresh_token, refresh_token_issued FROM schwabdev LIMIT 1")
        result = cursor.fetchone()

        if not result[0] or result[0].strip() == '':
            print("⚠️ refresh_token 的值是空的")
            conn.close()
            return False

        print(f"✓ refresh_token 有效（長度: {len(result[0])}）")

        # 步驟 6: 檢查發行時間
        issued_time_str = result[1]
        issued_time = datetime.fromisoformat(issued_time_str)
        expiry_time = issued_time + timedelta(days=7)

        print(f"✓ Token 發行時間: {issued_time}")
        print(f"✓ Token 過期時間: {expiry_time}")

        conn.close()
        print("✅ Token DB 結構驗證通過！")
        return True

    except Exception as e:
        print(f"❌ 檢查 Token DB 結構時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_token_db(tokens_path):
    """完整診斷 Token DB（調試用）"""
    print("\n" + "="*60)
    print("🔍 Token DB 完整診斷")
    print("="*60)

    try:
        if not os.path.exists(tokens_path):
            print("❌ tokens.db 不存在")
            return

        file_size = os.path.getsize(tokens_path)
        print(f"📁 檔案路徑: {tokens_path}")
        print(f"📁 檔案大小: {file_size} bytes")

        conn = sqlite3.connect(tokens_path)
        cursor = conn.cursor()

        # 列出所有表格
        print("\n📊 資料庫表格:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            print(f"\n  表格: {table_name}")

            # 顯示表格結構
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"  欄位:")
            for col in columns:
                print(f"    - {col[1]} ({col[2]})")

            # 顯示記錄數
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  記錄數: {count}")

            # 顯示所有記錄
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            if rows:
                print(f"  記錄內容:")
                for i, row in enumerate(rows, 1):
                    print(f"\n    記錄 {i}:")
                    for j, col in enumerate(columns):
                        col_name = col[1]
                        col_value = row[j]

                        # 特殊處理
                        if col_name == 'value' and isinstance(col_value, str):
                            display_value = f"{col_value[:20]}...{col_value[-10:]}" if len(col_value) > 30 else col_value
                        elif col_name in ['creation', 'expiration']:
                            display_value = f"{col_value} ({datetime.fromtimestamp(col_value, tz=timezone.utc)})"
                        else:
                            display_value = col_value

                        print(f"      {col_name}: {display_value}")

        conn.close()
        print("\n" + "="*60)

    except Exception as e:
        print(f"❌ 診斷過程發生錯誤: {e}")
        import traceback
        traceback.print_exc()


class OAuthSetupWindow:
    """OAuth 認證設定視窗"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("API 認證設定")
        self.root.geometry("1200x1000")
        self.root.resizable(True, True)
        self.root.configure(bg='#1a1a1a')
        self.root.minsize(1200, 1000)

        # 🔥 設定視窗圖示
        self._set_window_icon()

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

    def _set_window_icon(self):
        """設定視窗圖示（使用工具函數）"""
        try:
            # 🔥 使用工具函數取得圖示路徑
            icon_path = get_resource_path('logo.ico')

            if os.path.exists(icon_path):
                self.root.iconbitmap(icon_path)
                print(f"✓ OAuth 視窗圖示已載入: {icon_path}")
            else:
                print(f"⚠️ 找不到圖示檔案: {icon_path}")

        except Exception as e:
            print(f"⚠️ 載入 OAuth 視窗圖示時發生錯誤: {e}")

    def setup_ui(self):
        """完整的 UI 設定方法 - App Key 和 App Secret 並排顯示"""

        # 主框架
        main_frame = tk.Frame(self.root, bg='#2d2d2d')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 標題
        title_label = tk.Label(
            main_frame,
            text="📊 Schwab API 認證設定",
            font=('微軟正黑體', 16, 'bold'),
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

        # 輸入框容器（使用 pack 佈局實現左右並排）
        input_frame = tk.Frame(step1_frame, bg='#2d2d2d')
        input_frame.pack(pady=15, padx=20, fill=tk.X)

        # 左側 - App Key
        left_frame = tk.Frame(input_frame, bg='#2d2d2d')
        left_frame.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 10))

        tk.Label(
            left_frame,
            text="🔑 App Key:",
            font=('微軟正黑體', 10, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(anchor=tk.W, pady=(0, 5))

        self.app_key_entry = tk.Entry(
            left_frame,
            font=('Consolas', 10),
            bg='#3d3d3d',
            fg='#ffffff',
            insertbackground='#00d4aa',
            relief='flat',
            bd=2
        )
        self.app_key_entry.pack(fill=tk.X, ipady=8)

        # 右側 - App Secret
        right_frame = tk.Frame(input_frame, bg='#2d2d2d')
        right_frame.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(10, 0))

        tk.Label(
            right_frame,
            text="🔐 App Secret:",
            font=('微軟正黑體', 10, 'bold'),
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(anchor=tk.W, pady=(0, 5))

        self.app_secret_entry = tk.Entry(
            right_frame,
            font=('Consolas', 10),
            bg='#3d3d3d',
            fg='#ffffff',
            insertbackground='#00d4aa',
            show="●",
            relief='flat',
            bd=2
        )
        self.app_secret_entry.pack(fill=tk.X, ipady=8)

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
            width=25,
            height=1,
            relief='flat',
            cursor='hand2'
        )
        self.generate_btn.pack()

        # === 步驟 2: 在瀏覽器中完成認證 ===
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

        # 醒目的紅色警告框
        warning_frame = tk.Frame(step2_frame, bg='#ff4757', relief='solid', bd=3)
        warning_frame.pack(fill=tk.X, pady=(8, 5), padx=20)

        tk.Label(
            warning_frame,
            text="⏰ 重要提醒：授權碼只有 30 秒有效期！",
            font=('微軟正黑體', 12, 'bold'),
            fg='white',
            bg='#ff4757'
        ).pack(pady=6)

        # 說明文字
        instruction_text = """
        點擊下方按鈕後，瀏覽器將開啟 Schwab 認證頁面。

        請在瀏覽器中：
          1. 登入您的 Schwab 帳號
          2. 授權應用程式存取權限
          3. 完成後會跳轉到一個「無法連接」的頁面（這是正常的！）
          4. ⚡ 立即複製瀏覽器網址列中的完整 URL
          5. ⚡ 快速回到此視窗並貼到步驟 3（必須在 30 秒內完成）
                """

        tk.Label(
            step2_frame,
            text=instruction_text,
            font=('微軟正黑體', 11),
            justify=tk.LEFT,
            fg='#ffffff',
            bg='#2d2d2d'
        ).pack(pady=5, padx=20)

        # 授權 URL 顯示框
        url_display_frame = tk.Frame(step2_frame, bg='#2d2d2d')
        url_display_frame.pack(fill=tk.X, pady=5, padx=20)

        tk.Label(
            url_display_frame,
            text="授權連結：",
            font=('微軟正黑體', 8),
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
            width=25,
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
                f"App Key 長度不正確！\n當前：{len(app_key)} 字數\n正確：32 或 48 字數")
            return

        if len(app_secret) not in (16, 64):
            messagebox.showerror("❌ 格式錯誤",
                f"App Secret 長度不正確！\n當前：{len(app_secret)} 字數\n正確：16 或 64 字數")
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
        """開啟瀏覽器 - 先顯示說明再開啟"""
        if self.auth_url:
            # 🔥 防止重複點擊
            if self.auth_thread and self.auth_thread.is_alive():
                messagebox.showwarning(
                    "⚠️ 請稍候",
                    "認證程序正在進行中，請勿重複點擊。"
                )
                return

            # 🔥 先顯示說明（阻塞式，等待用戶按「確定」）
            messagebox.showinfo(
                "🌐 即將開啟瀏覽器",
                "點擊「確定」後，瀏覽器將開啟 Schwab 認證頁面。\n\n"
                "請在瀏覽器中完成認證。\n\n"
                "⏰ 重要：授權碼只有 30 秒有效期！\n"
                "（30 秒從完成授權、跳轉到無法連接的頁面時才開始計算）\n\n"
                "完成後：\n"
                "1. 立即複製瀏覽器網址列的完整 URL\n"
                "2. 快速回到此視窗\n"
                "3. 貼到「步驟 3」的輸入框中並點擊「完成認證」"
            )

            # 🔥 用戶按下「確定」後，才開啟瀏覽器
            webbrowser.open(self.auth_url)
            print("🌐 瀏覽器已開啟")

            # 保存配置
            config_data = {
                'app_key': self.app_key,
                'app_secret': self.app_secret
            }
            self.config_manager.save_config(config_data)

            # 🔥 禁用按鈕，防止重複點擊
            self.browser_btn.config(state='disabled', text="🌐 瀏覽器已開啟")

            # 啟動背景執行緒
            self.start_auth_thread()

            # 啟用輸入和按鈕
            self.callback_entry.config(state='normal')
            self.complete_btn.config(state='normal')

    def start_auth_thread(self):
        """在背景執行緒啟動 schwabdev Client - 改進錯誤處理版"""

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

                    # 🔥 關鍵改進：使用 try-except 捕獲所有可能的錯誤
                    try:
                        client = schwabdev.Client(
                            app_key=self.app_key,
                            app_secret=self.app_secret,
                            callback_url="https://127.0.0.1",
                            tokens_db=tokens_file_path,
                            encryption=None,
                            timeout=30,
                            call_on_auth=None
                        )

                        print("✅ schwabdev Client 初始化成功！")
                        print(f"✅ Token 已保存為 .db 格式: {tokens_file_path}")
                        self.result_queue.put(('success', None))

                    except AttributeError as e:
                        # 🔥 捕獲 schwabdev 內部的 AttributeError
                        error_msg = str(e)
                        print(f"❌ schwabdev 內部錯誤: {error_msg}")

                        # 判斷是否為授權碼過期導致的錯誤
                        if "'bool' object has no attribute 'get'" in error_msg:
                            friendly_error = (
                                "授權碼已過期或無效！\n\n"
                                "常見原因：\n"
                                "1. 從瀏覽器複製 URL 到貼上花費超過 30 秒\n"
                                "2. 重複使用已使用過的 URL\n"
                                "3. URL 複製不完整\n\n"
                                "解決方法：\n"
                                "• 點擊「開啟瀏覽器進行認證」重新開始\n"
                                "• 在瀏覽器完成授權後，立即複製並貼上完整的 URL\n"
                                "• 確保在 30 秒內完成貼上動作"
                            )
                            self.result_queue.put(('error', friendly_error))
                        else:
                            self.result_queue.put(('error', f"AttributeError: {error_msg}"))

                    except Exception as e:
                        # 🔥 捕獲其他所有錯誤
                        error_msg = str(e)
                        print(f"❌ 認證過程發生錯誤: {error_msg}")

                        # 檢查是否為授權碼過期錯誤
                        if "AuthorizationCode has expired" in error_msg or \
                                "Bad authorization code" in error_msg:
                            friendly_error = (
                                "授權碼已過期！\n\n"
                                "Schwab 的授權碼只有 30 秒有效期。\n\n"
                                "請重試，並在瀏覽器完成授權後\n"
                                "立即複製並貼上完整的 URL。"
                            )
                            self.result_queue.put(('error', friendly_error))
                        elif "invalid_request" in error_msg or \
                                "unsupported_token_type" in error_msg:
                            friendly_error = (
                                "授權請求無效！\n\n"
                                "可能原因：\n"
                                "• 授權碼已過期（超過 30 秒）\n"
                                "• URL 複製不完整\n"
                                "• App Key 或 App Secret 不正確\n\n"
                                "請重試，並確保：\n"
                                "1. 快速複製完整的 URL\n"
                                "2. 在 30 秒內貼上\n"
                                "3. App 憑證正確無誤"
                            )
                            self.result_queue.put(('error', friendly_error))
                        else:
                            self.result_queue.put(('error', error_msg))

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
        """完成認證 - 簡化版"""
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

            # 把 URL 放入 queue
            self.callback_queue.put(returned_url)

            # 禁用按鈕
            self.complete_btn.config(state='disabled', text="⏳ 處理中...")
            self.callback_entry.config(state='disabled')

            # 啟動檢查結果
            self.root.after(100, self.check_auth_result)

        except Exception as e:
            messagebox.showerror("❌ 錯誤", f"發生錯誤：\n{str(e)}")
            self.complete_btn.config(state='normal', text="✅ 完成認證")
            self.callback_entry.config(state='normal')

    def check_auth_result(self):
        """檢查認證結果 - 改進錯誤訊息版"""
        try:
            result = self.result_queue.get_nowait()

            if result[0] == 'success':
                messagebox.showinfo(
                    "✅ 認證成功",
                    "Schwab API認證成功並保存！\n\n"
                    "程式現在可以正常使用了。"
                )
                self.config_saved = True
                self.root.quit()
                self.root.destroy()
            else:
                # 🔥 顯示友善的錯誤訊息
                error_msg = result[1]

                messagebox.showerror(
                    "❌ 認證失敗",
                    f"{error_msg}\n\n"
                    "⏰ 提醒：授權碼只有 30 秒有效期！\n"
                    "請重新點擊「開啟瀏覽器進行認證」，\n"
                    "並在 30 秒內完成貼上動作。"
                )

                # 🔥 恢復按鈕狀態，允許重試
                self.complete_btn.config(state='normal', text="✅ 完成認證")
                self.callback_entry.config(state='normal')
                self.browser_btn.config(state='normal', text="🌐 開啟瀏覽器進行認證")

        except queue.Empty:
            # 還沒有結果，繼續等待
            self.root.after(100, self.check_auth_result)

    def cancel_setup(self):
        """取消設定"""
        result = messagebox.askyesno(
            "確認退出",
            "尚未完成設定，確定要退出嗎？"
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

        print("🔄 準備顯示對話框...")
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
        print(f"✓ 用戶選擇: {'是' if response else '否'}")

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


# 測試程式碼
if __name__ == "__main__":
    cm = ConfigManager()

    print("\n" + "=" * 60)
    print("測試 1: 驗證 Token DB 結構")
    print("=" * 60)
    result = verify_token_db_structure(cm.tokens_path)
    print(f"\n結果: {'✅ 通過' if result else '❌ 失敗'}")

    print("\n" + "=" * 60)
    print("測試 2: 快速檢查 Token 有效性")
    print("=" * 60)
    is_valid, hours, expiry, status = cm.is_token_valid_fast()
    print(f"有效: {is_valid}")
    print(f"狀態: {status}")
    if hours > 0:
        print(f"剩餘時間: {hours / 24:.1f} 天")
    else:
        print(f"已過期: {abs(hours) / 24:.1f} 天前")

# 測試用
if __name__ == "__main__":
    config, should_continue = check_and_setup_config()