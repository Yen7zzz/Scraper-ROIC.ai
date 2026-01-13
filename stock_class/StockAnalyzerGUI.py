import warnings
import sys
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import asyncio
import os
from datetime import datetime
import time

# 🔥 關鍵修復：抑制警告
warnings.filterwarnings('ignore', category=ResourceWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=DeprecationWarning)

# 🔥 修復：Windows 事件循環策略設定
if sys.platform == 'win32':
    # 強制使用 ProactorEventLoop
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        print("✓ GUI: 已設定 Windows ProactorEventLoop 策略")
    except AttributeError:
        # Python 3.7 或更早版本
        pass

# 在事件循環設定完成後才導入其他模組
from excel_template.fundamental_excel_template import Fundamental_Excel_Template_Base64
# from excel_template.option_chain_excel_template import Option_Chain_Excel_Template_Base64
from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess
from stock_class.StockManager import StockManager
from stock_class.StockValidator import StockValidator

# ====== GUI 部分 ======
class StockAnalyzerGUI:
    def __init__(self, config=None):
        self.root = tk.Tk()
        self.root.title("財報數據自動化程式 v3.0")
        self.root.geometry("1400x1000")
        self.root.configure(bg='#1a1a1a')  # 深色背景
        self.root.minsize(1200, 900)
        # 🔥 綁定視窗關閉事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # 🔥 綁定 Ctrl+C 處理
        self.root.bind('<Control-c>', lambda e: self.on_closing())
        # 保存配置
        self.config = config  # 👈 儲存配置

        # 設定樣式
        self.style = ttk.Style()
        self.style.theme_use('clam')

        # 自訂顏色主題
        self.setup_custom_styles()

        # 變數
        self.stocks_var = tk.StringVar()
        self.output_folder_var = tk.StringVar(value=os.getcwd())
        self.is_running = False

        # 新增：模板選擇變數
        self.stock_analysis_var = tk.BooleanVar(value=True)  # 預設勾選
        self.option_analysis_var = tk.BooleanVar(value=True)  # 預設勾選

        # 🔥 新增：追蹤當前運行的資源（用於強制清理）
        self.current_scraper = None
        self.current_manager = None
        self.cleanup_lock = threading.Lock()  # 防止重複清理

        self.setup_ui()

        # 用於追蹤當前運行的任務和線程
        self.current_task = None
        self.current_thread = None
        self.event_loop = None

    def on_closing(self):
        """處理視窗關閉事件"""
        if self.is_running:
            # 如果正在運行，先停止
            response = messagebox.askyesno(
                "⚠️ 確認退出",
                "數據自動化正在運行中，確定要退出嗎？\n\n"
                "這將強制停止所有任務並清理資源。"
            )
            if response:
                print("\n⚠️ 用戶請求退出，開始清理資源...")
                self.stop_analysis()  # 調用停止邏輯

                # 🔥 給更多時間讓清理完成
                import time
                time.sleep(3)  # 增加到 3 秒

                self.root.destroy()
        else:
            # 沒有運行，直接關閉
            self.root.destroy()

    def setup_custom_styles(self):
        """設定現代化樣式 - 優化字體和配色"""

        # ===== 🎨 統一字體配置 =====
        # 🔥 關鍵改進：使用一致的字體系統

        # 主要字體（中英文混合）
        FONT_PRIMARY = 'Microsoft JhengHei'  # 微軟正黑體 - 現代化、清晰

        # 次要字體（純英文/數字）
        FONT_SECONDARY = 'Segoe UI'  # Windows 原生字體

        # 等寬字體（代碼/股票代碼）
        FONT_MONOSPACE = 'Consolas'

        # 字體大小
        SIZE_TITLE = 20  # 主標題
        SIZE_SUBTITLE = 14  # 副標題
        SIZE_HEADING = 16  # 章節標題
        SIZE_BODY = 13  # 內文
        SIZE_SMALL = 12  # 小字
        SIZE_BUTTON = 15  # 按鈕
        SIZE_LOG = 13  # 日誌

        # ===== 🎨 優化配色方案 =====
        # 🔥 關鍵改進：提高對比度，避免過暗

        # 背景色（稍微提亮）
        bg_dark = '#1e1e1e'  # 從 #1a1a1a 改為 #1e1e1e
        bg_card = '#2d2d2d'  # 保持不變
        bg_input = '#3d3d3d'  # 保持不變

        # 強調色（稍微調整飽和度）
        accent_blue = '#00d4aa'  # 保持不變（主要強調色）
        accent_orange = '#ff6b35'  # 保持不變（警告/停止）
        accent_green = '#00b894'  # 新增：成功狀態

        # 文字顏色（提高對比度）
        text_primary = '#f5f5f5'  # 從 #ffffff 改為稍柔和的白色
        text_secondary = '#c0c0c0'  # 從 #b0b0b0 提亮
        text_muted = '#909090'  # 新增：更暗的次要文字
        text_warning = '#ffd93d'  # 新增：警告色（更醒目）

        # ===== 配置主框架樣式 =====
        self.style.configure('Card.TFrame',
                             background=bg_card,
                             relief='flat',
                             borderwidth=1)

        # ===== 配置標籤框架樣式 =====
        self.style.configure('Card.TLabelframe',
                             background=bg_card,
                             foreground=text_primary,
                             borderwidth=2,
                             relief='flat')

        self.style.configure('Card.TLabelframe.Label',
                             background=bg_card,
                             foreground=accent_blue,
                             font=(FONT_PRIMARY, SIZE_HEADING, 'bold'))  # 🔥 統一字體

        # ===== 主要按鈕樣式 =====
        self.style.configure('Primary.TButton',
                             font=(FONT_PRIMARY, SIZE_BUTTON, 'bold'),  # 🔥 統一字體
                             foreground='white',
                             focuscolor='none',
                             borderwidth=0,
                             padding=(20, 10))
        self.style.map('Primary.TButton',
                       background=[('active', accent_green), ('!active', accent_blue)])

        # ===== 停止按鈕樣式 =====
        self.style.configure('Danger.TButton',
                             font=(FONT_PRIMARY, SIZE_BUTTON, 'bold'),  # 🔥 統一字體
                             foreground='white',
                             focuscolor='none',
                             borderwidth=0,
                             padding=(20, 10))
        self.style.map('Danger.TButton',
                       background=[('active', '#e84393'), ('!active', accent_orange)])

        # ===== 瀏覽按鈕樣式 =====
        self.style.configure('Secondary.TButton',
                             font=(FONT_PRIMARY, SIZE_SMALL),  # 🔥 統一字體
                             foreground=text_primary,
                             focuscolor='none',
                             borderwidth=1,
                             padding=(15, 8))
        self.style.map('Secondary.TButton',
                       background=[('active', '#636e72'), ('!active', '#74b9ff')])

        # ===== 標籤樣式 =====
        self.style.configure('Title.TLabel',
                             background=bg_card,
                             foreground=text_primary,
                             font=(FONT_PRIMARY, SIZE_TITLE))  # 🔥 統一字體

        self.style.configure('Subtitle.TLabel',
                             background=bg_card,
                             foreground=text_secondary,
                             font=(FONT_PRIMARY, SIZE_SUBTITLE))  # 🔥 統一字體

        # ===== 輸入框樣式 =====
        self.style.configure('Modern.TEntry',
                             fieldbackground=bg_input,
                             foreground=text_primary,
                             borderwidth=1,
                             insertcolor=text_primary,
                             selectbackground=accent_blue)

        # ===== 進度條樣式 =====
        self.style.configure('Modern.Horizontal.TProgressbar',
                             background=accent_blue,
                             troughcolor=bg_input,
                             borderwidth=0,
                             lightcolor=accent_blue,
                             darkcolor=accent_blue,
                             focuscolor='none')

        self.style.map('Modern.Horizontal.TProgressbar',
                       background=[('active', accent_blue),
                                   ('!active', accent_blue)])

        # 🔥 保存配色方案供其他地方使用
        self.colors = {
            'bg_dark': bg_dark,
            'bg_card': bg_card,
            'bg_input': bg_input,
            'accent_blue': accent_blue,
            'accent_orange': accent_orange,
            'accent_green': accent_green,
            'text_primary': text_primary,
            'text_secondary': text_secondary,
            'text_muted': text_muted,
            'text_warning': text_warning
        }

        # 🔥 保存字體方案供其他地方使用
        self.fonts = {
            'primary': FONT_PRIMARY,
            'secondary': FONT_SECONDARY,
            'monospace': FONT_MONOSPACE,
            'size_title': SIZE_TITLE,
            'size_subtitle': SIZE_SUBTITLE,
            'size_heading': SIZE_HEADING,
            'size_body': SIZE_BODY,
            'size_small': SIZE_SMALL,
            'size_button': SIZE_BUTTON,
            'size_log': SIZE_LOG
        }

    def setup_ui(self):
        """優化版面配置 - 日誌空間更大，設定區域更緊湊"""

        # 主框架
        main_frame = tk.Frame(self.root, bg=self.colors['bg_dark'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # ===== 標題區域（大幅縮小）=====
        title_frame = tk.Frame(main_frame, bg=self.colors['bg_card'], relief='flat', bd=2)
        title_frame.pack(fill=tk.X, pady=(0, 8))  # 🔥 從 10 改成 8

        title_content = tk.Frame(title_frame, bg=self.colors['bg_card'])
        title_content.pack(fill=tk.X, padx=15, pady=6)  # 🔥 從 20, 10 改成 15, 6

        # 主標題（縮小字體）
        title_label = tk.Label(
            title_content,
            text="📊 財報數據自動化系統",
            font=(self.fonts['primary'], 18, 'bold'),  # 🔥 從 18 改成 16
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        title_label.pack()

        # 副標題（縮小字體）
        subtitle_label = tk.Label(
            title_content,
            text="財報數據自動化工具 | Version 3.0",
            font=(self.fonts['primary'], 10),  # 🔥 從 12 改成 10
            foreground=self.colors['text_secondary'],
            bg=self.colors['bg_card']
        )
        subtitle_label.pack(pady=(2, 0))  # 🔥 從 3 改成 2

        # ===== 輸入區域框架（壓縮間距）=====
        input_frame = tk.Frame(main_frame, bg=self.colors['bg_card'], relief='flat', bd=2)
        input_frame.pack(fill=tk.X, pady=(0, 8))  # 🔥 從 10 改成 8

        input_content = tk.Frame(input_frame, bg=self.colors['bg_card'])
        input_content.pack(fill=tk.X, padx=12, pady=8)  # 🔥 從 15, 10 改成 12, 8

        # 設定標題
        input_title = tk.Label(
            input_content,
            text="🔍 設定",
            font=(self.fonts['primary'], 12, 'bold'),  # 🔥 從 14 改成 12
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        input_title.pack(anchor=tk.W, pady=(0, 6))  # 🔥 從 8 改成 6

        # ===== 模板選擇區域（壓縮）=====
        template_frame = tk.Frame(input_content, bg=self.colors['bg_card'])
        template_frame.pack(fill=tk.X, pady=(0, 6))  # 🔥 從 10 改成 6

        tk.Label(
            template_frame,
            text="📋 選擇模板",
            font=(self.fonts['primary'], 10, 'bold'),  # 🔥 從 11 改成 10
            foreground=self.colors['text_primary'],
            bg=self.colors['bg_card']
        ).pack(anchor=tk.W, pady=(0, 4))  # 🔥 從 6 改成 4

        # 卡片容器
        cards_container = tk.Frame(template_frame, bg=self.colors['bg_card'])
        cards_container.pack(fill=tk.X)

        # 股票分析卡片
        self.stock_card = self.create_template_card(
            cards_container,
            title="📈 財報基本面分析",
            descriptions=[
                "✓ 完整財務報表",
                "✓ 財報健檢",
                "✓ F.A.C.T.S系統",
                "✓ DCF 現金流折現法"
            ],
            variable=self.stock_analysis_var,
            side=tk.LEFT
        )

        # 選擇權分析卡片
        self.option_card = self.create_template_card(
            cards_container,
            title="📊 選擇權鏈分析",
            descriptions=[
                "✓ 所有選擇權合約資訊",
                "✓ 分析主頁",
                "✓ 總結主頁",
                "✓ 選擇權步驟指引與建議"
            ],
            variable=self.option_analysis_var,
            side=tk.LEFT,
            padx=(8, 0)  # 🔥 從 10 改成 8
        )

        # ===== 股票代碼輸入區（壓縮）=====
        stock_frame = tk.Frame(input_content, bg=self.colors['bg_card'])
        stock_frame.pack(fill=tk.X, pady=(6, 4))  # 🔥 從 8, 6 改成 6, 4

        tk.Label(
            stock_frame,
            text="💼 股票代碼",
            font=(self.fonts['primary'], 10, 'bold'),  # 🔥 從 11 改成 10
            foreground=self.colors['text_primary'],
            bg=self.colors['bg_card']
        ).pack(anchor=tk.W, pady=(0, 3))  # 🔥 從 4 改成 3

        # 輸入框
        stocks_entry = tk.Entry(
            stock_frame,
            textvariable=self.stocks_var,
            font=(self.fonts['monospace'], 10),  # 🔥 從 11 改成 10
            bg=self.colors['bg_input'],
            fg=self.colors['text_primary'],
            insertbackground=self.colors['accent_blue'],
            selectbackground=self.colors['accent_blue'],
            selectforeground='#000000',
            relief='flat',
            bd=2
        )
        stocks_entry.pack(fill=tk.X, ipady=4)  # 🔥 從 5 改成 4

        # 提示文字（縮小字體）
        help_label = tk.Label(
            stock_frame,
            text=(
                "💡 輸入股票代碼，多個代碼請用逗號分隔 (例如: NVDA, MSFT, AAPL)\n"
                "💡 代碼中若包含『-』請直接輸入(例如：BRK-B)\n"
                "💡 若輸入非美國股票代碼，部分資料將有缺失！"
            ),
            font=(self.fonts['primary'], 9),  # 🔥 從 10 改成 9
            foreground=self.colors['text_warning'],
            bg=self.colors['bg_card'],
            justify=tk.LEFT
        )
        help_label.pack(anchor=tk.W, pady=(3, 0))  # 🔥 從 4 改成 3

        # ===== 輸出資料夾選擇（壓縮）=====
        folder_frame = tk.Frame(input_content, bg=self.colors['bg_card'])
        folder_frame.pack(fill=tk.X, pady=(4, 0))  # 🔥 從 6 改成 4

        tk.Label(
            folder_frame,
            text="📁 資料夾路徑",
            font=(self.fonts['primary'], 10, 'bold'),  # 🔥 從 11 改成 10
            foreground=self.colors['text_primary'],
            bg=self.colors['bg_card']
        ).pack(anchor=tk.W, pady=(0, 3))  # 🔥 從 4 改成 3

        folder_input_frame = tk.Frame(folder_frame, bg=self.colors['bg_card'])
        folder_input_frame.pack(fill=tk.X)

        # 路徑輸入框
        folder_entry = tk.Entry(
            folder_input_frame,
            textvariable=self.output_folder_var,
            font=(self.fonts['monospace'], 10),  # 🔥 從 11 改成 10
            bg=self.colors['bg_input'],
            fg=self.colors['text_primary'],
            insertbackground=self.colors['accent_blue'],
            relief='flat',
            bd=2
        )
        folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=3)  # 🔥 從 4 改成 3

        # 瀏覽按鈕
        browse_btn = tk.Button(
            folder_input_frame,
            text="📂 瀏覽",
            command=self.browse_folder,
            font=(self.fonts['primary'], 9, 'bold'),  # 🔥 從 10 改成 9
            bg='#74b9ff',
            fg='white',
            activebackground='#0984e3',
            activeforeground='white',
            relief='flat',
            bd=0,
            cursor='hand2'
        )
        browse_btn.pack(side=tk.RIGHT, padx=(5, 0), ipady=3, ipadx=8)  # 🔥 從 6, 4, 10 改成 5, 3, 8

        # ===== 控制區域框架（大幅壓縮）=====
        control_frame = tk.Frame(main_frame, bg=self.colors['bg_card'], relief='flat', bd=2)
        control_frame.pack(fill=tk.X, pady=(0, 8))  # 🔥 從 10 改成 8

        control_content = tk.Frame(control_frame, bg=self.colors['bg_card'])
        control_content.pack(fill=tk.X, padx=12, pady=8)  # 🔥 從 15, 10 改成 12, 8

        # 控制標題
        control_title = tk.Label(
            control_content,
            text="🎮 分析控制",
            font=(self.fonts['primary'], 12, 'bold'),  # 🔥 從 14 改成 12
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        control_title.pack(anchor=tk.W, pady=(0, 6))  # 🔥 從 8 改成 6

        # ===== 按鈕區（縮小按鈕）=====
        button_frame = tk.Frame(control_content, bg=self.colors['bg_card'])
        button_frame.pack(pady=(0, 6))  # 🔥 從 10 改成 6

        # 開始按鈕
        self.start_btn = tk.Button(
            button_frame,
            text="🚀 開始",
            command=self.start_analysis,
            font=(self.fonts['primary'], 11, 'bold'),  # 🔥 從 13 改成 11
            bg=self.colors['accent_blue'],
            fg='white',
            activebackground=self.colors['accent_green'],
            activeforeground='white',
            relief='flat',
            bd=0,
            cursor='hand2',
            width=10,  # 🔥 從 12 改成 10
            height=1
        )
        self.start_btn.pack(side=tk.LEFT, padx=(0, 8))  # 🔥 從 10 改成 8

        # 停止按鈕
        self.stop_btn = tk.Button(
            button_frame,
            text="⏹️ 停止",
            command=self.stop_analysis,
            font=(self.fonts['primary'], 11, 'bold'),  # 🔥 從 13 改成 11
            bg=self.colors['accent_orange'],
            fg='white',
            activebackground='#e84393',
            activeforeground='white',
            relief='flat',
            bd=0,
            cursor='hand2',
            width=10,  # 🔥 從 12 改成 10
            height=1,
            state=tk.DISABLED
        )
        self.stop_btn.pack(side=tk.LEFT)

        # ===== 進度區域（壓縮）=====
        progress_frame = tk.Frame(control_content, bg=self.colors['bg_card'])
        progress_frame.pack(fill=tk.X, pady=(0, 6))  # 🔥 從 8 改成 6

        tk.Label(
            progress_frame,
            text="📊 數據自動化進度",
            font=(self.fonts['primary'], 10, 'bold'),  # 🔥 從 11 改成 10
            foreground=self.colors['text_primary'],
            bg=self.colors['bg_card']
        ).pack(anchor=tk.W, pady=(0, 3))  # 🔥 從 4 改成 3

        # 進度條容器（縮小高度）
        progress_container = tk.Frame(progress_frame, bg=self.colors['bg_input'], height=14)  # 🔥 從 16 改成 14
        progress_container.pack(fill=tk.X, pady=(0, 4))  # 🔥 從 6 改成 4
        progress_container.pack_propagate(False)

        self.progress = ttk.Progressbar(
            progress_container,
            mode='determinate',
            maximum=100,
            value=0,
            style='Modern.Horizontal.TProgressbar',
            length=400
        )
        self.progress.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        # 百分比標籤
        self.progress_percent_label = tk.Label(
            progress_frame,
            text="0%",
            font=(self.fonts['secondary'], 9, 'bold'),  # 🔥 從 10 改成 9
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        self.progress_percent_label.pack(anchor=tk.W, pady=(2, 0))

        # 狀態標籤
        self.status_label = tk.Label(
            control_content,
            text="✅ 系統準備就緒",
            font=(self.fonts['primary'], 10, 'bold'),  # 🔥 從 12 改成 10
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        self.status_label.pack(pady=(6, 0))  # 🔥 從 8 改成 6

        # ===== 日誌區域框架（🔥 關鍵：擴大空間）=====
        log_frame = tk.Frame(main_frame, bg=self.colors['bg_card'], relief='flat', bd=2)
        log_frame.pack(fill=tk.BOTH, expand=True)  # 🔥 使用 expand=True 佔據剩餘空間

        log_content = tk.Frame(log_frame, bg=self.colors['bg_card'])
        log_content.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)  # 🔥 從 15, 10 改成 12, 8

        # 日誌標題
        log_title = tk.Label(
            log_content,
            text="📋 執行日誌",
            font=(self.fonts['primary'], 12, 'bold'),  # 🔥 從 14 改成 12
            foreground=self.colors['accent_blue'],
            bg=self.colors['bg_card']
        )
        log_title.pack(anchor=tk.W, pady=(0, 4))  # 🔥 從 6 改成 4

        # 🔥 日誌文字框（增加最小高度）
        self.log_text = scrolledtext.ScrolledText(
            log_content,
            font=(self.fonts['monospace'], 14),  # 🔥 從 11 改成 10
            bg='#1a1a1a',
            fg='#00ff00',
            insertbackground=self.colors['accent_blue'],
            selectbackground=self.colors['accent_blue'],
            selectforeground='#000000',
            relief='flat',
            bd=2,
            wrap=tk.WORD,
            height=25  # 🔥 從 20 增加到 25
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)  # 🔥 確保填滿所有剩餘空間

        # 初始化日誌
        self.log_text.insert(tk.END, "=== 程式已啟動 ===\n")
        self.log_text.insert(tk.END, "系統準備就緒，請選擇模板並輸入股票代碼開始自動化...\n\n")

    def create_template_card(self, parent, title, descriptions, variable, side=tk.LEFT, padx=(0, 0)):
        """創建模板選擇卡片 - 壓縮版本"""

        # 卡片外框
        card_frame = tk.Frame(parent, bg=self.colors['bg_input'], relief='flat', bd=2, cursor='hand2')
        card_frame.pack(side=side, padx=padx, fill=tk.BOTH, expand=True)

        # 卡片內容容器（減少 padding）
        card_content = tk.Frame(card_frame, bg=self.colors['bg_input'])
        card_content.pack(fill=tk.BOTH, expand=True, padx=10, pady=8)  # 🔥 從 12, 12 改成 10, 8

        # 標題（縮小字體）
        title_label = tk.Label(
            card_content,
            text=title,
            font=(self.fonts['primary'], 13, 'bold'),  # 🔥 從 11 改成 10
            foreground=self.colors['text_primary'],
            bg=self.colors['bg_input']
        )
        title_label.pack(anchor=tk.W, pady=(0, 6))  # 🔥 從 8 改成 6

        # 分隔線
        separator = tk.Frame(card_content, bg=self.colors['accent_blue'], height=2)
        separator.pack(fill=tk.X, pady=(0, 6))  # 🔥 從 8 改成 6

        # 描述文字（縮小字體和間距）
        for desc in descriptions:
            desc_label = tk.Label(
                card_content,
                text=desc,
                font=(self.fonts['primary'], 12),  # 🔥 從 10 改成 9
                foreground=self.colors['text_secondary'],
                bg=self.colors['bg_input'],
                anchor=tk.W
            )
            desc_label.pack(anchor=tk.W, pady=1)

        # 狀態標籤
        status_label = tk.Label(
            card_content,
            text="[已選擇]" if variable.get() else "[點擊選擇]",
            font=(self.fonts['primary'], 9, 'bold'),  # 🔥 從 10 改成 9
            foreground=self.colors['accent_blue'] if variable.get() else '#666666',
            bg=self.colors['bg_input']
        )
        status_label.pack(pady=(8, 0))  # 🔥 從 10 改成 8

        # 綁定點擊事件
        def toggle_selection(event=None):
            variable.set(not variable.get())
            self.update_card_appearance(card_frame, card_content, title_label,
                                        separator, status_label, variable.get())

        # 綁定所有元素的點擊事件
        for widget in [card_frame, card_content, title_label, separator, status_label] + list(
                card_content.winfo_children()):
            widget.bind('<Button-1>', toggle_selection)

        # 懸停效果
        def on_enter(event):
            if variable.get():
                card_frame.config(bg=self.colors['accent_blue'], bd=3)
            else:
                card_frame.config(bg='#555555', bd=3)

        def on_leave(event):
            if variable.get():
                card_frame.config(bg=self.colors['accent_blue'], bd=2)
            else:
                card_frame.config(bg=self.colors['bg_input'], bd=2)

        card_frame.bind('<Enter>', on_enter)
        card_frame.bind('<Leave>', on_leave)

        # 初始化外觀
        self.update_card_appearance(card_frame, card_content, title_label,
                                    separator, status_label, variable.get())

        return card_frame

    def update_card_appearance(self, card_frame, card_content, title_label, separator, status_label, is_selected):
        """更新卡片外觀"""
        if is_selected:
            card_frame.config(bg='#00d4aa')
            card_content.config(bg='#2d4d4d')
            title_label.config(bg='#2d4d4d', foreground='#00d4aa')
            separator.config(bg='#00d4aa')
            status_label.config(text="[已選擇]", foreground='#00d4aa', bg='#2d4d4d')

            # 更新所有子元素的背景
            for widget in card_content.winfo_children():
                if isinstance(widget, tk.Label) and widget != title_label and widget != status_label:
                    widget.config(bg='#2d4d4d')
        else:
            card_frame.config(bg='#3d3d3d')
            card_content.config(bg='#3d3d3d')
            title_label.config(bg='#3d3d3d', foreground='#ffffff')
            separator.config(bg='#666666')
            status_label.config(text="[點擊選擇]", foreground='#666666', bg='#3d3d3d')

            # 更新所有子元素的背景
            for widget in card_content.winfo_children():
                if isinstance(widget, tk.Label) and widget != title_label and widget != status_label:
                    widget.config(bg='#3d3d3d')

    def update_progress(self, current_step, total_steps, step_name=""):
        """更新進度條 - 帶動畫效果"""
        if total_steps > 0:
            target_progress = (current_step / total_steps) * 100
            current_progress = self.progress['value']

            if target_progress > current_progress:
                self.animate_progress_smooth(current_progress, target_progress, step_name, current_step, total_steps)
            else:
                self.progress['value'] = target_progress
                self.progress_percent_label.config(text=f"{target_progress:.1f}%")
                if step_name:
                    self.update_status(f"{step_name} ({current_step}/{total_steps})")
                self.root.update_idletasks()

    def animate_progress_smooth(self, start_value, end_value, step_name="", current_step=0, total_steps=0):
        """更平滑的動畫效果 - 使用緩動函數"""
        import math

        progress_diff = end_value - start_value
        animation_steps = max(int(progress_diff * 3), 30)
        total_duration = 1200
        delay_ms = int(total_duration / animation_steps)

        def ease_out_cubic(t):
            """緩出動畫函數 - 開始快，結束慢"""
            return 1 - pow(1 - t, 3)

        def animate_step(step):
            if step <= animation_steps:
                t = step / animation_steps
                eased_t = ease_out_cubic(t)
                current_value = start_value + (progress_diff * eased_t)

                if step == animation_steps:
                    current_value = end_value

                self.progress['value'] = current_value
                self.progress_percent_label.config(text=f"{current_value:.1f}%")

                if step == animation_steps and step_name:
                    self.update_status(f"{step_name} ({current_step}/{total_steps})")

                self.root.update_idletasks()

                if step < animation_steps:
                    self.root.after(delay_ms, lambda: animate_step(step + 1))

        animate_step(0)

    def reset_progress(self):
        """重置進度條"""
        self.progress['value'] = 0
        self.progress_percent_label.config(text="0%")
        self.root.update_idletasks()

    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.output_folder_var.set(folder)

    def log(self, message):
        """現代化日誌顯示"""
        timestamp = datetime.now().strftime("%H:%M:%S")

        if "步驟" in message:
            color = "#ffffff"
        elif "✅" in message or "成功" in message:
            color = "#00ff00"
        elif "❌" in message or "錯誤" in message or "失敗" in message:
            color = "#ff4757"
        elif "⚠️" in message or "警告" in message:
            color = "#ffa502"
        elif "🔄" in message or "處理" in message:
            color = "#37f4fa"
        elif "🚀" in message or "開始" in message:
            color = "#ff6b35"
        else:
            color = "#ffffff"

        tag_name = f"color_{color.replace('#', '')}"
        self.log_text.tag_configure(tag_name, foreground=color)
        self.log_text.tag_configure("timestamp", foreground="#70a1ff")

        self.log_text.insert(tk.END, f"[{timestamp}] ", "timestamp")
        self.log_text.insert(tk.END, f"{message}\n", tag_name)

        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def update_status(self, status):
        """更新狀態標籤"""
        if "完成" in status or "成功" in status:
            color = "#00d4aa"
            icon = "✅"
        elif "失敗" in status or "錯誤" in status:
            color = "#ff4757"
            icon = "❌"
        elif "停止" in status:
            color = "#ffa502"
            icon = "⏹️"
        elif "步驟" in status or "處理" in status:
            color = "#3742fa"
            icon = "🔄"
        else:
            color = "#ffffff"
            icon = "📊"

        self.status_label.config(text=f"{icon} {status}", foreground=color)
        self.root.update_idletasks()

    def start_analysis(self):
        """開始分析 - 加入模板選擇驗證（強化版）"""
        # 檢查是否至少選擇一個模板
        do_stock_analysis = self.stock_analysis_var.get()
        do_option_analysis = self.option_analysis_var.get()

        if not do_stock_analysis and not do_option_analysis:
            messagebox.showwarning("⚠️ 警告", "請至少選擇一個分析模板！")
            return

        # 🔥 修復：增加 None 檢查和更詳細的錯誤訊息
        if do_stock_analysis:
            # 檢查股票分析模板
            if Fundamental_Excel_Template_Base64 is None or \
                    not isinstance(Fundamental_Excel_Template_Base64, str) or \
                    Fundamental_Excel_Template_Base64.strip() == "" or \
                    "請將您從轉換工具得到的" in Fundamental_Excel_Template_Base64:
                messagebox.showerror(
                    "❌ 錯誤",
                    "股票分析模板未正確載入！\n\n"
                    "請檢查以下事項：\n"
                    "1. Fundamental_Excel_Template_Base64 變數是否已設定\n"
                    "2. 模板檔案是否存在於正確路徑\n"
                    "3. 檔案內容是否為有效的 base64 字串"
                )
                return

        if do_option_analysis:
            # 🔥 修改：檢查實體檔案是否存在
            if getattr(sys, 'frozen', False):
                base_path = os.path.dirname(sys.executable)
            else:
                current_file = os.path.abspath(__file__)
                base_path = os.path.dirname(os.path.dirname(current_file))

            template_path = os.path.join(base_path, 'excel_template', 'Option_Chain_Template.xlsm')

            if not os.path.exists(template_path):
                messagebox.showerror(
                    "❌ 錯誤",
                    f"選擇權分析模板未找到！\n\n"
                    f"請確認檔案存在：\n{template_path}"
                )
                return

        # 獲取輸入的股票代碼
        stocks_input = self.stocks_var.get().strip()
        if not stocks_input:
            messagebox.showwarning("⚠️ 警告", "請輸入至少一個股票代碼！")
            return

        # 處理股票代碼列表
        stocks_raw = [s.strip().upper() for s in stocks_input.split(',')]
        stocks = []

        seen = set()
        for stock in stocks_raw:
            if stock and stock not in seen:
                stocks.append(stock)
                seen.add(stock)

        if not stocks:
            messagebox.showwarning("⚠️ 警告", "請輸入有效的股票代碼！")
            return

        # 構建確認訊息
        templates_text = []
        if do_stock_analysis:
            templates_text.append("✅ 股票分析（完整數據）")
        if do_option_analysis:
            templates_text.append("✅ 選擇權分析（Option Chain）")

        templates_str = "\n   ".join(templates_text)

        confirmation_message = (
            f"即將驗證並數據自動化以下股票：\n"
            f"📈 {', '.join(stocks)}\n\n"
            f"📋 分析模板：\n"
            f"   {templates_str}\n\n"
            f"🔍 系統將先驗證股票代碼有效性\n"
            f"📊 僅數據自動化有效的股票代碼\n"
            f"🔥 預計需要數分鐘時間\n\n"
            f"是否開始？"
        )

        if not messagebox.askyesno("🚀 確認開始", confirmation_message):
            return

        # 禁用按鈕
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.is_running = True

        # 清空日誌
        self.log_text.delete(1.0, tk.END)

        # 重置進度條
        self.reset_progress()

        # 在創建線程時記錄引用
        self.current_thread = threading.Thread(target=self.run_analysis, args=(stocks,))
        self.current_thread.daemon = True
        self.current_thread.start()

    def stop_analysis(self):
        """立即強制停止分析並清理所有資源 - 修復遞迴錯誤"""
        try:
            # Step 1: 立即設定停止標誌
            self.is_running = False
            self.log("🛑 使用者請求立即停止，開始強制清理資源...")

            # Step 2: 強制清理 Playwright 資源
            with self.cleanup_lock:
                # 清理 Scraper
                if self.current_scraper:
                    self.log("🧹 正在關閉 Playwright 瀏覽器...")
                    try:
                        if self.event_loop and self.event_loop.is_running():
                            # 🔥 修改：不等待結果，直接發送取消信號
                            asyncio.run_coroutine_threadsafe(
                                self.current_scraper.cleanup(),
                                self.event_loop
                            )
                            self.log("✅ 已發送關閉信號給 Playwright")
                        else:
                            # 事件循環已停止，創建新的循環來清理
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            try:
                                new_loop.run_until_complete(
                                    asyncio.wait_for(self.current_scraper.cleanup(), timeout=3.0)
                                )
                            finally:
                                new_loop.close()
                            self.log("✅ Playwright 瀏覽器已關閉")
                    except Exception as e:
                        self.log(f"⚠️ 清理 Scraper 時發生錯誤（已忽略）: {e}")
                    finally:
                        self.current_scraper = None

                # 清理 Manager
                if self.current_manager:
                    self.log("🧹 正在清理 Manager 資源...")
                    try:
                        if hasattr(self.current_manager, 'cleanup'):
                            self.current_manager.cleanup()
                    except Exception as e:
                        self.log(f"⚠️ 清理 Manager 時發生錯誤（已忽略）: {e}")
                    finally:
                        self.current_manager = None

            # Step 3: 取消異步任務（非遞迴）
            if self.current_task and not self.current_task.done():
                self.log("🚫 正在取消異步任務...")
                try:
                    self.current_task.cancel()
                except Exception as e:
                    self.log(f"⚠️ 取消任務時發生錯誤（已忽略）: {e}")

            # Step 4: 停止事件循環
            if self.event_loop and self.event_loop.is_running():
                self.log("🔄 正在停止事件循環...")
                try:
                    self.event_loop.call_soon_threadsafe(self.event_loop.stop)
                except Exception as e:
                    self.log(f"⚠️ 停止事件循環時發生錯誤（已忽略）: {e}")

            # 🔥 Step 5: 移除強制清理剩餘任務的邏輯
            # 因為這會導致遞迴錯誤，改為讓事件循環自然停止

            # Step 6: 恢復 UI 狀態
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.progress['value'] = 0
            self.progress_percent_label.config(text="0%")
            self.update_status("數據自動化已停止")
            self.root.update_idletasks()

            self.log("✅ 資源清理完成，系統已就緒")

        except Exception as e:
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.progress['value'] = 0
            self.progress_percent_label.config(text="0%")
            self.log(f"⚠️ 停止過程中發生錯誤，但UI已恢復: {e}")

    def run_analysis(self, stocks):
        """執行分析的主函數"""
        try:
            # 🔥 確保舊的事件循環完全關閉
            try:
                old_loop = asyncio.get_event_loop()
                if old_loop and not old_loop.is_closed():
                    old_loop.close()
            except RuntimeError:
                pass

            # 🔥 創建全新的事件循環
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)
            print("✓ 新的事件循環已創建並設定")

            # 執行異步分析
            self.current_task = self.event_loop.create_task(self.async_analysis(stocks))
            self.event_loop.run_until_complete(self.current_task)

        except asyncio.CancelledError:
            self.log("🛑 異步任務已被成功取消")

        except KeyboardInterrupt:
            self.log("🛑 用戶中斷程式（Ctrl+C）")
            self.is_running = False

        except Exception as e:
            if self.is_running:
                self.log(f"❌ 發生錯誤：{str(e)}")
                import traceback
                traceback.print_exc()
                messagebox.showerror("❌ 錯誤", f"數據自動化過程中發生錯誤：\n{str(e)}")
            else:
                self.log("ℹ️ 數據自動化已被使用者停止")

        finally:
            self.current_task = None
            self.current_thread = None

            # 🔥 優雅地清理事件循環
            if self.event_loop:
                try:
                    # Step 1: 取消所有待處理任務
                    if not self.event_loop.is_closed():
                        pending = [task for task in asyncio.all_tasks(self.event_loop)
                                   if not task.done()]

                        if pending:
                            print(f"🧹 取消 {len(pending)} 個待處理任務...")
                            for task in pending:
                                try:
                                    task.cancel()
                                except Exception:
                                    pass

                            # 🔥 等待所有任務完成取消
                            try:
                                self.event_loop.run_until_complete(
                                    asyncio.wait_for(
                                        asyncio.gather(*pending, return_exceptions=True),
                                        timeout=8.0  # 🔥 增加到 8 秒
                                    )
                                )
                                print("✓ 所有任務已正確取消")
                            except asyncio.TimeoutError:
                                print("⚠️ 等待任務取消超時，強制繼續")
                            except Exception as e:
                                print(f"⚠️ 等待任務取消時發生錯誤: {e}")

                    # 🔥 Step 2: 額外等待，確保 Playwright 子進程完全結束
                    print("🧹 等待 Playwright 子進程完全結束...")
                    import time
                    time.sleep(1.5)  # 🔥 給 1.5 秒讓子進程清理
                    print("✓ Playwright 子進程已結束")

                    # Step 3: 停止事件循環
                    if self.event_loop.is_running():
                        self.event_loop.stop()

                    # Step 4: 再等一下
                    time.sleep(0.2)

                    # Step 5: 關閉事件循環
                    if not self.event_loop.is_closed():
                        self.event_loop.close()

                    print("✓ 事件循環已正確關閉")
                except Exception as e:
                    print(f"⚠️ 關閉事件循環時發生錯誤: {e}")
                finally:
                    self.event_loop = None

            # 恢復UI
            if self.is_running:
                self.start_btn.config(state=tk.NORMAL)
                self.stop_btn.config(state=tk.DISABLED)
                self.reset_progress()
                self.is_running = False

    async def async_analysis(self, stocks):
        """
        異步執行分析 - 改進版

        🔥 改進重點：
        1. 所有股票都會執行完整流程（美國 + 非美國）
        2. 只在 Financial 和 Ratios 步驟跳過非美國公司
        3. 簡潔清晰的日誌訊息
        """

        scraper = None
        processor = None
        manager = None

        try:
            # 獲取選擇的模板
            do_stock_analysis = self.stock_analysis_var.get()
            do_option_analysis = self.option_analysis_var.get()

            # 計算總步驟數
            total_steps = 0
            if do_stock_analysis and do_option_analysis:
                total_steps = 15
            elif do_stock_analysis:
                total_steps = 10
            elif do_option_analysis:
                total_steps = 6

            current_step = 0
            start_time = time.time()

            # ===== 啟動訊息 =====
            self.log("🎯" + "=" * 80)
            self.log("🚀 系統啟動中")
            self.log(f"📊 輸入股票：{', '.join(stocks)}")

            templates_info = []
            if do_stock_analysis:
                templates_info.append("股票分析")
            if do_option_analysis:
                templates_info.append("選擇權分析")
            self.log(f"📋 分析模板：{' + '.join(templates_info)}")
            self.log("🎯" + "=" * 80)

            def check_if_stopped():
                if not self.is_running:
                    self.log("🛑 檢測到停止信號，正在中止操作...")
                    raise asyncio.CancelledError("使用者請求停止")

            # ===== 驗證階段 =====

            # 初始化 Schwab API
            check_if_stopped()
            self.update_status("初始化 Schwab API")
            self.log("\n🔧 正在初始化 Schwab API...")

            temp_stocks_dict = {
                'final_stocks': stocks,
                'us_stocks': [],
                'non_us_stocks': []
            }

            scraper = StockScraper(stocks=temp_stocks_dict, config=self.config, max_concurrent=3)
            self.current_scraper = scraper

            if not scraper.schwab_client:
                self.log("❌ Schwab Client 初始化失敗")
                return

            self.log("✅ Schwab API 已就緒")

            # 創建 validator
            validator = StockValidator(
                schwab_client=scraper.schwab_client,
                request_delay=1.0
            )

            # 🔥 步驟 1: 驗證有效性
            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "驗證股票代碼有效性")
            self.log(f"\n🔍 步驟 {current_step}/{total_steps}：驗證股票代碼...")

            valid_stocks, invalid_stocks = await validator.validate_stocks_async(
                stocks, log_callback=lambda msg: None  # 🔥 簡化：不顯示每個股票的驗證訊息
            )

            if invalid_stocks:
                self.log(f"⚠️  無效股票：{', '.join(invalid_stocks)}")

            if not valid_stocks:
                self.log("❌ 沒有找到任何有效的股票代碼")
                self.update_status("失敗：無有效股票代碼")
                return

            self.log(f"✅ 有效股票：{', '.join(valid_stocks)}")

            # 🔥 步驟 2: 分類股票（US / Non-US）
            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "分類股票（US / Non-US）")
            self.log(f"\n🌍 步驟 {current_step}/{total_steps}：分類股票...")

            us_stocks, non_us_stocks = await validator.classify_stocks_async(
                valid_stocks, log_callback=lambda msg: None  # 🔥 簡化：不顯示每個股票的分類訊息
            )

            # 🔥 準備股票字典（包含所有有效股票）
            stocks_dict = {
                'final_stocks': valid_stocks,  # 🔥 關鍵：包含所有有效股票
                'us_stocks': us_stocks,
                'non_us_stocks': non_us_stocks
            }

            # 🔥 簡潔的分類摘要
            self.log("\n📊 股票分類結果：")
            if us_stocks:
                self.log(f"   🟢 美國公司：{len(us_stocks)} 支 → {', '.join(us_stocks)}")
            if non_us_stocks:
                self.log(f"   🔴 非美國公司：{len(non_us_stocks)} 支 → {', '.join(non_us_stocks)}")
                self.log(f"      💡 將自動跳過 Financial 和 Ratios（roic.ai 需付費）")

            self.log(f"\n🎯 將處理 {len(valid_stocks)} 支股票")
            self.log("🎯" + "=" * 80)

            # ===== 股票分析階段 =====
            saved_stock_files = []

            if do_stock_analysis:
                check_if_stopped()
                self.log("\n【第一階段：股票分析】")
                self.log("=" * 80)

                # 初始化系統
                self.update_status("設定基本面模板分析系統")
                self.log("🔧 設定系統中...")

                scraper = StockScraper(stocks=stocks_dict, config=self.config, max_concurrent=3)
                processor = StockProcess(max_concurrent=2)
                manager = StockManager(scraper=scraper, processor=processor,
                                       stocks=stocks_dict, validator=validator, max_concurrent=15)

                self.current_scraper = scraper
                self.current_manager = manager

                self.log("✅ 系統設定完成")

                # 初始化 Excel
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] 初始化 Excel")
                self.log(f"\n📄 步驟 {current_step}/{total_steps}：初始化 Excel 檔案...")

                success = await manager.initialize_excel_files()
                if not success:
                    self.log("❌ Excel 檔案初始化失敗")
                    return
                self.log("✅ Excel 檔案初始化完成")

                # Summary 和關鍵指標
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] Summary 和關鍵指標")
                self.log(f"\n📊 步驟 {current_step}/{total_steps}：抓取 Summary 和關鍵指標...")

                await manager.process_combined_summary_and_metrics()
                self.log("✅ Summary 和關鍵指標完成")

                # 🔥 Financial（只跑美國公司）
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] Financial")
                self.log(f"\n💰 步驟 {current_step}/{total_steps}：處理 Financial...")

                if us_stocks:
                    self.log(f"   抓取 {len(us_stocks)} 支美國公司的 Financial 數據")
                    await manager.process_financial()
                    self.log("✅ Financial 數據完成")

                if non_us_stocks:
                    self.log(f"   ⏭️  跳過 {len(non_us_stocks)} 支非美國公司（roic.ai 需付費）")

                # 🔥 Ratios（只跑美國公司）
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] Ratios")
                self.log(f"\n📈 步驟 {current_step}/{total_steps}：處理 Ratios...")

                if us_stocks:
                    self.log(f"   抓取 {len(us_stocks)} 支美國公司的 Ratios 數據")
                    await manager.process_ratios()
                    self.log("✅ Ratios 數據完成")

                if non_us_stocks:
                    self.log(f"   ⏭️  跳過 {len(non_us_stocks)} 支非美國公司（roic.ai 需付費）")

                # 其他數據（所有股票）
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] 其他數據")
                self.log(f"\n📋 步驟 {current_step}/{total_steps}：抓取其他數據...")

                await manager.process_others_data()
                self.log("✅ 其他數據完成")

                # Revenue Growth 和 WACC
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] Revenue Growth & WACC")
                self.log(f"\n📈 步驟 {current_step}/{total_steps}：處理 Revenue Growth 和 WACC...")

                await manager.process_seekingalpha()
                await manager.process_wacc()
                self.log("✅ Revenue Growth 和 WACC 完成")

                # TradingView
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] TradingView")
                self.log(f"\n📈 步驟 {current_step}/{total_steps}：處理 TradingView...")

                await manager.process_TradingView()
                self.log("✅ TradingView 完成")

                # 財報日期
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] 財報日期")
                self.log(f"\n📅 步驟 {current_step}/{total_steps}：抓取財報日期...")

                await manager.process_earnings_dates()
                self.log("✅ 財報日期完成")

                # 保存檔案
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "[股票] 保存 Excel")
                self.log(f"\n💾 步驟 {current_step}/{total_steps}：保存 Excel...")

                output_folder = self.output_folder_var.get()
                saved_stock_files = manager.save_all_excel_files(output_folder)
                self.log(f"✅ 已保存 {len(saved_stock_files)} 個股票分析檔案")
                self.log("=" * 80)

            # ===== 選擇權分析階段（保持不變）=====
            saved_option_files = []
            if do_option_analysis:
                check_if_stopped()
                self.log("\n【第二階段:選擇權分析】")
                self.log("🎯" + "=" * 80)

                # 如果股票分析沒執行,需要創建 manager
                if not do_stock_analysis:
                    self.update_status("設定選擇權分析系統")
                    self.log("🔧 正在設定選擇權分析系統...")

                    scraper = StockScraper(stocks=stocks_dict, config=self.config, max_concurrent=3)
                    processor = StockProcess(max_concurrent=2)
                    manager = StockManager(scraper=scraper, processor=processor,
                                           stocks=stocks_dict, validator=validator, max_concurrent=15)

                    # 🔥 保存引用
                    self.current_scraper = scraper
                    self.current_manager = manager

                    self.log("✅ 選擇權系統設定完成")

                # 初始化選擇權 Excel
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[選擇權] 設定 Excel 檔案")
                self.log(f"\n📄 步驟 {step_num}:[選擇權] 正在設定 Excel 檔案...")

                try:
                    success = await manager.initialize_option_excel_files()
                    if not success:
                        self.log("⚠️ 選擇權 Excel 檔案設定失敗")
                        if do_stock_analysis:
                            self.log("⚠️ 股票分析已完成,將跳過選擇權分析")
                        else:
                            self.log("❌ 選擇權分析失敗,停止")
                            self.update_status("失敗:選擇權 Excel 初始化錯誤")
                            return
                    else:
                        self.log("✅ 選擇權 Excel 檔案初始化完成")

                        # 🔥 步驟 1: 批次抓取所有數據
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 批次抓取所有數據")
                        self.log(f"\n📊 步驟 {step_num}:[選擇權] 正在批次抓取 Beta、Barchart 和 Option Chain...")

                        # 依序抓取但不寫入
                        await manager.process_beta()
                        await manager.process_barchart_for_options()
                        await manager.process_option_chains()

                        self.log("✅ 所有選擇權數據抓取完成")

                        # 🔥 新增：財報日期處理（針對 Option 模板）
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 抓取財報公布日期")
                        self.log(f"\n📅 步驟 {step_num}:[選擇權] 正在寫入財報公布日期到選擇權模板...")

                        # 🔥 關鍵：如果股票分析沒執行，需要先抓取財報日期
                        if not do_stock_analysis:
                            await manager.process_earnings_dates()
                        else:
                            # 如果已經在股票分析階段抓取過，只需要寫入 Option 模板
                            self.log("   ℹ️ 財報日期已在股票分析階段抓取，正在寫入選擇權模板...")

                            # 直接從已抓取的數據寫入（需要確保 scraper 已執行過 run_earnings_dates）
                            # 或者重新執行一次（比較安全）
                            await manager.process_earnings_dates()

                        self.log("✅ 財報公布日期寫入選擇權模板完成")

                        # 🔥 步驟 2: 批次寫入 (實際上已在上面的方法中完成)
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 批次寫入 Excel")
                        self.log(f"\n💾 步驟 {step_num}:[選擇權] 已完成批次寫入到 Excel")
                        self.log("✅ 選擇權數據批次處理完成")

                        # 保存選擇權檔案
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 保存 Excel 檔案")
                        self.log(f"\n💾 步驟 {step_num}:[選擇權] 正在保存選擇權 Excel 檔案...")

                        output_folder = self.output_folder_var.get()
                        saved_option_files = manager.save_all_option_excel_files(output_folder)
                        self.log(f"✅ 選擇權 Excel 檔案保存完成（{len(saved_option_files)} 個檔案）")

                except Exception as e:
                    self.log(f"⚠️ 選擇權分析過程發生錯誤: {e}")
                    if do_stock_analysis:
                        self.log("⚠️ 股票分析已完成,將繼續完成流程")
                    else:
                        self.log("❌ 選擇權分析失敗,停止")
                        raise e

                self.log("🎯" + "=" * 80)

            # 完成
            self.update_progress(total_steps, total_steps, "完成！")

            end_time = time.time()
            execution_time = end_time - start_time

            # 🔥 簡潔的完成訊息
            self.log("\n" + "🎉" + "=" * 80)
            self.log("🎊 股票數據自動化完成！")
            self.log(f"⏱️  執行時間：{execution_time:.2f} 秒")
            self.log(f"📊 處理股票：{len(stocks_dict['final_stocks'])} 支")

            if do_stock_analysis:
                if us_stocks:
                    self.log(f"   🟢 美國公司：{len(us_stocks)} 支（完整數據）")
                if non_us_stocks:
                    self.log(f"   🔴 非美國公司：{len(non_us_stocks)} 支（已跳過 Financial/Ratios）")
                self.log(f"   💾 股票檔案：{len(saved_stock_files)} 個")

            if do_option_analysis:
                self.log(f"   💾 選擇權檔案：{len(saved_option_files)} 個")

            self.log(f"📁 保存位置：{self.output_folder_var.get()}")
            self.log("🎉" + "=" * 80)

            self.update_status("完成！")

            # 顯示完成對話框
            completion_msg = f"股票數據自動化已成功完成！\n\n"
            completion_msg += f"📊 處理股票：{len(stocks_dict['final_stocks'])} 支\n"

            if do_stock_analysis:
                if us_stocks:
                    completion_msg += f"🟢 美國公司：{len(us_stocks)} 支\n"
                if non_us_stocks:
                    completion_msg += f"🔴 非美國公司：{len(non_us_stocks)} 支\n"
                    completion_msg += f"   （已跳過 Financial/Ratios）\n"

            completion_msg += f"⏱️  執行時間：{execution_time:.1f} 秒\n"
            completion_msg += f"💾 保存檔案：{len(saved_stock_files) + len(saved_option_files)} 個"

            messagebox.showinfo("🎉 完成", completion_msg)

        except asyncio.CancelledError:
            self.log("🛑 數據自動化任務已被使用者取消")
            self.update_status("數據自動化已停止")
            raise

        except Exception as e:
            self.reset_progress()
            error_msg = f"系統錯誤：{str(e)}"
            self.log(f"❌ {error_msg}")
            self.update_status("數據自動化失敗")
            messagebox.showerror("❌ 錯誤", f"數據自動化過程中發生錯誤：\n{str(e)}")
            raise e

        finally:
            # 🔥 優雅地清理資源
            self.log("🧹 開始最終清理...")

            cleanup_tasks = []

            # 清理 Scraper
            if scraper and scraper == self.current_scraper:
                self.log("🧹 清理 Scraper 資源...")
                cleanup_tasks.append(scraper.cleanup())

            # 清理 Manager
            if manager and manager == self.current_manager:
                self.log("🧹 清理 Manager 資源...")
                if hasattr(manager, 'cleanup') and asyncio.iscoroutinefunction(manager.cleanup):
                    cleanup_tasks.append(manager.cleanup())

            # 🔥 等待所有清理任務完成（增加超時）
            if cleanup_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*cleanup_tasks, return_exceptions=True),
                        timeout=10.0  # 🔥 增加到 10 秒
                    )
                    self.log("✅ 所有資源已正確清理")
                except asyncio.TimeoutError:
                    self.log("⚠️ 清理超時（已等待 10 秒），強制繼續")
                except Exception as e:
                    self.log(f"⚠️ 清理時發生錯誤（已忽略）: {e}")

            self.current_scraper = None
            self.current_manager = None
            self.log("✅ 最終清理完成")

    def run(self):
        """啟動GUI"""
        self.root.mainloop()


# ===== 程式進入點 =====
if __name__ == "__main__":
    app = StockAnalyzerGUI()
    app.run()