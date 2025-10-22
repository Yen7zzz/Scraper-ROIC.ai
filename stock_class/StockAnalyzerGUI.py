import warnings
import sys
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import asyncio
import os
from datetime import datetime
import time
from excel_template.fundamental_excel_template import Fundamental_Excel_Template_Base64
from excel_template.option_chain_excel_template import Option_Chain_Excel_Template_Base64
from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess
from stock_class.StockManager import StockManager
from stock_class.StockValidator import StockValidator

# 🔥 抑制不必要的警告
warnings.filterwarnings('ignore', category=ResourceWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=DeprecationWarning)

# 🔥 Windows 特定：使用 Selector 事件循環策略（更穩定）
if sys.platform == 'win32':
    # 對於 Python 3.8+
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except AttributeError:
        # Python 3.7 或更早版本
        pass

# ====== GUI 部分 ======
class StockAnalyzerGUI:
    def __init__(self, config=None):
        self.root = tk.Tk()
        self.root.title("股票爬蟲程式 v2.1")
        self.root.geometry("1400x1000")
        self.root.configure(bg='#1a1a1a')  # 深色背景
        self.root.minsize(1200, 900)

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

    def setup_custom_styles(self):
        """設定現代化樣式"""
        # 深色主題配色
        bg_dark = '#1a1a1a'
        bg_card = '#2d2d2d'
        accent_blue = '#00d4aa'
        accent_orange = '#ff6b35'
        text_primary = '#ffffff'
        text_secondary = '#b0b0b0'

        # 配置主框架樣式
        self.style.configure('Card.TFrame',
                             background=bg_card,
                             relief='flat',
                             borderwidth=1)

        # 配置標籤框架樣式
        self.style.configure('Card.TLabelframe',
                             background=bg_card,
                             foreground=text_primary,
                             borderwidth=2,
                             relief='flat')

        self.style.configure('Card.TLabelframe.Label',
                             background=bg_card,
                             foreground=accent_blue,
                             font=('Microsoft JhengHei', 12, 'bold'))

        # 主要按鈕樣式
        self.style.configure('Primary.TButton',
                             font=('Microsoft JhengHei', 11, 'bold'),
                             foreground='white',
                             focuscolor='none',
                             borderwidth=0,
                             padding=(20, 10))
        self.style.map('Primary.TButton',
                       background=[('active', '#00b894'), ('!active', accent_blue)])

        # 停止按鈕樣式
        self.style.configure('Danger.TButton',
                             font=('Microsoft JhengHei', 11, 'bold'),
                             foreground='white',
                             focuscolor='none',
                             borderwidth=0,
                             padding=(20, 10))
        self.style.map('Danger.TButton',
                       background=[('active', '#e84393'), ('!active', accent_orange)])

        # 瀏覽按鈕樣式
        self.style.configure('Secondary.TButton',
                             font=('Microsoft JhengHei', 9),
                             foreground=text_primary,
                             focuscolor='none',
                             borderwidth=1,
                             padding=(15, 8))
        self.style.map('Secondary.TButton',
                       background=[('active', '#636e72'), ('!active', '#74b9ff')])

        # 標籤樣式
        self.style.configure('Title.TLabel',
                             background=bg_card,
                             foreground=text_primary,
                             font=('Microsoft JhengHei', 14))

        self.style.configure('Subtitle.TLabel',
                             background=bg_card,
                             foreground=text_secondary,
                             font=('Microsoft JhengHei', 10))

        # 輸入框樣式
        self.style.configure('Modern.TEntry',
                             fieldbackground='#3d3d3d',
                             foreground=text_primary,
                             borderwidth=1,
                             insertcolor=text_primary,
                             selectbackground=accent_blue)

        # 進度條樣式
        self.style.configure('Modern.Horizontal.TProgressbar',
                             background=accent_blue,
                             troughcolor='#3d3d3d',
                             borderwidth=0,
                             lightcolor=accent_blue,
                             darkcolor=accent_blue,
                             focuscolor='none')

        self.style.map('Modern.Horizontal.TProgressbar',
                       background=[('active', accent_blue),
                                   ('!active', accent_blue)])

    def setup_ui(self):
        # 主框架 - 減少外邊距
        main_frame = tk.Frame(self.root, bg='#1a1a1a')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)  # 從 20 改成 15

        # 標題區域 - 大幅縮小
        title_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        title_frame.pack(fill=tk.X, pady=(0, 10))  # 從 15 改成 10

        title_content = tk.Frame(title_frame, bg='#2d2d2d')
        title_content.pack(fill=tk.X, padx=20, pady=10)  # 從 25, 15 改成 20, 10

        # 縮小標題字體
        title_label = tk.Label(title_content,
                               text="📊 股票爬蟲程式",
                               font=('標楷體', 18, 'bold'),  # 從 22 改成 18
                               foreground='#00d4aa',
                               bg='#2d2d2d')
        title_label.pack()

        subtitle_label = tk.Label(title_content,
                                  text="專業級股票數據爬蟲工具 | Version 2.1",
                                  font=('標楷體', 11),  # 從 16 改成 11
                                  foreground='#b0b0b0',
                                  bg='#2d2d2d')
        subtitle_label.pack(pady=(3, 0))  # 從 5 改成 3

        # 輸入區域框架 - 縮小間距
        input_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        input_frame.pack(fill=tk.X, pady=(0, 10))  # 從 15 改成 10

        input_content = tk.Frame(input_frame, bg='#2d2d2d')
        input_content.pack(fill=tk.X, padx=15, pady=10)  # 從 20, 15 改成 15, 10

        input_title = tk.Label(input_content,
                               text="🔍 爬蟲設定",
                               font=('標楷體', 14, 'bold'),  # 從 16 改成 14
                               foreground='#00d4aa',
                               bg='#2d2d2d')
        input_title.pack(anchor=tk.W, pady=(0, 8))  # 從 10 改成 8

        # ===== 模板選擇區域 - 縮小間距 =====
        template_frame = tk.Frame(input_content, bg='#2d2d2d')
        template_frame.pack(fill=tk.X, pady=(0, 10))  # 從 15 改成 10

        tk.Label(template_frame,
                 text="📋 選擇分析模板",
                 font=('標楷體', 12, 'bold'),  # 從 14 改成 12
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 6))  # 從 10 改成 6

        # 卡片容器
        cards_container = tk.Frame(template_frame, bg='#2d2d2d')
        cards_container.pack(fill=tk.X)

        # 股票分析卡片
        self.stock_card = self.create_template_card(
            cards_container,
            title="📈 股票深度分析",
            descriptions=[
                "✓ 完整財務報表",
                "✓ 估值與成長分析",
                "✓ 關鍵財務比率",
                "✓ WACC 與 DCF"
            ],
            variable=self.stock_analysis_var,
            side=tk.LEFT
        )

        # 選擇權分析卡片
        self.option_card = self.create_template_card(
            cards_container,
            title="📊 選擇權鏈分析",
            descriptions=[
                "✓ 即時履約價資訊",
                "✓ 隱含波動率分析",
                "✓ 到期日結構",
                "✓ Greeks 數據"
            ],
            variable=self.option_analysis_var,
            side=tk.LEFT,
            padx=(10, 0)  # 從 15 改成 10
        )

        # 股票代碼輸入區 - 縮小間距
        stock_frame = tk.Frame(input_content, bg='#2d2d2d')
        stock_frame.pack(fill=tk.X, pady=(8, 6))  # 從 10 改成 8, 6

        tk.Label(stock_frame,
                 text="💼 股票代碼",
                 font=('標楷體', 12, 'bold'),  # 從 14 改成 12
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 4))  # 從 5 改成 4

        stocks_entry = tk.Entry(stock_frame,
                                textvariable=self.stocks_var,
                                font=('Consolas', 11),  # 從 12 改成 11
                                bg='#3d3d3d',
                                fg='#ffffff',
                                insertbackground='#00d4aa',
                                selectbackground='#00d4aa',
                                selectforeground='#000000',
                                relief='flat',
                                bd=2)
        stocks_entry.pack(fill=tk.X, ipady=5)  # 從 6 改成 5

        help_label = tk.Label(stock_frame,
                              text="💡 輸入股票代碼，多個代碼請用逗號分隔 (例如: NVDA, MSFT, AAPL, GOOGL)\n💡 代碼中若包含『-』請直接輸入(例如：BRK-B)\n💡 若輸入非美國股票代碼，部分資料將有缺失！",
                              font=('Times New Roman', 10),  # 從 12 改成 10
                              foreground='#ffb347',
                              bg='#2d2d2d',
                              justify=tk.LEFT)
        help_label.pack(anchor=tk.W, pady=(4, 0))  # 從 5 改成 4

        # 輸出資料夾選擇 - 縮小間距
        folder_frame = tk.Frame(input_content, bg='#2d2d2d')
        folder_frame.pack(fill=tk.X, pady=(6, 0))  # 從 10 改成 6

        tk.Label(folder_frame,
                 text="📁 輸出資料夾",
                 font=('標楷體', 12, 'bold'),  # 從 14 改成 12
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 4))  # 從 5 改成 4

        folder_input_frame = tk.Frame(folder_frame, bg='#2d2d2d')
        folder_input_frame.pack(fill=tk.X)

        folder_entry = tk.Entry(folder_input_frame,
                                textvariable=self.output_folder_var,
                                font=('Consolas', 11),  # 從 12 改成 11
                                bg='#3d3d3d',
                                fg='#ffffff',
                                insertbackground='#00d4aa',
                                relief='flat',
                                bd=2)
        folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=4)  # 從 5 改成 4

        browse_btn = tk.Button(folder_input_frame,
                               text="📂 瀏覽",
                               command=self.browse_folder,
                               font=('新細明體', 10, 'bold'),  # 從 12 改成 10
                               bg='#74b9ff',
                               fg='white',
                               activebackground='#0984e3',
                               activeforeground='white',
                               relief='flat',
                               bd=0,
                               cursor='hand2')
        browse_btn.pack(side=tk.RIGHT, padx=(6, 0), ipady=4, ipadx=10)  # 從 8, 5, 12 改成 6, 4, 10

        # 控制區域框架 - 大幅縮小
        control_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        control_frame.pack(fill=tk.X, pady=(0, 10))  # 從 15 改成 10

        control_content = tk.Frame(control_frame, bg='#2d2d2d')
        control_content.pack(fill=tk.X, padx=15, pady=10)  # 從 20, 15 改成 15, 10

        control_title = tk.Label(control_content,
                                 text="🎮 分析控制",
                                 font=('標楷體', 14, 'bold'),  # 從 16 改成 14
                                 foreground='#00d4aa',
                                 bg='#2d2d2d')
        control_title.pack(anchor=tk.W, pady=(0, 8))  # 從 10 改成 8

        # 按鈕區 - 縮小按鈕
        button_frame = tk.Frame(control_content, bg='#2d2d2d')
        button_frame.pack(pady=(0, 10))  # 從 15 改成 10

        self.start_btn = tk.Button(button_frame,
                                   text="🚀 開始爬蟲",
                                   command=self.start_analysis,
                                   font=('標楷體', 13, 'bold'),  # 從 15 改成 13
                                   bg='#00d4aa',
                                   fg='white',
                                   activebackground='#00b894',
                                   activeforeground='white',
                                   relief='flat',
                                   bd=0,
                                   cursor='hand2',
                                   width=12,  # 從 15 改成 12
                                   height=1)  # 從 2 改成 1
        self.start_btn.pack(side=tk.LEFT, padx=(0, 10))  # 從 15 改成 10

        self.stop_btn = tk.Button(button_frame,
                                  text="⏹️ 停止爬蟲",
                                  command=self.stop_analysis,
                                  font=('標楷體', 13, 'bold'),  # 從 15 改成 13
                                  bg='#ff6b35',
                                  fg='white',
                                  activebackground='#e84393',
                                  activeforeground='white',
                                  relief='flat',
                                  bd=0,
                                  cursor='hand2',
                                  width=12,  # 從 15 改成 12
                                  height=1,  # 從 2 改成 1
                                  state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT)

        # 進度區域 - 縮小間距
        progress_frame = tk.Frame(control_content, bg='#2d2d2d')
        progress_frame.pack(fill=tk.X, pady=(0, 8))  # 從 10 改成 8

        tk.Label(progress_frame,
                 text="📊 爬蟲進度",
                 font=('標楷體', 11, 'bold'),  # 從 12 改成 11
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 4))  # 從 5 改成 4

        progress_container = tk.Frame(progress_frame, bg='#3d3d3d', height=16)  # 從 20 改成 16
        progress_container.pack(fill=tk.X, pady=(0, 6))  # 從 8 改成 6
        progress_container.pack_propagate(False)

        self.progress = ttk.Progressbar(progress_container,
                                        mode='determinate',
                                        maximum=100,
                                        value=0,
                                        style='Modern.Horizontal.TProgressbar',
                                        length=400)
        self.progress.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        self.progress_percent_label = tk.Label(progress_frame,
                                               text="0%",
                                               font=('標楷體', 9, 'bold'),  # 從 10 改成 9
                                               foreground='#00d4aa',
                                               bg='#2d2d2d')
        self.progress_percent_label.pack(anchor=tk.W, pady=(2, 0))

        self.status_label = tk.Label(control_content,
                                     text="✅ 系統準備就緒",
                                     font=('標楷體', 12, 'bold'),  # 從 13 改成 12
                                     foreground='#00d4aa',
                                     bg='#2d2d2d')
        self.status_label.pack(pady=(8, 0))  # 從 10 改成 8

        # 日誌區域框架 - 這是最重要的，設定最小高度
        log_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        log_frame.pack(fill=tk.BOTH, expand=True)

        log_content = tk.Frame(log_frame, bg='#2d2d2d')
        log_content.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)  # 從 20, 15 改成 15, 10

        log_title = tk.Label(log_content,
                             text="📋 執行日誌",
                             font=('標楷體', 14, 'bold'),  # 從 16 改成 14
                             foreground='#00d4aa',
                             bg='#2d2d2d')
        log_title.pack(anchor=tk.W, pady=(0, 6))  # 從 8 改成 6

        # 日誌文字框 - 確保有足夠高度
        self.log_text = scrolledtext.ScrolledText(log_content,
                                                  font=('Consolas', 11),  # 從 12 改成 11
                                                  bg='#1a1a1a',
                                                  fg='#00ff00',
                                                  insertbackground='#00d4aa',
                                                  selectbackground='#00d4aa',
                                                  selectforeground='#000000',
                                                  relief='flat',
                                                  bd=2,
                                                  wrap=tk.WORD,
                                                  height=20)  # 🔥 新增：設定最小高度為 20 行
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # 初始化日誌
        self.log_text.insert(tk.END, "=== 股票爬蟲程式已啟動 ===\n")
        self.log_text.insert(tk.END, "系統準備就緒，請選擇模板並輸入股票代碼開始爬蟲...\n\n")

    def create_template_card(self, parent, title, descriptions, variable, side=tk.LEFT, padx=(0, 0)):
        """創建模板選擇卡片"""
        # 卡片外框
        card_frame = tk.Frame(parent, bg='#3d3d3d', relief='flat', bd=2, cursor='hand2')
        card_frame.pack(side=side, padx=padx, fill=tk.BOTH, expand=True)

        # 卡片內容容器
        card_content = tk.Frame(card_frame, bg='#3d3d3d')
        card_content.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)  # 從 15 改成 12

        # 標題
        title_label = tk.Label(card_content,
                               text=title,
                               font=('標楷體', 12, 'bold'),  # 從 14 改成 12
                               foreground='#ffffff',
                               bg='#3d3d3d')
        title_label.pack(anchor=tk.W, pady=(0, 8))  # 從 10 改成 8

        # 分隔線
        separator = tk.Frame(card_content, bg='#00d4aa', height=2)
        separator.pack(fill=tk.X, pady=(0, 8))  # 從 10 改成 8

        # 描述文字
        for desc in descriptions:
            desc_label = tk.Label(card_content,
                                  text=desc,
                                  font=('Microsoft JhengHei', 9),  # 從 10 改成 9
                                  foreground='#b0b0b0',
                                  bg='#3d3d3d',
                                  anchor=tk.W)
            desc_label.pack(anchor=tk.W, pady=1)  # 從 2 改成 1

        # 狀態標籤
        status_label = tk.Label(card_content,
                                text="[已選擇]" if variable.get() else "[點擊選擇]",
                                font=('標楷體', 10, 'bold'),  # 從 11 改成 10
                                foreground='#00d4aa' if variable.get() else '#666666',
                                bg='#3d3d3d')
        status_label.pack(pady=(10, 0))  # 從 15 改成 10

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
                card_frame.config(bg='#00d4aa', bd=3)
            else:
                card_frame.config(bg='#555555', bd=3)

        def on_leave(event):
            if variable.get():
                card_frame.config(bg='#00d4aa', bd=2)
            else:
                card_frame.config(bg='#3d3d3d', bd=2)

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
        """開始分析 - 加入模板選擇驗證"""
        # 檢查是否至少選擇一個模板
        do_stock_analysis = self.stock_analysis_var.get()
        do_option_analysis = self.option_analysis_var.get()

        if not do_stock_analysis and not do_option_analysis:
            messagebox.showwarning("⚠️ 警告", "請至少選擇一個分析模板！")
            return

        # 檢查對應的Excel模板
        if do_stock_analysis:
            if Fundamental_Excel_Template_Base64.strip() == "" or "請將您從轉換工具得到的" in Fundamental_Excel_Template_Base64:
                messagebox.showerror("❌ 錯誤",
                                     "請先設定 Fundamental_Excel_Template_Base64 變數！\n請將股票分析Excel模板轉換為base64後貼入程式碼中。")
                return

        if do_option_analysis:
            if Option_Chain_Excel_Template_Base64.strip() == "" or "請將您從轉換工具得到的" in Option_Chain_Excel_Template_Base64:
                messagebox.showerror("❌ 錯誤",
                                     "請先設定 Option_Chain_Excel_Template_Base64 變數！\n請將選擇權Excel模板轉換為base64後貼入程式碼中。")
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
            f"即將驗證並爬蟲以下股票：\n"
            f"📈 {', '.join(stocks)}\n\n"
            f"📋 分析模板：\n"
            f"   {templates_str}\n\n"
            f"🔍 系統將先驗證股票代碼有效性\n"
            f"📊 僅爬蟲有效的股票代碼\n"
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
        """立即強制停止分析並清理所有資源 - 改進版"""
        try:
            # 🔥 Step 1: 立即設定停止標誌
            self.is_running = False
            self.log("🛑 使用者請求立即停止，開始強制清理資源...")

            # 🔥 Step 2: 強制清理 Playwright 資源（最重要）
            with self.cleanup_lock:
                cleanup_tasks = []

                # 清理 Scraper
                if self.current_scraper:
                    self.log("🧹 正在關閉 Playwright 瀏覽器...")
                    try:
                        # 如果事件循環還在運行，使用 run_coroutine_threadsafe
                        if self.event_loop and self.event_loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(
                                self.current_scraper.cleanup(),
                                self.event_loop
                            )
                            # 等待最多 5 秒
                            future.result(timeout=5)
                        else:
                            # 事件循環已停止，創建新的循環來清理
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            new_loop.run_until_complete(self.current_scraper.cleanup())
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
                        # Manager 可能有自己的清理邏輯
                        if hasattr(self.current_manager, 'cleanup'):
                            self.current_manager.cleanup()
                    except Exception as e:
                        self.log(f"⚠️ 清理 Manager 時發生錯誤（已忽略）: {e}")
                    finally:
                        self.current_manager = None

            # 🔥 Step 3: 取消異步任務
            if self.current_task and not self.current_task.done():
                self.log("🚫 正在取消異步任務...")
                self.current_task.cancel()

            # 🔥 Step 4: 停止事件循環
            if self.event_loop and self.event_loop.is_running():
                self.log("🔄 正在停止事件循環...")
                try:
                    self.event_loop.call_soon_threadsafe(self.event_loop.stop)
                except Exception as e:
                    self.log(f"⚠️ 停止事件循環時發生錯誤（已忽略）: {e}")

            # 🔥 Step 5: 恢復 UI 狀態
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.progress['value'] = 0
            self.progress_percent_label.config(text="0%")
            self.update_status("爬蟲已停止")

            # 🔥 Step 6: 強制更新 UI
            self.root.update_idletasks()

            self.log("✅ 所有資源清理完成，系統已就緒")

        except Exception as e:
            # 即使發生錯誤也要確保 UI 恢復
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.progress['value'] = 0
            self.progress_percent_label.config(text="0%")
            self.log(f"⚠️ 停止過程中發生錯誤，但UI已恢復: {e}")

    def run_analysis(self, stocks):
        """執行分析的主函數"""
        try:
            # 創建新的事件循環並記錄引用
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)

            # 執行異步分析並記錄任務引用
            self.current_task = self.event_loop.create_task(self.async_analysis(stocks))
            self.event_loop.run_until_complete(self.current_task)

        except asyncio.CancelledError:
            # 處理任務被取消的情況
            self.log("🛑 異步任務已被成功取消")

        except Exception as e:
            # 只有在系統仍在運行時才顯示錯誤
            if self.is_running:
                self.log(f"❌ 發生錯誤：{str(e)}")
                messagebox.showerror("❌ 錯誤", f"爬蟲過程中發生錯誤：\n{str(e)}")
            else:
                self.log("ℹ️ 爬蟲已被使用者停止")

        finally:
            # 清理資源
            self.current_task = None
            self.current_thread = None
            self.event_loop = None

            # 只有在系統仍在運行時才恢復UI
            if self.is_running:
                self.start_btn.config(state=tk.NORMAL)
                self.stop_btn.config(state=tk.DISABLED)
                self.reset_progress()
                self.is_running = False

    async def async_analysis(self, stocks):
        """異步執行分析 - 支援雙模板選擇，並確保資源清理"""

        # 🔥 初始化資源引用
        scraper = None
        processor = None
        manager = None

        try:
            # 獲取選擇的模板
            do_stock_analysis = self.stock_analysis_var.get()
            do_option_analysis = self.option_analysis_var.get()

            # 構建模板說明
            templates_info = []
            if do_stock_analysis:
                templates_info.append("股票分析")
            if do_option_analysis:
                templates_info.append("選擇權分析")
            templates_str = " + ".join(templates_info)

            self.log("🎯" + "=" * 80)
            self.log("🚀 股票爬蟲系統啟動")
            self.log(f"📊 輸入股票：{', '.join(stocks)}")
            self.log(f"📢 輸入數量：{len(stocks)} 支")
            self.log(f"📋 分析模板：{templates_str}")
            self.log("🎯" + "=" * 80)

            def check_if_stopped():
                if not self.is_running:
                    self.log("🛑 檢測到停止信號，正在中止操作...")
                    raise asyncio.CancelledError("使用者請求停止")

            start_time = time.time()

            # 計算總步驟數
            total_steps = 0
            if do_stock_analysis and do_option_analysis:
                total_steps = 14
            elif do_stock_analysis:
                total_steps = 10
            elif do_option_analysis:
                total_steps = 5

            current_step = 0

            # ===== 驗證階段 =====
            validator = StockValidator(request_delay=1.5)
            valid_stocks = []
            invalid_stocks = []
            us_stocks = []
            non_us_stocks = []

            # 如果選擇了股票分析，執行完整驗證（含國籍檢查）
            if do_stock_analysis:
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "驗證股票代碼有效性")
                self.log(f"\n🔍 步驟 {current_step}/{total_steps}：正在驗證股票代碼...")

                valid_stocks, invalid_stocks = await validator.validate_stocks_async(
                    stocks, log_callback=self.log
                )

                if invalid_stocks:
                    self.log("\n⚠️ 發現無效股票代碼:")
                    for invalid_stock in invalid_stocks:
                        self.log(f"   ❌ {invalid_stock}")

                if not valid_stocks:
                    self.log("❌ 沒有找到任何有效的股票代碼，停止爬蟲")
                    self.update_status("爬蟲失敗：無有效股票代碼")
                    return

                self.log(f"\n✅ 有效股票代碼：{', '.join(valid_stocks)}")

                # 國籍檢查
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "檢查股票國籍")
                self.log(f"\n🌍 步驟 {current_step}/{total_steps}：正在檢查股票國籍...")

                us_stocks, non_us_stocks = await validator.check_stocks_nationality_async(
                    valid_stocks, log_callback=self.log
                )

                if non_us_stocks:
                    self.log("\n📋 國籍檢查摘要：")
                    self.log(f"   🇺🇸 美國股票 ({len(us_stocks)} 支)：{', '.join(us_stocks)}")
                    self.log(f"   🌍 非美國股票 ({len(non_us_stocks)} 支)：")
                    for stock in non_us_stocks:
                        country = validator.get_stock_country(stock)
                        self.log(f"      • {stock} ({country})")
                    self.log(f"   💡 說明：非美國股票在 roic.ai 的 financial 和 ratios 需付費，將自動跳過")

            # 如果只選擇權分析，只做基本驗證
            elif do_option_analysis:
                check_if_stopped()
                current_step += 1
                self.update_progress(current_step, total_steps, "驗證股票代碼有效性")
                self.log(f"\n🔍 步驟 {current_step}/{total_steps}：正在驗證股票代碼...")

                valid_stocks, invalid_stocks = await validator.validate_stocks_async(
                    stocks, log_callback=self.log
                )

                if invalid_stocks:
                    self.log("\n⚠️ 發現無效股票代碼:")
                    for invalid_stock in invalid_stocks:
                        self.log(f"   ❌ {invalid_stock}")

                if not valid_stocks:
                    self.log("❌ 沒有找到任何有效的股票代碼，停止爬蟲")
                    self.update_status("爬蟲失敗：無有效股票代碼")
                    return

                self.log(f"\n✅ 有效股票代碼：{', '.join(valid_stocks)}")

            self.log(f"\n🎯 最終處理清單：{', '.join(valid_stocks)}")
            self.log("🎯" + "=" * 80)

            # 準備股票字典
            stocks_dict = {
                'final_stocks': valid_stocks,
                'us_stocks': us_stocks,
                'non_us_stocks': non_us_stocks
            }

            # ===== 股票分析階段 =====
            saved_stock_files = []

            if do_stock_analysis:
                check_if_stopped()
                self.log("\n【第一階段：股票分析】")
                self.log("🎯" + "=" * 80)

                # 🔥 創建分析物件並保存引用
                self.update_status("初始化股票分析系統")
                self.log("🔧 正在初始化股票爬蟲系統...")

                scraper = StockScraper(stocks=stocks_dict, config=self.config, max_concurrent=3)
                processor = StockProcess(max_concurrent=2)
                manager = StockManager(scraper=scraper, processor=processor,
                                       stocks=stocks_dict, validator=validator, max_concurrent=3)

                # 🔥 保存到實例變數（供 stop_analysis 使用）
                self.current_scraper = scraper
                self.current_manager = manager

                self.log("✅ 股票爬蟲系統初始化完成")

                # 初始化 Excel 檔案
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 初始化 Excel 檔案")
                self.log(f"\n📄 步驟 {step_num}：[股票] 正在初始化 Excel 檔案...")

                success = await manager.initialize_excel_files()
                if not success:
                    self.log("❌ Excel 檔案初始化失敗，停止爬蟲")
                    self.update_status("爬蟲失敗：Excel 初始化錯誤")
                    return
                self.log("✅ Excel 檔案初始化完成")

                # Summary 和關鍵指標
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 抓取 Summary 和關鍵指標")
                self.log(f"\n📊 步驟 {step_num}：[股票] 正在抓取 Summary 和關鍵指標數據...")

                await manager.process_combined_summary_and_metrics()
                self.log("✅ Summary 和關鍵指標數據處理完成")

                # Financial 數據
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 處理 Financial 數據")
                self.log(f"\n💰 步驟 {step_num}：[股票] 正在處理 Financial 數據...")

                await manager.process_financial()
                self.log("✅ Financial 數據處理完成")

                # Ratios 數據
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 處理 Ratios 數據")
                self.log(f"\n📈 步驟 {step_num}：[股票] 正在處理 Ratios 數據...")

                await manager.process_ratios()
                self.log("✅ Ratios 數據處理完成")

                # 其他數據
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 抓取其他數據")
                self.log(f"\n📋 步驟 {step_num}：[股票] 正在抓取其他股票數據...")

                await manager.process_others_data()
                self.log("✅ 其他股票數據處理完成")

                # Revenue Growth 和 WACC
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 處理 Revenue Growth 和 WACC")
                self.log(f"\n📈 步驟 {step_num}：[股票] 正在處理 Revenue Growth 和 WACC 數據...")

                await manager.process_seekingalpha()
                await manager.process_wacc()
                self.log("✅ Revenue Growth 和 WACC 數據處理完成")

                # Trading View
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 處理 Trading View 資料")
                self.log(f"\n📈 步驟 {step_num}：[股票] 正在處理 Trading View 資料...")

                await manager.process_TradingView()
                self.log("✅ Trading View 資料處理完成")

                # 保存檔案
                check_if_stopped()
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[股票] 保存 Excel 檔案")
                self.log(f"\n💾 步驟 {step_num}：[股票] 正在保存 Excel 檔案...")

                output_folder = self.output_folder_var.get()
                saved_stock_files = manager.save_all_excel_files(output_folder)
                self.log(f"✅ 股票分析 Excel 檔案保存完成（{len(saved_stock_files)} 個檔案）")
                self.log("🎯" + "=" * 80)

            # ===== 選擇權分析階段 =====
            saved_option_files = []
            if do_option_analysis:
                check_if_stopped()
                self.log("\n【第二階段：選擇權分析】")
                self.log("🎯" + "=" * 80)

                # 如果股票分析沒執行，需要創建 manager
                if not do_stock_analysis:
                    self.update_status("初始化選擇權分析系統")
                    self.log("🔧 正在初始化選擇權爬蟲系統...")

                    scraper = StockScraper(stocks=stocks_dict, config=self.config, max_concurrent=3)
                    processor = StockProcess(max_concurrent=2)
                    manager = StockManager(scraper=scraper, processor=processor,
                                           stocks=stocks_dict, validator=validator, max_concurrent=3)

                    # 🔥 保存引用
                    self.current_scraper = scraper
                    self.current_manager = manager

                    self.log("✅ 選擇權爬蟲系統初始化完成")

                # 初始化選擇權 Excel
                current_step += 1
                step_num = f"{current_step}/{total_steps}"
                self.update_progress(current_step, total_steps, "[選擇權] 初始化 Excel 檔案")
                self.log(f"\n📄 步驟 {step_num}：[選擇權] 正在初始化 Excel 檔案...")

                try:
                    success = await manager.initialize_option_excel_files()
                    if not success:
                        self.log("⚠️ 選擇權 Excel 檔案初始化失敗")
                        if do_stock_analysis:
                            self.log("⚠️ 股票分析已完成，將跳過選擇權分析")
                            # 繼續執行，不中斷
                        else:
                            self.log("❌ 選擇權分析失敗，停止爬蟲")
                            self.update_status("爬蟲失敗：選擇權 Excel 初始化錯誤")
                            return
                    else:
                        self.log("✅ 選擇權 Excel 檔案初始化完成")

                        # 抓取 Barchart 數據
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 抓取 Barchart 波動率")
                        self.log(f"\n📊 步驟 {step_num}：[選擇權] 正在抓取 Barchart 波動率數據...")

                        await manager.process_barchart_for_options()
                        self.log("✅ Barchart 波動率數據處理完成")

                        # 抓取 Option Chain 數據
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 抓取 Option Chain 數據")
                        self.log(f"\n🔗 步驟 {step_num}：[選擇權] 正在抓取 Option Chain 數據...")

                        await manager.process_option_chains()
                        self.log("✅ Option Chain 數據處理完成")

                        # 保存選擇權檔案
                        check_if_stopped()
                        current_step += 1
                        step_num = f"{current_step}/{total_steps}"
                        self.update_progress(current_step, total_steps, "[選擇權] 保存 Excel 檔案")
                        self.log(f"\n💾 步驟 {step_num}：[選擇權] 正在保存選擇權 Excel 檔案...")

                        output_folder = self.output_folder_var.get()
                        saved_option_files = manager.save_all_option_excel_files(output_folder)
                        self.log(f"✅ 選擇權 Excel 檔案保存完成（{len(saved_option_files)} 個檔案）")

                except Exception as e:
                    self.log(f"⚠️ 選擇權分析過程發生錯誤: {e}")
                    if do_stock_analysis:
                        self.log("⚠️ 股票分析已完成，將繼續完成流程")
                        # 繼續執行，不中斷
                    else:
                        self.log("❌ 選擇權分析失敗，停止爬蟲")
                        raise e

                self.log("🎯" + "=" * 80)

            # 完成時設置進度條為 100%
            self.update_progress(total_steps, total_steps, "爬蟲完成！")

            # 計算執行時間
            end_time = time.time()
            execution_time = end_time - start_time

            # 顯示完成摘要
            self.log("\n" + "🎉" + "=" * 80)
            self.log("🎊 股票爬蟲完成！")
            self.log(f"⏱️ 總執行時間：{execution_time:.2f} 秒")
            self.log(f"📊 成功爬蟲股票：{len(valid_stocks)} 支")

            if do_stock_analysis:
                self.log(f"🇺🇸 美國股票：{len(us_stocks)} 支（完整數據）")
                if non_us_stocks:
                    self.log(f"🌍 非美國股票：{len(non_us_stocks)} 支（部分數據）")
                self.log(f"💾 股票分析檔案：{len(saved_stock_files)} 個")

            if do_option_analysis:
                self.log(f"💾 選擇權分析檔案：{len(saved_option_files)} 個")

            total_files = len(saved_stock_files) + len(saved_option_files)
            self.log(f"📁 保存位置：{self.output_folder_var.get()}")

            if saved_stock_files or saved_option_files:
                self.log("\n📋 已保存的檔案：")
                for file_path in saved_stock_files:
                    filename = os.path.basename(file_path)
                    self.log(f"   ✅ {filename}")
                for file_path in saved_option_files:
                    filename = os.path.basename(file_path)
                    self.log(f"   ✅ {filename}")

            self.log("🎉" + "=" * 80)

            self.update_status("爬蟲完成！")

            # 顯示完成對話框
            completion_msg = f"股票爬蟲已成功完成！\n\n"
            completion_msg += f"📊 爬蟲股票：{len(valid_stocks)} 支\n"
            if do_stock_analysis:
                completion_msg += f"🇺🇸 美國股票：{len(us_stocks)} 支（完整數據）\n"
                if non_us_stocks:
                    completion_msg += f"🌍 非美國股票：{len(non_us_stocks)} 支（部分數據）\n"
            completion_msg += f"⏱️ 執行時間：{execution_time:.1f} 秒\n"
            completion_msg += f"💾 保存檔案：{total_files} 個\n"
            completion_msg += f"📁 保存位置：{self.output_folder_var.get()}\n"

            messagebox.showinfo("🎉 爬蟲完成", completion_msg)


        except asyncio.CancelledError:

            # 任務被取消時的處理

            self.log("🛑 爬蟲任務已被使用者取消")

            self.update_status("爬蟲已停止")

            raise


        except Exception as e:

            # 發生錯誤時也要停止進度條

            self.reset_progress()

            error_msg = f"系統錯誤：{str(e)}"

            self.log(f"❌ {error_msg}")

            self.update_status("爬蟲失敗")

            messagebox.showerror("❌ 錯誤", f"爬蟲過程中發生錯誤：\n{str(e)}")

            raise e


        finally:

            # 🔥 確保資源被清理（無論是正常結束還是異常）

            self.log("🧹 開始最終清理...")

            try:

                # 清理 Scraper

                if scraper and scraper == self.current_scraper:
                    self.log("🧹 清理 Scraper 資源...")

                    await scraper.cleanup()

                    self.current_scraper = None

                    self.log("✅ Scraper 清理完成")

                # 清理 Manager（如果有自己的清理邏輯）

                if manager and manager == self.current_manager:

                    self.log("🧹 清理 Manager 資源...")

                    if hasattr(manager, 'cleanup'):
                        manager.cleanup()

                    self.current_manager = None

                    self.log("✅ Manager 清理完成")


            except Exception as e:

                self.log(f"⚠️ 最終清理時發生錯誤（已忽略）: {e}")

            self.log("✅ 最終清理完成")

    def run(self):
        """啟動GUI"""
        self.root.mainloop()


# ===== 程式進入點 =====
if __name__ == "__main__":
    app = StockAnalyzerGUI()
    app.run()