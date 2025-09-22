import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import asyncio
import os
from datetime import datetime
import time
from excel_template.excel_template import EXCEL_TEMPLATE_BASE64
from stock_class.StockScraper import StockScraper
from stock_class.StockProcess import StockProcess
from stock_class.StockManager import StockManager
from stock_class.StockValidator import StockValidator


# ===== GUI 部分 =====
class StockAnalyzerGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("股票爬蟲程式 v2.0")
        self.root.geometry("1400x1000")
        self.root.configure(bg='#1a1a1a')  # 深色背景
        self.root.minsize(1200, 900)

        # 設定樣式
        self.style = ttk.Style()
        self.style.theme_use('clam')

        # 自訂顏色主題
        self.setup_custom_styles()

        # 變數
        self.stocks_var = tk.StringVar()
        self.output_folder_var = tk.StringVar(value=os.getcwd())
        self.is_running = False

        self.setup_ui()

        # 新增：用於追蹤當前運行的任務和線程
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
                             background=accent_blue,  # 進度條填充顏色
                             troughcolor='#3d3d3d',  # 進度條背景顏色
                             borderwidth=0,  # 無邊框
                             lightcolor=accent_blue,  # 亮部顏色
                             darkcolor=accent_blue,  # 暗部顏色
                             focuscolor='none')  # 無焦點顏色

        # 新增：確保進度條在不同狀態下的顏色
        self.style.map('Modern.Horizontal.TProgressbar',
                       background=[('active', accent_blue),
                                   ('!active', accent_blue)])

    def setup_ui(self):
        # 主框架 - 添加漸層效果
        main_frame = tk.Frame(self.root, bg='#1a1a1a')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # 標題區域 - 縮小高度
        title_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        title_frame.pack(fill=tk.X, pady=(0, 15))

        # 縮小標題區域的內邊距
        title_content = tk.Frame(title_frame, bg='#2d2d2d')
        title_content.pack(fill=tk.X, padx=25, pady=15)

        # 縮小主標題字體
        title_label = tk.Label(title_content,
                               text="📊 股票爬蟲程式",
                               font=('標楷體', 22, 'bold'),  # 從28減少到22
                               foreground='#00d4aa',
                               bg='#2d2d2d')
        title_label.pack()

        # 縮小副標題字體和內容
        subtitle_label = tk.Label(title_content,
                                  text="專業級股票數據爬蟲工具 | Version 2.0",  # 合併成一行
                                  font=('標楷體', 16),  # 從18減少到12
                                  foreground='#b0b0b0',
                                  bg='#2d2d2d')
        subtitle_label.pack(pady=(5, 0))

        # 輸入區域框架 - 縮小間距
        input_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        input_frame.pack(fill=tk.X, pady=(0, 15))

        input_content = tk.Frame(input_frame, bg='#2d2d2d')
        input_content.pack(fill=tk.X, padx=20, pady=15)

        # 縮小區域標題
        input_title = tk.Label(input_content,
                               text="🔍 爬蟲設定",
                               font=('標楷體', 16, 'bold'),  # 從18減少到14
                               foreground='#00d4aa',
                               bg='#2d2d2d')
        input_title.pack(anchor=tk.W, pady=(0, 10))

        # 股票代碼輸入區 - 縮小間距和字體
        stock_frame = tk.Frame(input_content, bg='#2d2d2d')
        stock_frame.pack(fill=tk.X, pady=(0, 10))

        tk.Label(stock_frame,
                 text="💼 股票代碼",
                 font=('標楷體', 14, 'bold'),  # 從14減少到12
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 5))

        stocks_entry = tk.Entry(stock_frame,
                                textvariable=self.stocks_var,
                                font=('Consolas', 12),  # 從12減少到11
                                bg='#3d3d3d',
                                fg='#ffffff',
                                insertbackground='#00d4aa',
                                selectbackground='#00d4aa',
                                selectforeground='#000000',
                                relief='flat',
                                bd=2)
        stocks_entry.pack(fill=tk.X, ipady=6)

        # 縮小說明文字
        help_label = tk.Label(stock_frame,
                              text="💡 輸入股票代碼，多個代碼請用逗號分隔 (例如: NVDA, MSFT, AAPL, GOOGL)\n💡 請勿輸入非美國股票代碼",
                              font=('Times New Roman', 12),  # 從12減少到10
                              foreground='#ffb347',
                              bg='#2d2d2d',
                              justify=tk.LEFT)
        help_label.pack(anchor=tk.W, pady=(5, 0))

        # 輸出資料夾選擇 - 縮小間距
        folder_frame = tk.Frame(input_content, bg='#2d2d2d')
        folder_frame.pack(fill=tk.X, pady=(10, 0))

        tk.Label(folder_frame,
                 text="📁 輸出資料夾",
                 font=('標楷體', 14, 'bold'),  # 從14減少到12
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 5))

        folder_input_frame = tk.Frame(folder_frame, bg='#2d2d2d')
        folder_input_frame.pack(fill=tk.X)

        folder_entry = tk.Entry(folder_input_frame,
                                textvariable=self.output_folder_var,
                                font=('Consolas', 12),  # 從11減少到10
                                bg='#3d3d3d',
                                fg='#ffffff',
                                insertbackground='#00d4aa',
                                relief='flat',
                                bd=2)
        folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=5)

        browse_btn = tk.Button(folder_input_frame,
                               text="📂 瀏覽",
                               command=self.browse_folder,
                               font=('新細明體', 12, 'bold'),  # 從12減少到10
                               bg='#74b9ff',
                               fg='white',
                               activebackground='#0984e3',
                               activeforeground='white',
                               relief='flat',
                               bd=0,
                               cursor='hand2')
        browse_btn.pack(side=tk.RIGHT, padx=(8, 0), ipady=5, ipadx=12)

        # 控制區域框架 - 縮小間距
        control_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        control_frame.pack(fill=tk.X, pady=(0, 15))

        control_content = tk.Frame(control_frame, bg='#2d2d2d')
        control_content.pack(fill=tk.X, padx=20, pady=15)

        # 縮小控制區域標題
        control_title = tk.Label(control_content,
                                 text="🎮 分析控制",
                                 font=('標楷體', 16, 'bold'),  # 從18減少到14
                                 foreground='#00d4aa',
                                 bg='#2d2d2d')
        control_title.pack(anchor=tk.W, pady=(0, 10))

        # 按鈕區 - 縮小按鈕大小
        button_frame = tk.Frame(control_content, bg='#2d2d2d')
        button_frame.pack(pady=(0, 15))

        self.start_btn = tk.Button(button_frame,
                                   text="🚀 開始爬蟲",
                                   command=self.start_analysis,
                                   font=('標楷體', 15, 'bold'),  # 從16減少到13
                                   bg='#00d4aa',
                                   fg='white',
                                   activebackground='#00b894',
                                   activeforeground='white',
                                   relief='flat',
                                   bd=0,
                                   cursor='hand2',
                                   width=15,  # 從15減少到12
                                   height=2)  # 從2減少到1
        self.start_btn.pack(side=tk.LEFT, padx=(0, 15))

        self.stop_btn = tk.Button(button_frame,
                                  text="⏹️ 停止爬蟲",
                                  command=self.stop_analysis,
                                  font=('標楷體', 15, 'bold'),  # 從16減少到13
                                  bg='#ff6b35',
                                  fg='white',
                                  activebackground='#e84393',
                                  activeforeground='white',
                                  relief='flat',
                                  bd=0,
                                  cursor='hand2',
                                  width=15,  # 從15減少到12
                                  height=2,  # 從2減少到1
                                  state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT)

        # 進度區域 - 修改進度條設置
        progress_frame = tk.Frame(control_content, bg='#2d2d2d')
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        tk.Label(progress_frame,
                 text="📊 爬蟲進度",
                 font=('標楷體', 12, 'bold'),
                 foreground='#ffffff',
                 bg='#2d2d2d').pack(anchor=tk.W, pady=(0, 5))

        # 修正：進度條容器 - 增加高度讓進度條更明顯
        progress_container = tk.Frame(progress_frame, bg='#3d3d3d', height=20)  # 從 8 增加到 20
        progress_container.pack(fill=tk.X, pady=(0, 8))
        progress_container.pack_propagate(False)

        # 修正：進度條設置 - 添加更多屬性確保正常顯示
        self.progress = ttk.Progressbar(progress_container,
                                        mode='determinate',
                                        maximum=100,
                                        value=0,
                                        style='Modern.Horizontal.TProgressbar',
                                        length=400)  # 新增：設定長度
        self.progress.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)  # 新增：內邊距

        # 進度百分比標籤
        self.progress_percent_label = tk.Label(progress_frame,
                                               text="0%",
                                               font=('標楷體', 10, 'bold'),
                                               foreground='#00d4aa',
                                               bg='#2d2d2d')
        self.progress_percent_label.pack(anchor=tk.W, pady=(2, 0))

        # 需要在你的 setup_ui 方法中添加狀態標籤

        # 在進度條區域之後，日誌區域之前添加：
        self.status_label = tk.Label(control_content,
                                     text="✅ 系統準備就緒",
                                     font=('標楷體', 13, 'bold'),
                                     foreground='#00d4aa',
                                     bg='#2d2d2d')
        self.status_label.pack(pady=(10, 0))

        # 日誌區域框架 - 這裡是最重要的部分，讓它佔用更多空間
        log_frame = tk.Frame(main_frame, bg='#2d2d2d', relief='flat', bd=2)
        log_frame.pack(fill=tk.BOTH, expand=True)  # 確保日誌區域可以擴展

        log_content = tk.Frame(log_frame, bg='#2d2d2d')
        log_content.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        # 縮小日誌標題
        log_title = tk.Label(log_content,
                             text="📋 執行日誌",
                             font=('標楷體', 16, 'bold'),  # 從18減少到14
                             foreground='#00d4aa',
                             bg='#2d2d2d')
        log_title.pack(anchor=tk.W, pady=(0, 8))

        # 放大滾動文字框 - 這是關鍵改進
        self.log_text = scrolledtext.ScrolledText(log_content,
                                                  font=('Consolas', 12),  # 稍微增加字體大小，從11到12
                                                  bg='#1a1a1a',
                                                  fg='#00ff00',
                                                  insertbackground='#00d4aa',
                                                  selectbackground='#00d4aa',
                                                  selectforeground='#000000',
                                                  relief='flat',
                                                  bd=2,
                                                  wrap=tk.WORD)  # 添加自動換行
        self.log_text.pack(fill=tk.BOTH, expand=True)  # 確保日誌文字框能夠擴展

        # 初始化日誌
        self.log_text.insert(tk.END, "=== 股票爬蟲程式已啟動 ===\n")
        self.log_text.insert(tk.END, "系統準備就緒，請輸入股票代碼開始爬蟲...\n\n")

    def update_progress(self, current_step, total_steps, step_name=""):
        """更新進度條 - 帶動畫效果"""
        if total_steps > 0:
            target_progress = (current_step / total_steps) * 100
            current_progress = self.progress['value']

            # 如果進度需要增加，使用動畫效果
            if target_progress > current_progress:
                self.animate_progress_smooth(current_progress, target_progress, step_name, current_step, total_steps)
            else:
                # 如果進度不變或減少，直接設置
                self.progress['value'] = target_progress
                self.progress_percent_label.config(text=f"{target_progress:.1f}%")
                if step_name:
                    self.update_status(f"{step_name} ({current_step}/{total_steps})")
                self.root.update_idletasks()

    def animate_progress(self, start_value, end_value, step_name="", current_step=0, total_steps=0):
        """動畫效果填滿進度條"""
        # 計算動畫參數
        progress_diff = end_value - start_value
        animation_steps = max(int(progress_diff * 2), 20)  # 至少20步，確保動畫流暢
        step_increment = progress_diff / animation_steps
        delay_ms = max(10, int(800 / animation_steps))  # 總動畫時間約800ms

        def animate_step(step):
            if step <= animation_steps:
                # 計算當前進度值
                current_value = start_value + (step * step_increment)
                if step == animation_steps:
                    current_value = end_value  # 確保最後一步精確到目標值

                # 更新進度條
                self.progress['value'] = current_value
                self.progress_percent_label.config(text=f"{current_value:.1f}%")

                # 更新狀態（只在最後一步更新，避免閃爍）
                if step == animation_steps and step_name:
                    self.update_status(f"{step_name} ({current_step}/{total_steps})")

                self.root.update_idletasks()

                # 如果還沒到最後一步，繼續動畫
                if step < animation_steps:
                    self.root.after(delay_ms, lambda: animate_step(step + 1))

        # 開始動畫
        animate_step(0)

    # 可選：添加更精細的動畫控制
    def animate_progress_smooth(self, start_value, end_value, step_name="", current_step=0, total_steps=0):
        """更平滑的動畫效果 - 使用緩動函數"""
        import math

        progress_diff = end_value - start_value
        animation_steps = max(int(progress_diff * 3), 30)  # 更多步驟，更平滑
        total_duration = 1200  # 總動畫時間1.2秒
        delay_ms = int(total_duration / animation_steps)

        def ease_out_cubic(t):
            """緩出動畫函數 - 開始快，結束慢"""
            return 1 - pow(1 - t, 3)

        def animate_step(step):
            if step <= animation_steps:
                # 使用緩動函數計算進度
                t = step / animation_steps
                eased_t = ease_out_cubic(t)
                current_value = start_value + (progress_diff * eased_t)

                if step == animation_steps:
                    current_value = end_value

                # 更新UI
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

        # 根據訊息類型選擇顏色
        if "✅" in message or "成功" in message:
            color = "#00ff00"  # 綠色
        elif "❌" in message or "錯誤" in message or "失敗" in message:
            color = "#ff4757"  # 紅色
        elif "⚠️" in message or "警告" in message:
            color = "#ffa502"  # 橙色
        elif "🔄" in message or "處理" in message:
            color = "#3742fa"  # 藍色
        elif "🚀" in message or "開始" in message:
            color = "#ff6b35"  # 橙紅色
        else:
            color = "#ffffff"  # 白色

        # 配置顏色標籤
        tag_name = f"color_{color.replace('#', '')}"
        self.log_text.tag_configure(tag_name, foreground=color)
        self.log_text.tag_configure("timestamp", foreground="#70a1ff")

        # 插入訊息
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
        """開始分析 - 加入輸入驗證"""
        # 檢查Excel模板
        if EXCEL_TEMPLATE_BASE64.strip() == "" or "我的模板" in EXCEL_TEMPLATE_BASE64:
            messagebox.showerror("❌ 錯誤",
                                 "請先設定 EXCEL_TEMPLATE_BASE64 變數！\n請將Excel模板轉換為base64後貼入程式碼中。")
            return

        # 獲取輸入的股票代碼
        stocks_input = self.stocks_var.get().strip()
        if not stocks_input:
            messagebox.showwarning("⚠️ 警告", "請輸入至少一個股票代碼！")
            return

        # 處理股票代碼列表，移除空白和重複
        stocks_raw = [s.strip().upper() for s in stocks_input.split(',')]
        stocks = []

        # 過濾空白和重複的股票代碼
        seen = set()
        for stock in stocks_raw:
            if stock and stock not in seen:
                stocks.append(stock)
                seen.add(stock)

        if not stocks:
            messagebox.showwarning("⚠️ 警告", "請輸入有效的股票代碼！")
            return

        # 確認開始（顯示即將驗證的股票）
        confirmation_message = (
            f"即將驗證並爬蟲以下股票：\n"
            f"📈 {', '.join(stocks)}\n\n"
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

        # 重置進度條（不再使用 start()）
        self.reset_progress()

        # 在創建線程時記錄引用
        self.current_thread = threading.Thread(target=self.run_analysis, args=(stocks,))
        self.current_thread.daemon = True
        self.current_thread.start()

    # 3. 完全重寫 stop_analysis 方法 - 立即停止並恢復UI
    def stop_analysis(self):
        """立即停止分析並恢復UI狀態"""
        try:
            # 1. 立即設定停止標誌
            self.is_running = False

            # 2. 立即恢復UI狀態（不等待線程結束）
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)

            # 3. 重置進度條歸零
            self.progress['value'] = 0
            self.progress_percent_label.config(text="0%")

            # 4. 更新狀態標籤
            self.update_status("爬蟲已停止")

            # 5. 記錄停止訊息
            self.log("🛑 使用者請求立即停止爬蟲")
            self.log("✅ UI狀態已恢復，可以重新開始爬蟲")

            # 6. 嘗試取消當前的異步任務
            if self.current_task and not self.current_task.done():
                self.current_task.cancel()
                self.log("🚫 已取消正在執行的異步任務")

            # 7. 嘗試停止事件循環
            if self.event_loop and self.event_loop.is_running():
                self.event_loop.call_soon_threadsafe(self.event_loop.stop)
                self.log("🔄 已請求停止事件循環")

            # 8. 強制更新UI
            self.root.update_idletasks()

            self.log("✅ 停止操作完成，系統已就緒")

        except Exception as e:
            # 即使發生錯誤也要確保UI恢復
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

            # 只有在系統仍在運行時才恢復UI（避免重複恢復）
            if self.is_running:
                self.start_btn.config(state=tk.NORMAL)
                self.stop_btn.config(state=tk.DISABLED)
                self.reset_progress()
                self.is_running = False

    async def async_analysis(self, stocks):
        """異步執行分析 - 整合Summary和EPS/PE/MarketCap的合併抓取"""
        try:
            self.log("🎯" + "=" * 80)
            self.log("🚀 股票爬蟲系統啟動")
            self.log(f"📊 輸入股票：{', '.join(stocks)}")
            self.log(f"🔢 輸入數量：{len(stocks)} 支")
            self.log("🎯" + "=" * 80)

            # 在每個主要步驟前都檢查停止狀態
            def check_if_stopped():
                if not self.is_running:
                    self.log("🛑 檢測到停止信號，正在中止操作...")
                    raise asyncio.CancelledError("使用者請求停止")

            start_time = time.time()

            # 定義總步驟數（根據你的流程）
            total_steps = 9  # 9個主要步驟
            current_step = 0

            # 步驟 1：股票代碼驗證步驟
            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "驗證股票代碼有效性")
            self.log("\n🔍 步驟 1/9：正在驗證股票代碼...")

            validator = StockValidator()
            valid_stocks, invalid_stocks = await validator.validate_stocks_async(
                stocks, log_callback=self.log
            )

            # 如果有無效股票，顯示警告
            if invalid_stocks:
                self.log("\n⚠️ 發現無效股票代碼:")
                for invalid_stock in invalid_stocks:
                    self.log(f"   ❌ {invalid_stock}")

            # 如果沒有有效股票，停止分析
            if not valid_stocks:
                self.log("❌ 沒有找到任何有效的股票代碼，停止爬蟲")
                self.update_status("爬蟲失敗：無有效股票代碼")
                return

            # 更新要分析的股票列表
            stocks = valid_stocks
            self.log(f"\n✅ 將爬蟲以下有效股票：{', '.join(stocks)}")
            self.log("🎯" + "=" * 80)

            # 檢查是否被停止
            if not self.is_running:
                self.log("🛑 爬蟲被使用者停止")
                return

            # 創建分析物件（使用有效股票列表）
            self.update_status("初始化爬蟲系統")
            self.log("🔧 正在初始化爬蟲系統...")
            scraper = StockScraper(stocks=stocks, max_concurrent=3)
            processor = StockProcess(max_concurrent=2, request_delay=2.5)
            manager = StockManager(scraper, processor, max_concurrent=3)
            self.log("✅ 爬蟲系統初始化完成")

            # 步驟 2：初始化 Excel 檔案
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "初始化 Excel 檔案")
            self.log("\n📄 步驟 2/9：正在初始化 Excel 檔案...")

            success = await manager.initialize_excel_files(stocks)
            if not success:
                self.log("❌ Excel 檔案初始化失敗，停止爬蟲")
                self.update_status("爬蟲失敗：Excel 初始化錯誤")
                return

            self.log("✅ Excel 檔案初始化完成")

            # 步驟 3：抓取 Summary 和關鍵指標數據
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "抓取 Summary 和關鍵指標數據")
            self.log("\n📊 步驟 3/9：正在同時抓取 Summary 和 EPS/PE/MarketCap 數據...")

            await manager.process_combined_summary_and_metrics(stocks)
            self.log("✅ Summary 和關鍵指標數據處理完成")

            # 步驟 4：抓取 Financial 數據
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "抓取 Financial 數據")
            self.log("\n💰 步驟 4/9：正在抓取 Financial 數據...")

            await manager.process_financial(stocks)
            self.log("✅ Financial 數據處理完成")

            # 步驟 5：抓取 Ratios 數據
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "抓取 Ratios 數據")
            self.log("\n📈 步驟 5/9：正在抓取 Ratios 數據...")

            await manager.process_ratios(stocks)
            self.log("✅ Ratios 數據處理完成")

            # 步驟 6：抓取其他數據
            if not self.is_running:
                return

            current_step += 1
            self.update_progress(current_step, total_steps, "抓取其他股票數據")
            self.log("\n📋 步驟 6/9：正在抓取其他股票數據...")

            await manager.process_others_data(stocks)
            self.log("✅ 其他股票數據處理完成")

            # 步驟 6：處理 Revenue Growth 和 WACC 數據
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "處理 Revenue Growth 和 WACC 數據")
            self.log("\n📈 步驟 7/9：正在處理 Revenue Growth 和 WACC 數據...")

            # 處理 SeekingAlpha Revenue Growth 數據
            self.log("🔍 正在抓取 SeekingAlpha Revenue Growth 數據...")
            await manager.process_seekingalpha(stocks)
            self.log("✅ SeekingAlpha Revenue Growth 數據處理完成")

            # 處理 GuruFocus WACC 數據
            self.log("💰 正在抓取 GuruFocus WACC 數據...")
            await manager.process_wacc(stocks)
            self.log("✅ GuruFocus WACC 數據處理完成")

            # 步驟 7：處理 Trading View處理
            if not self.is_running:
                return

            check_if_stopped()
            current_step += 1
            self.update_progress(current_step, total_steps, "處理 EPS 成長率")
            self.log("\n📈 步驟 8/9：正在處理 Trading View資料...")

            await manager.process_TradingView(stocks=stocks)
            self.log("✅ Trading View資料處理完成")

            # 保存檔案
            if not self.is_running:
                return

            current_step += 1
            self.update_progress(current_step, total_steps, "保存 Excel 檔案")
            self.log("\n💾 步驟 9/9：正在保存 Excel 檔案...")

            output_folder = self.output_folder_var.get()
            saved_files = manager.save_all_excel_files(stocks, output_folder)

            # 完成時設置進度條為 100%
            self.update_progress(total_steps, total_steps, "爬蟲完成！")

            # 計算執行時間
            end_time = time.time()
            execution_time = end_time - start_time

            # 顯示完成摘要
            self.log("\n" + "🎉" + "=" * 80)
            self.log("🎊 股票爬蟲完成！")
            self.log(f"⏱️ 總執行時間：{execution_time:.2f} 秒")
            self.log(f"📊 成功爬蟲股票：{len(stocks)} 支")
            self.log(f"💾 保存檔案數量：{len(saved_files)} 個")
            self.log(f"📁 保存位置：{output_folder}")
            # self.log(f"🚀 效能提升：合併抓取減少了約 {len(stocks)} 次網絡請求")

            if saved_files:
                self.log("\n📋 已保存的檔案：")
                for file_path in saved_files:
                    filename = os.path.basename(file_path)
                    self.log(f"   ✅ {filename}")

            self.log("🎉" + "=" * 80)

            self.update_status("爬蟲完成！")

            # 顯示完成對話框
            messagebox.showinfo(
                "🎉 爬蟲完成",
                f"股票爬蟲已成功完成！\n\n"
                f"📊 爬蟲股票：{len(stocks)} 支\n"
                f"⏱️ 執行時間：{execution_time:.1f} 秒\n"
                f"💾 保存檔案：{len(saved_files)} 個\n"
                f"📁 保存位置：{output_folder}\n"
                # f"🚀 效能優化：減少 {len(stocks)} 次重複請求"
            )

        except asyncio.CancelledError:
            # 任務被取消時的處理
            self.log("🛑 爬蟲任務已被成功取消")
            self.update_status("爬蟲已停止")
            raise  # 重新拋出以確保任務正確終止

        except Exception as e:

            # 發生錯誤時也要停止進度條

            self.reset_progress()
            error_msg = f"系統錯誤：{str(e)}"
            self.log(f"❌ {error_msg}")
            self.update_status("爬蟲失敗")
            messagebox.showerror("❌ 錯誤", f"爬蟲過程中發生錯誤：\n{str(e)}")

            raise e

    def run(self):
        """啟動GUI"""
        self.root.mainloop()