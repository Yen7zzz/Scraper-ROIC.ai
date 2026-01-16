"""
通用工具模組
提供跨環境（開發/打包）的資源路徑管理
"""

import os
import sys


def get_resource_path(relative_path):
    """
    取得資源檔案的絕對路徑

    適用於：
    - 開發環境（直接執行 .py）
    - PyInstaller 打包後的執行檔

    Args:
        relative_path: 相對於專案根目錄的路徑
                      例如：'logo.ico', 'excel_template/template.xlsm'

    Returns:
        str: 資源的絕對路徑

    Example:
        >>> icon_path = get_resource_path('logo.ico')
        >>> template_path = get_resource_path('excel_template/template.xlsm')
    """
    if getattr(sys, 'frozen', False):
        # 打包後：使用 PyInstaller 的臨時解壓縮目錄
        base_path = sys._MEIPASS
    else:
        # 開發環境：使用專案根目錄
        base_path = os.path.dirname(os.path.abspath(__file__))

        # 🔥 如果當前檔案在子目錄，往上找到專案根目錄
        while True:
            # 檢查是否已經到達專案根目錄（通常有 main.py 或 .git）
            if os.path.exists(os.path.join(base_path, 'main.py')):
                break
            if os.path.exists(os.path.join(base_path, '.git')):
                break

            parent = os.path.dirname(base_path)
            if parent == base_path:  # 已經到達根目錄
                break
            base_path = parent

    return os.path.join(base_path, relative_path)


def get_base_path():
    """
    取得專案根目錄或打包後的臨時目錄

    Returns:
        str: 基礎路徑
    """
    if getattr(sys, 'frozen', False):
        return sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))

        while True:
            if os.path.exists(os.path.join(base_path, 'main.py')):
                break
            if os.path.exists(os.path.join(base_path, '.git')):
                break

            parent = os.path.dirname(base_path)
            if parent == base_path:
                break
            base_path = parent

        return base_path


# 🔥 額外工具：檢查資源是否存在
def resource_exists(relative_path):
    """
    檢查資源檔案是否存在

    Args:
        relative_path: 相對於專案根目錄的路徑

    Returns:
        bool: 檔案是否存在
    """
    return os.path.exists(get_resource_path(relative_path))