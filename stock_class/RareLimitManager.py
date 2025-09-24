import asyncio
import time
import random

class RateLimitManager:
    """統一的API速率限制管理器"""

    def __init__(self, request_delay=2.0):
        self.request_delay = request_delay
        self.last_request_time = {}

    async def rate_limit(self, api_key="yfinance"):
        """實施速率限制"""
        current_time = time.time()

        if api_key not in self.last_request_time:
            self.last_request_time[api_key] = 0

        time_since_last_request = current_time - self.last_request_time[api_key]

        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            # 添加隨機延遲，避免所有請求同時發送
            sleep_time += random.uniform(0.5, 1.5)
            await asyncio.sleep(sleep_time)

        self.last_request_time[api_key] = time.time()