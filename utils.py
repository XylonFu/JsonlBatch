# utils.py
"""
通用工具模块。

存放与具体业务逻辑无关、可在框架内复用的组件，例如装饰器。
"""
import asyncio
import random
from functools import wraps
import aiologger

logger = aiologger.getLogger(__name__)

def retry_with_backoff(retries: int = 3, initial_delay: float = 1, backoff_factor: float = 2):
    """
    一个支持指数退避和随机抖动的异步函数重试装饰器。

    Args:
        retries (int): 最大重试次数。
        initial_delay (float): 初始等待延迟（秒）。
        backoff_factor (float): 每次重试后延迟的倍增因子。
    
    Returns:
        Callable: 应用了重试逻辑的装饰器。
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            for i in range(retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if i == retries:
                        await logger.error(f"函数 {func.__name__} 在 {retries} 次重试后最终失败。", exc_info=True)
                        raise
                    
                    # 增加随机抖动(jitter)，防止多个任务在同一时间点集中重试
                    jitter = delay * 0.2 * (random.random() - 0.5)
                    current_delay = delay + jitter
                    await logger.warning(f"函数 {func.__name__} 失败: {e}。将在 {current_delay:.2f} 秒后进行第 {i+1}/{retries} 次重试...")
                    await asyncio.sleep(current_delay)
                    delay *= backoff_factor
        return wrapper
    return decorator
