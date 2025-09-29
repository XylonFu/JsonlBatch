# utils.py
"""
通用工具模块。

存放与具体业务逻辑无关、可在框架内复用的组件，例如装饰器。
"""
import asyncio
import random
from functools import wraps
from typing import Callable, Coroutine, Any, TypeVar

from aiologger import logger

# 定义一个类型变量，用于精确注解被装饰的异步函数，以保持类型提示的完整性。
F = TypeVar('F', bound=Callable[..., Coroutine[Any, Any, Any]])

def retry_with_backoff(retries: int = 3, initial_delay: float = 1, backoff_factor: float = 2) -> Callable[[F], F]:
    """
    一个支持指数退避和随机抖动的异步函数重试装饰器。

    Args:
        retries (int): 最大重试次数。
        initial_delay (float): 初始等待延迟（秒）。
        backoff_factor (float): 每次重试后延迟的倍增因子。
    
    Returns:
        Callable: 一个接收异步函数并返回带有重试逻辑的异步函数的装饰器。
    """
    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            for i in range(retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if i == retries:
                        await logger.error(f"函数 {func.__name__} 在 {retries} 次重试后最终失败。", exc_info=True)
                        raise
                    
                    # 增加随机抖动(jitter)，防止多个任务在同一时间点集中重试。
                    jitter = delay * 0.2 * (random.random() - 0.5)
                    current_delay = delay + jitter
                    await logger.warning(f"函数 {func.__name__} 失败: {e}。将在 {current_delay:.2f} 秒后进行第 {i+1}/{retries} 次重试...")
                    await asyncio.sleep(current_delay)
                    delay *= backoff_factor
            # 此处代码理论上不可达，仅为满足静态类型检查器的要求。
            raise RuntimeError("重试逻辑异常退出")
        return wrapper # type: ignore
    return decorator
