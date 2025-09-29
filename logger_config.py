# logging_config.py
"""
专用的异步日志系统配置文件。

此模块创建并配置一个全局共享的 aiologger 实例。
其他所有模块都应从这里导入 `logger` 对象，以确保日志配置的全局统一。
"""
import sys
import os
import logging
from aiologger import Logger
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.handlers.files import AsyncFileHandler
from aiologger.formatters.base import Formatter

from config import settings

def setup_logger() -> Logger:
    """
    创建并配置一个全局的、完全自定义的异步 Logger 实例。
    
    Returns:
        Logger: 一个配置了控制台和文件输出的 aiologger 实例。
    """
    # 使用标准库 logging 的级别常量来解析用户配置，这是最稳健的方式。
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    
    # 创建一个新的 Logger 实例，不使用默认 handlers，以便完全自定义。
    log_instance = Logger(name='JsonlBatch', level=log_level)
    
    # 定义所有 handler 共享的格式化器。
    formatter = Formatter(
        '%(asctime)s - %(name)-15s - %(levelname)-8s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 创建并添加控制台 Handler。
    console_handler = AsyncStreamHandler(stream=sys.stdout)
    console_handler.formatter = formatter
    log_instance.add_handler(console_handler)

    # 创建并添加文件 Handler。
    try:
        log_dir = os.path.dirname(settings.LOG_FILE)
        if log_dir: 
            os.makedirs(log_dir, exist_ok=True)
        
        file_handler = AsyncFileHandler(filename=settings.LOG_FILE, mode='a', encoding='utf-8')
        file_handler.formatter = formatter
        log_instance.add_handler(file_handler)
    except OSError as e:
        # 在日志系统完全可用前，使用 print 输出到 stderr 以确保错误可见。
        print(f"CRITICAL: 无法配置异步日志文件处理器: {e}", file=sys.stderr)

    return log_instance

# 创建并导出全局唯一的 logger 实例。
logger = setup_logger()
