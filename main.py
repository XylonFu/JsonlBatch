# main.py
"""
JsonlBatch 框架主程序入口 (通用启动器)。

本文件负责组装框架的各个部分并启动任务，通常情况下用户无需修改此文件。
"""
import asyncio
import os
import sys

from aiologger import logger as aiologger
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.handlers.files import AsyncFileHandler
from aiologger.formatters.base import Formatter

# 导入配置、核心处理器以及用户定义的全部逻辑
from config import settings
from core_processor import JsonlBatchProcessor
from user_logic import process_record, on_startup, on_shutdown

# 获取一个异步 logger 实例
logger = aiologger.get_logger(__name__)

def setup_logging():
    """配置 aiologger 日志系统，使其能够同时异步输出到控制台和文件。"""
    log_level = getattr(aiologger, settings.LOG_LEVEL.upper(), aiologger.INFO)
    root_logger = aiologger.get_logger()
    root_logger.level = log_level
    formatter = Formatter('%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    console_handler = AsyncStreamHandler(stream=sys.stdout)
    console_handler.formatter = formatter
    root_logger.add_handler(console_handler)

    try:
        log_dir = os.path.dirname(settings.LOG_FILE)
        if log_dir: os.makedirs(log_dir, exist_ok=True)
        
        file_handler = AsyncFileHandler(filename=settings.LOG_FILE, mode='a', encoding='utf-8')
        file_handler.formatter = formatter
        root_logger.add_handler(file_handler)
    except Exception as e:
        print(f"CRITICAL: 无法配置异步日志文件处理器: {e}")

async def main():
    """异步主函数，负责编排整个处理流程。"""
    await logger.info("="*50)
    await logger.info(f"JsonlBatch v3.2 (Final) 异步处理器启动")
    
    try:
        # 1. 配置注入：将配置实例传递给处理器
        processor = JsonlBatchProcessor(config=settings)
        
        # 2. 运行处理器，并传入从 user_logic.py 导入的业务逻辑和生命周期钩子
        await processor.run(
            process_function=process_record,
            on_startup=on_startup,
            on_shutdown=on_shutdown
        )

    except Exception:
        await logger.critical("处理器在顶层发生未捕获的严重错误，程序终止。", exc_info=True)
    finally:
        await logger.info("JsonlBatch 处理器运行结束")
        # 终极保障：无论程序如何退出（即使 on_startup 失败），都尝试关闭 logger
        await aiologger.shutdown()

if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # 在顶层捕获 Ctrl+C，以提供更友好的退出信息
        print("\n程序被用户手动中断。")
