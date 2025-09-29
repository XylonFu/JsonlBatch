# main.py
"""
JsonlBatch 框架主程序入口 (通用启动器)。

本文件负责组装框架的各个部分并启动任务，用户通常无需修改。
"""
import asyncio
import sys

# 从独立的配置文件中导入已配置好的 logger 实例
from logging_config import logger

from config import settings
from processor import JsonlBatchProcessor
from task import process_record, on_startup, on_shutdown

async def main():
    """异步主函数，负责编排整个处理流程。"""
    await logger.info("="*50)
    await logger.info(f"JsonlBatch v5.0 (Final) 异步处理器启动")
    
    try:
        # 配置注入：将配置实例传递给处理器
        processor = JsonlBatchProcessor(config=settings)
        
        # 运行处理器，并传入从 task.py 导入的业务逻辑和生命周期钩子
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
        await logger.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # 在顶层捕获 Ctrl+C，以提供更友好的退出信息
        print("\n程序被用户手动中断。")
