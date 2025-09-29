# config.py
"""
JsonlBatch 框架的中央配置文件 (类模式)。

用户应在此文件中修改参数以适配不同的数据处理任务。
"""
from typing import ClassVar

class Settings:
    """
    一个用于封装所有框架配置的类。
    
    该模式提供了优秀的结构、IDE自动补全支持和未来的可扩展性。
    """
    # --- 1. 文件路径配置 ---
    # 输入的JSONL文件路径，每行一个JSON对象。
    INPUT_FILE: ClassVar[str] = "data/input.jsonl"

    # 成功处理后的记录所输出的JSONL文件路径。
    OUTPUT_FILE: ClassVar[str] = "data/output.jsonl"

    # 处理失败的记录及错误信息将被写入此JSONL文件。
    ERROR_FILE: ClassVar[str] = "data/error.jsonl"

    # 异步日志文件的路径。
    LOG_FILE: ClassVar[str] = "logs/processor.log"

    # --- 2. 核心标识符配置 ---
    # 记录的唯一标识符键名，用于断点续跑。
    # Example: 在 `{"id": "user_123", ...}` 中, ID_KEY 应为 "id"。
    ID_KEY: ClassVar[str] = "id"

    # (可选) 强制重跑标识符的键名。设为 `None` 可禁用此功能。
    # 如果 `OUTPUT_FILE` 中某条记录包含此键，该记录将在下次运行时被重新处理。
    RERUN_KEY: ClassVar[str | None] = "force_rerun"

    # --- 3. 性能配置 ---
    # 最大并发任务数，需根据目标API的速率限制进行调整。
    MAX_CONCURRENCY: ClassVar[int] = 100

    # 结果写入文件的批量大小，以减少I/O开销。
    WRITE_BATCH_SIZE: ClassVar[int] = 100

    # --- 4. 日志配置 ---
    # 日志级别，可选值: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    LOG_LEVEL: ClassVar[str] = "INFO"

# 创建一个全局唯一的配置实例，供其他模块导入。
settings = Settings()
