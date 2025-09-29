# user_logic.py
"""
用户业务逻辑模块 (User Logic Module)。

本文件是用户唯一需要关注和修改的地方，用于定义一个完整任务的全部自定义逻辑，
包括：资源初始化(`on_startup`)、核心处理(`process_record`)和资源清理(`on_shutdown`)。
"""
from typing import Dict, Any
import aiologger

# 从工具模块导入可复用的组件
from utils import retry_with_backoff

# 获取一个异步 logger 实例
logger = aiologger.getLogger(__name__)

# ==============================================================================
# 1. 定义生命周期钩子 (Lifecycle Hooks)
# ==============================================================================
async def on_startup() -> Dict[str, Any]:
    """
    在所有任务开始前执行一次，用于异步初始化所有共享资源。
    
    Returns:
        一个包含共享资源的上下文（context）字典，将被传递给每个 `process_record` 调用。
    """
    await logger.info("正在执行 on_startup 钩子：初始化共享资源...")
    # 示例：如果您需要一个自定义的SDK客户端
    # from some_sdk import MyAsyncSDKClient
    # sdk_client = MyAsyncSDKClient(api_key="...")
    # await logger.info("自定义SDK客户端已创建。")
    # return {"my_sdk_client": sdk_client}
    return {}

async def on_shutdown(context: Dict[str, Any]):
    """
    在所有任务结束后执行一次，用于清理和释放资源。
    
    Args:
        context (Dict[str, Any]): on_startup 返回的上下文。
    """
    await logger.info("正在执行 on_shutdown 钩子：清理资源...")
    # 示例：如果您的客户端需要手动关闭
    # if "my_sdk_client" in context:
    #     await context["my_sdk_client"].close()
    #     await logger.info("自定义SDK客户端已关闭。")
    pass

# ==============================================================================
# 2. 核心业务逻辑实现 (Core Business Logic)
# ==============================================================================
@retry_with_backoff(retries=3, initial_delay=2)
async def process_record(record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    处理单条记录的异步函数。

    Args:
        record (Dict[str, Any]): 从输入文件中读取并解析的一条数据。
        context (Dict[str, Any]): 包含所有共享资源的上下文。
                                   框架会自动向此上下文中注入 `context['session']` (aiohttp.ClientSession)。

    Returns:
        Dict[str, Any] | None: 处理成功后的记录，将被写入输出文件。返回 `None` 则跳过写入。
    
    Raises:
        Exception: 任何未捕获的异常都将导致此任务被标记为失败，并由框架记录。
    """
    # --- (最佳实践) 幂等性检查 ---
    # 对于关键的写操作，建议先检查目标系统中是否已存在该记录，以避免重复处理。
    # if context.get("db_client") and await context["db_client"].exists(record['id']):
    #     await logger.warning(f"ID '{record['id']}' 已存在于目标数据库中，跳过处理。")
    #     return {"id": record['id'], "status": "skipped_as_exists"}

    # --- 从上下文中获取共享资源 ---
    session = context['session']
    
    # --- 核心业务逻辑 (示例) ---
    record_id = record.get("id", "N/A")
    url = f"https://httpbin.org/get"
    params = {'id': record_id, 'data': record.get('data')}
    
    async with session.get(url, params=params, timeout=15) as response:
        response.raise_for_status()
        api_data = await response.json()
        
        record["processed_by"] = "JsonlBatch v3.2 Final"
        record["api_args"] = api_data["args"]
        return record
