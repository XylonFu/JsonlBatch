# processor.py
"""
JsonlBatch 框架核心处理器 (完全异步版)。

封装了并发控制、速率控制、断点续跑、异步批量I/O、异步日志记录和进度显示等核心功能。
通过生命周期钩子和配置注入，实现了与业务逻辑和配置的完全解耦。
"""
import asyncio
import json
import os
import time
from typing import Callable, Dict, Any, List, Coroutine, TypeAlias, Protocol

import aiofiles
import aiohttp
from tqdm import tqdm

from logging_config import logger
from utils import retry_with_backoff

# --- 类型定义，用于清晰地定义接口和函数签名 ---
ProcessFunction: TypeAlias = Callable[[Dict[str, Any], Dict[str, Any]], Coroutine[Any, Any, Dict[str, Any] | None]]
LifecycleHook: TypeAlias = Callable[..., Coroutine[Any, Any, Any]]

class ConfigProtocol(Protocol):
    """定义期望的配置对象的结构（“形状”），用于更严格的类型提示。"""
    INPUT_FILE: str; OUTPUT_FILE: str; ERROR_FILE: str; LOG_FILE: str
    ID_KEY: str; RERUN_KEY: str | None
    MAX_CONCURRENCY: int; REQUESTS_PER_MINUTE: int; WRITE_BATCH_SIZE: int
    MAX_RETRIES: int; RETRY_INITIAL_DELAY: float

class JsonlBatchProcessor:
    """用于网络I/O密集型任务的异步JSONL文件处理器。"""

    def __init__(self, config: ConfigProtocol):
        self.config = config
        self.processed_ids: set = set()
        self._file_write_lock = asyncio.Lock()
        self._rate_limit_lock = asyncio.Lock()
        self._last_request_time = 0

    async def _ensure_dir_exists(self):
        """确保所有在配置中定义的输出目录都存在。"""
        for file_path in [self.config.OUTPUT_FILE, self.config.ERROR_FILE, self.config.LOG_FILE]:
            dir_name = os.path.dirname(file_path)
            if dir_name: os.makedirs(dir_name, exist_ok=True)

    async def _load_processed_state(self):
        """从输出文件异步加载已处理记录的状态，以实现断点续跑。"""
        if not os.path.exists(self.config.OUTPUT_FILE): return
        
        await logger.info(f"正在从 '{self.config.OUTPUT_FILE}' 加载已处理记录...")
        async with aiofiles.open(self.config.OUTPUT_FILE, 'r', encoding='utf-8') as f:
            async for line in f:
                try:
                    data = json.loads(line)
                    record_id = data.get(self.config.ID_KEY)
                    if not record_id: continue
                    if self.config.RERUN_KEY and self.config.RERUN_KEY in data: continue
                    self.processed_ids.add(record_id)
                except json.JSONDecodeError:
                    await logger.warning(f"解析输出文件行失败，已跳过: '{line.strip()}'")
        await logger.info(f"加载完成。找到 {len(self.processed_ids)} 条可跳过的已处理记录。")

    async def _get_tasks_to_process(self) -> List[Dict[str, Any]]:
        """从输入文件异步读取数据，并根据已处理ID集合进行过滤。"""
        if not os.path.exists(self.config.INPUT_FILE):
            await logger.error(f"输入文件未找到: '{self.config.INPUT_FILE}'。请检查路径配置。")
            return []
        
        tasks = []
        await logger.info(f"正在从 '{self.config.INPUT_FILE}' 筛选待处理任务...")
        async with aiofiles.open(self.config.INPUT_FILE, 'r', encoding='utf-8') as f:
            line_num = 1
            async for line in f:
                try:
                    data = json.loads(line)
                    record_id = data.get(self.config.ID_KEY)
                    if not record_id:
                        await logger.warning(f"输入文件第 {line_num} 行记录缺少ID键 '{self.config.ID_KEY}'，已跳过。")
                        continue
                    if record_id not in self.processed_ids:
                        tasks.append(data)
                except json.JSONDecodeError:
                    await logger.warning(f"解析输入文件第 {line_num} 行失败，已跳过: '{line.strip()}'")
                line_num += 1
        await logger.info(f"筛选完成。共找到 {len(tasks)} 个新任务需要处理。")
        return tasks

    async def _write_batch(self, batch: List[Dict], file_path: str):
        """将一个批次的数据以异步、安全的方式追加写入文件。"""
        if not batch: return
        try:
            async with self._file_write_lock:
                async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
                    for item in batch:
                        await f.write(json.dumps(item, ensure_ascii=False) + '\n')
        except OSError as e:
            await logger.error(f"异步写入文件 '{file_path}' 失败: {e}", exc_info=True)

    async def _run_processing_loop(self, tasks: List, process_func: ProcessFunction, context: Dict) -> tuple[int, int]:
        """内部异步循环，负责并发执行、结果收集和进度显示。"""
        results_batch, errors_batch = [], []
        success_count, error_count = 0, 0
        semaphore = asyncio.Semaphore(self.config.MAX_CONCURRENCY)
        
        rpm = self.config.REQUESTS_PER_MINUTE
        request_delay = 60.0 / rpm if rpm and rpm > 0 else 0
        if request_delay > 0:
            await logger.info(f"速率限制已启用: 每分钟 {rpm} 次请求 (请求最小间隔: {request_delay:.2f} 秒)。")

        async def rate_limited_process(task: Dict):
            if request_delay > 0:
                async with self._rate_limit_lock:
                    now = time.monotonic()
                    elapsed = now - self._last_request_time
                    if elapsed < request_delay:
                        await asyncio.sleep(request_delay - elapsed)
                    self._last_request_time = time.monotonic()
            return await process_func(task, context)
        
        robust_processor = retry_with_backoff(
            retries=self.config.MAX_RETRIES,
            initial_delay=self.config.RETRY_INITIAL_DELAY
        )(rate_limited_process)

        async def worker(task: Dict):
            async with semaphore:
                try:
                    return await robust_processor(task)
                except Exception as e:
                    return e, task

        coroutines = [worker(task) for task in tasks]
        pbar = tqdm(asyncio.as_completed(coroutines), total=len(tasks), desc="处理中")
        
        try:
            for future in pbar:
                result = await future
                if isinstance(result, tuple) and isinstance(result[0], Exception):
                    exc, original_task = result
                    task_id = original_task.get(self.config.ID_KEY, "N/A")
                    await logger.error(f"记录 '{task_id}' 在所有重试后处理失败: {exc}", exc_info=False)
                    errors_batch.append({ "record_id": task_id, "error_message": str(exc), "original_record": original_task })
                    error_count += 1
                elif result is not None:
                    results_batch.append(result)
                    success_count += 1

                pbar.set_description(f"处理中 | 成功: {success_count}, 失败: {error_count}")

                if len(results_batch) >= self.config.WRITE_BATCH_SIZE:
                    await self._write_batch(results_batch, self.config.OUTPUT_FILE); results_batch = []
                if len(errors_batch) >= self.config.WRITE_BATCH_SIZE:
                    await self._write_batch(errors_batch, self.config.ERROR_FILE); errors_batch = []
        finally:
            await logger.info("处理循环结束或中断，正在写入剩余批次数据...")
            await self._write_batch(results_batch, self.config.OUTPUT_FILE)
            await self._write_batch(errors_batch, self.config.ERROR_FILE)
        
        return success_count, error_count

    async def run(self, process_function: ProcessFunction, on_startup: LifecycleHook | None = None, on_shutdown: LifecycleHook | None = None):
        """启动处理流程的公共入口方法。"""
        await self._ensure_dir_exists()
        await self._load_processed_state()
        tasks_to_process = await self._get_tasks_to_process()
        
        skipped_count = len(self.processed_ids)
        if not tasks_to_process:
            await logger.info(f"无需处理新任务。已跳过 {skipped_count} 条已处理记录。程序结束。")
            return
        
        context = {}
        start_time = time.monotonic()
        try:
            if on_startup:
                context = await on_startup() or {}

            async with aiohttp.ClientSession() as session:
                context['session'] = session
                success_count, error_count = await self._run_processing_loop(tasks_to_process, process_function, context)
        finally:
            if on_shutdown:
                await on_shutdown(context)
            
            end_time = time.monotonic()
            duration = end_time - start_time
            
            initial_task_count = len(tasks_to_process)

            await logger.info("="*20 + " 任务最终统计 " + "="*20)
            await logger.info(f"总计耗时: {duration:.2f} 秒")
            await logger.info(f"跳过的已处理记录: {skipped_count}")
            await logger.info(f"本次计划处理任务: {initial_task_count}")
            await logger.info(f"  - 成功: {success_count}")
            await logger.info(f"  - 失败: {error_count}")
            await logger.info(f"成功结果已保存至: '{self.config.OUTPUT_FILE}'")
            if error_count > 0:
                 await logger.warning(f"失败记录已保存至: '{self.config.ERROR_FILE}'")
            await logger.info("="*58)
