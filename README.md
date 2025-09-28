# JsonlBatch: High-Performance Async Processor

[](https://www.python.org/downloads/)
[](https://opensource.org/licenses/MIT)
[](https://www.google.com/search?q=)

JsonlBatch is a high-performance Python framework for concurrent processing of JSONL files, architected for I/O-intensive tasks.

It provides an elegant, out-of-the-box solution for processing large JSONL datasets where each line requires an I/O-bound operation, such as calling a web API, querying a database, or generating vector embeddings. The framework abstracts away the complexities of concurrency, state management, error handling, and logging, allowing you to focus purely on your business logic.

### Ideal Use Cases

  - **Data Enrichment**: Calling external APIs to enrich records in a large dataset.
  - **Batch API Calls**: Sending a high volume of requests to a rate-limited API endpoint.
  - **ETL Processes**: Performing transformations that involve heavy I/O with databases or file systems.
  - **Vector Embedding**: Generating embeddings for a large corpus of text via model APIs.
  - **Web Scraping**: Processing a list of URLs to fetch and parse data.

## Key Features

  - 🚀 **High-Performance Concurrency**: Utilizes `asyncio` to handle thousands of concurrent I/O tasks efficiently, maximizing throughput.
  - 🛡️ **Resilient & Resumable**: Never lose work on interruptions. Automatically tracks and skips completed tasks on restart.
  - ⚡ **Fully Asynchronous**: Employs an end-to-end non-blocking I/O pipeline with `aiofiles` and `aiologger` for a truly stall-free performance.
  - 📦 **Managed Resource Lifecycle**: Provides `on_startup` and `on_shutdown` hooks to gracefully initialize and release shared resources like database connections and API clients.
  - ✍️ **Efficient Batch Writing**: Buffers results and writes them to disk in batches to minimize I/O overhead.
  - 💣 **Granular Error Handling**: Isolates failures to individual records. Failed items are logged with rich context to a separate error file without halting the entire process.
  - 🔄 **Built-in Retry Logic**: Includes a ready-to-use `@retry_with_backoff` decorator with exponential backoff and jitter to handle transient network errors.
  - 📊 **Real-time Progress & Reporting**: Displays a dynamic `tqdm` progress bar with live success/failure counts and generates a detailed statistical summary upon completion.

## Project Structure

The framework is organized into five well-defined modules for maximum clarity and maintainability.

```
your_project/
├── config.py             # Your single source of truth for all settings.
├── utils.py              # Houses reusable, business-agnostic components.
├── core_processor.py     # The framework's engine. You should not need to edit this.
├── user_logic.py         # Your workspace. Implement your task-specific logic here.
└── main.py               # The application's generic entry point.
```

## Getting Started

#### 1\. Installation

The framework requires Python 3.9+. Place the five framework files in your project directory and install the dependencies:

```bash
pip install aiohttp tqdm aiofiles aiologger
```

#### 2\. Configuration (`config.py`)

Modify the `Settings` class in `config.py` to define your file paths, unique ID key, and performance parameters.

```python
class Settings:
    INPUT_FILE: str = "data/input.jsonl"
    OUTPUT_FILE: str = "data/output.jsonl"
    ID_KEY: str = "id"
    MAX_CONCURRENCY: int = 50
    # ...
```

#### 3\. Implementation (`user_logic.py`)

This is where you define your task by implementing three key functions. See "Core Concepts Explained" below for details.

#### 4\. Execution

Run the framework from your terminal:

```bash
python main.py
```

## Core Concepts Explained

The framework interacts with your code through a clear contract defined in `user_logic.py`. You must implement three functions that manage your task's lifecycle.

  - `on_startup() -> dict`:
    This coroutine runs **once** before any processing begins. Its purpose is to initialize and return a `context` dictionary containing any shared resources (e.g., SDK clients, database connection pools).

  - `process_record(record: dict, context: dict) -> dict`:
    This is your main worker coroutine. It runs concurrently for **each line** in your input file. It receives the data `record` for one line and the shared `context` object. Its job is to perform the desired operation and return the resulting dictionary to be saved.

  - `on_shutdown(context: dict)`:
    This coroutine runs **once** after all processing is finished (even if errors occurred). Use it to gracefully clean up and release the resources created in `on_startup`.

The `context` object acts as the bridge, safely passing shared resources from startup to every worker and finally to shutdown. The framework also automatically adds a shared `aiohttp.ClientSession` to the context as `context['session']`.

## Usage Walkthrough

**Goal**: Enrich a `users.jsonl` file with location data from an external API.

**1. Input Data (`data/users.jsonl`)**:

```json
{"user_id": 1, "ip_address": "8.8.8.8"}
{"user_id": 2, "ip_address": "1.1.1.1"}
```

**2. Configuration (`config.py`)**:

```python
class Settings:
    INPUT_FILE: str = "data/users.jsonl"
    OUTPUT_FILE: str = "data/users_enriched.jsonl"
    ID_KEY: str = "user_id"
    MAX_CONCURRENCY: int = 10
    # ...
```

**3. Logic Implementation (`user_logic.py`)**:

```python
# user_logic.py
from typing import Dict, Any
from utils import retry_with_backoff

# No special resources needed, so startup/shutdown are simple.
async def on_startup() -> Dict[str, Any]:
    return {}

async def on_shutdown(context: Dict[str, Any]):
    pass

@retry_with_backoff(retries=2, initial_delay=5)
async def process_record(record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    session = context['session']  # Use the session provided by the framework
    ip = record.get("ip_address")
    
    api_url = f"http://ip-api.com/json/{ip}"
    
    async with session.get(api_url) as response:
        response.raise_for_status()
        location_data = await response.json()
        
        if location_data.get('status') == 'success':
            record['country'] = location_data.get('country')
            record['city'] = location_data.get('city')
            record['isp'] = location_data.get('isp')
        
        return record
```

**4. Run & Check Output (`data/users_enriched.jsonl`)**:
After running `python main.py`, the output file will contain:

```json
{"user_id": 1, "ip_address": "8.8.8.8", "country": "United States", "city": "Mountain View", "isp": "Google LLC"}
{"user_id": 2, "ip_address": "1.1.1.1", "country": "United States", "city": "Los Angeles", "isp": "Cloudflare, Inc."}
```

## Advanced Topics

### Retry Decorator

The `utils.py` module provides a powerful `@retry_with_backoff` decorator. Apply it to your `process_record` function to automatically handle transient network errors.

### Handling Synchronous SDKs

If you must use a blocking library, wrap the blocking call in `await asyncio.to_thread(...)` to prevent it from stalling the framework.

```python
# Inside process_record
# blocking_result = await asyncio.to_thread(my_sync_sdk.call, arg1)
```

### Idempotency

For critical tasks that write to a database, ensure your `process_record` logic is **idempotent** (running it multiple times has the same effect as running it once). This prevents duplicate writes if a job is restarted after a partial failure.

### Forcing Reruns

To re-process specific, already completed items, set the `RERUN_KEY` in `config.py` (e.g., to `"force_rerun"`). In your `output.jsonl` file, add this key to any record you want to re-run (e.g., `{"id": "...", "force_rerun": true}`). The framework will pick it up on the next run.