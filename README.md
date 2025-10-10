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

  - 🚀 **High-Performance Concurrency**: Utilizes `asyncio` to handle a high volume of concurrent I/O tasks efficiently, maximizing throughput.
  - 🛡️ **Resilient & Resumable**: Never lose work on interruptions. Automatically tracks and skips completed tasks on restart.
  - ⏱️ **Built-in Rate Limiting**: Easily conform to API limits by setting a maximum number of requests per minute.
  - ⚡ **Fully Asynchronous**: Employs an end-to-end non-blocking I/O pipeline with `aiofiles` and `aiologger` for a truly stall-free performance.
  - 📦 **Managed Resource Lifecycle**: Provides `on_startup` and `on_shutdown` hooks to gracefully initialize and release shared resources.
  - ✍️ **Efficient Batch Writing**: Buffers results and writes them to disk in batches to minimize I/O overhead.
  - 💣 **Granular Error Handling**: Isolates failures to individual records. Failed items are logged with rich context to a separate error file without halting the process.
  - 🔄 **Built-in Retry Logic**: Includes a ready-to-use `@retry_with_backoff` decorator to handle transient network errors gracefully.
  - 📊 **Real-time Progress & Reporting**: Displays a dynamic `tqdm` progress bar with live success/failure counts and generates a detailed statistical summary upon completion.

## Project Structure

The framework is organized into six well-defined modules for maximum clarity and maintainability.

```
your_project/
├── config.py             # Your single source of truth for all settings.
├── logging_config.py     # Centralized configuration for the async logger.
├── utils.py              # Houses reusable, business-agnostic components.
├── processor.py          # The framework's engine. You should not need to edit this.
├── task.py               # Your workspace. Implement your task-specific logic here.
└── main.py               # The application's generic entry point.
```

## Dependencies

  - `aiohttp`
  - `tqdm`
  - `aiofiles`
  - `aiologger`

## Getting Started

#### 1\. Installation

The framework requires Python 3.9+. Place the six framework files in your project directory and install the dependencies:

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
    REQUESTS_PER_MINUTE: int = 0
    # ...
```

#### 3\. Implementation (`task.py`)

This is where you define your task by implementing three key functions. See "Core Concepts Explained" below for details.

#### 4\. Execution

Run the framework from your terminal:

```bash
python main.py
```

## Core Concepts Explained

The framework interacts with your code through a clear contract defined in `task.py`. You must implement three functions that manage your task's lifecycle.

  - `on_startup() -> dict`:
    This coroutine runs **once** before any processing begins. Its purpose is to initialize and return a `context` dictionary containing any shared resources (e.g., SDK clients, database connection pools).

  - `process_record(record: dict, context: dict) -> dict | None`:
    This is your main worker coroutine. It runs concurrently for **each line** in your input file. It receives the data `record` for one line and the shared `context` object. Its job is to perform the desired operation and return the resulting dictionary to be saved. Returning `None` will skip saving a result for that record.

  - `on_shutdown(context: dict)`:
    This coroutine runs **once** after all processing is finished (even if errors occurred). Use it to gracefully clean up and release the resources created in `on_startup`.

The `context` object acts as the bridge, safely passing shared resources from startup to every worker and finally to shutdown. The framework also automatically adds a shared `aiohttp.ClientSession` to the context as `context['session']`.

## Usage Walkthrough

**Goal**: Enrich a list of IP addresses from a JSONL file with geolocation data from an external API.

**1. Input Data (`data/ips.jsonl`)**:

```json
{"ip": "8.8.8.8", "source": "Google DNS"}
{"ip": "1.1.1.1", "source": "Cloudflare DNS"}
```

**2. Configuration (`config.py`)**:

```python
class Settings:
    INPUT_FILE: str = "data/ips.jsonl"
    OUTPUT_FILE: str = "data/ips_enriched.jsonl"
    ID_KEY: str = "ip"
    MAX_CONCURRENCY: int = 10
    REQUESTS_PER_MINUTE: int = 120
    # ...
```

**3. Logic Implementation (`task.py`)**:

```python
# task.py
from typing import Dict, Any
from utils import retry_with_backoff

# No special resources are needed for this simple task.
async def on_startup() -> Dict[str, Any]:
    return {}

async def on_shutdown(context: Dict[str, Any]):
    pass

@retry_with_backoff(retries=2, initial_delay=5)
async def process_record(record: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    session = context['session']  # Use the session provided by the framework
    ip_address = record.get("ip")
    
    api_url = f"http://ip-api.com/json/{ip_address}"
    
    async with session.get(api_url) as response:
        response.raise_for_status()
        location_data = await response.json()
        
        if location_data.get('status') == 'success':
            record['country'] = location_data.get('country')
            record['city'] = location_data.get('city')
            record['isp'] = location_data.get('isp')
        
        return record
```

**4. Run & Check Output (`data/ips_enriched.jsonl`)**:
After running `python main.py`, the output file will contain:

```json
{"ip": "8.8.8.8", "source": "Google DNS", "country": "United States", "city": "Mountain View", "isp": "Google LLC"}
{"ip": "1.1.1.1", "source": "Cloudflare DNS", "country": "United States", "city": "Los Angeles", "isp": "Cloudflare, Inc."}
```

## Advanced Topics

### Rate Limiting

The framework provides two parameters to control how you interact with external services: `MAX_CONCURRENCY` and `REQUESTS_PER_MINUTE`. It's important to understand their roles:

  - **`MAX_CONCURRENCY`**: This limits how many tasks can be *running in parallel* at any single moment. It's like setting the number of workers on an assembly line. This is useful for managing your system's resources (memory, open connections) and for services that limit concurrent connections.
  - **`REQUESTS_PER_MINUTE`**: This controls the *overall rate* at which new tasks are started. It's like setting the speed of the conveyor belt feeding the assembly line. This is essential for APIs that have a strict rate limit (e.g., "100 calls per minute").

Set `REQUESTS_PER_MINUTE` to a positive integer in `config.py` to enable it. The framework will automatically introduce the necessary delay between starting each task to meet the target rate. Setting it to `0` disables this feature, relying solely on `MAX_CONCURRENCY`.

### Retry Decorator

The `utils.py` module provides a powerful `@retry_with_backoff` decorator. Apply it to your `process_record` function to automatically handle transient network errors.

### Handling Synchronous SDKs

If you must use a blocking library (e.g., a synchronous database driver), wrap the blocking call in `await asyncio.to_thread(...)` inside your `process_record` function to keep the framework responsive.

### Idempotency

For critical tasks that write to a database, ensure your `process_record` logic is **idempotent** (running an operation multiple times has the same effect as running it once). This prevents duplicate writes if a job is restarted after a partial failure.

### Forcing Reruns

To re-process specific, already completed items, set the `RERUN_KEY` in `config.py` (e.g., to `"force_rerun"`). In your `output.jsonl` file, add this key to any record you want to re-run (e.g., `{"id": "...", "force_rerun": true}`). The framework will pick it up on the next run.

## License

This project is licensed under the MIT License.