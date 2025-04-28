# Scheduler DAG - 通用工作流调度工具

## 项目概述

Scheduler DAG 是一个使用Python实现的通用工作流调度工具，用于以工作流形式串并联执行Job任务。主要特性包括：

1. 以工作流形式串并联Job任务
2. 支持多种Job类型：Spark SQL、Python、PySpark、Shell、Hive SQL
3. 支持工作流和Job级别的任务重跑或回溯
4. 支持工作流级别的全局传参和Job级别的传参
5. 支持参数替换，包括自动解析时间类参数
6. 支持任务失败时告警通知（飞书webhook）
7. 支持配置文件驱动的工作流定义和执行
8. 提供统一的命令行接口进行工作流调度和回溯
9. 灵活的回溯方式，支持按天、按周、按月或自定义日期列表回溯
10. 支持多时间参数和自定义格式，满足不同任务对日期格式的需求

## 项目结构

```
scheduler_dag/
├── scheduler/             # 核心代码
│   ├── __init__.py        # 包初始化
│   ├── config.py          # 配置解析模块
│   ├── dag.py             # DAG引擎核心实现
│   ├── task.py            # 任务基类及各类型任务实现
│   ├── params.py          # 参数管理实现
│   ├── alert.py           # 告警功能实现
│   └── utils.py           # 工具函数
├── scheduler_cli.py       # 通用调度命令行工具
├── config/                # 配置文件目录
│   ├── screenrecog_workflow.json             # 工作流配置示例
│   ├── params_example.json                   # 参数配置示例
│   ├── backfill_params_example.json          # 按天回溯参数示例
│   ├── backfill_params_week_example.json     # 按周回溯参数示例
│   ├── backfill_params_month_example.json    # 按月回溯参数示例
│   └── backfill_params_custom_dates_example.json # 自定义日期回溯参数示例
├── sql/                   # SQL脚本目录
│   ├── dwd_screenrecog_event_detail_di_etl.sql
│   └── dws_screenrecog_user_behavior_di_etl.sql
├── scripts/               # Python脚本目录
│   └── data_quality_check.py
├── examples/              # 示例代码目录
│   ├── example_workflow.py            # 基本工作流示例
│   ├── config_workflow_example.py     # 基于配置文件的工作流示例
│   ├── rerun_workflow.py              # 任务重跑示例
│   ├── alert_example.py               # 告警功能示例
│   ├── cli_examples.py                # 命令行工具使用示例
│   ├── backfill_example.py            # 数据回溯编程示例
│   └── multi_date_format_backfill_example.py # 多日期格式回溯示例
├── logs/                  # 日志目录
├── output/                # 输出目录
├── README.md              # 项目说明
└── setup.py               # 安装配置
```

## 安装指南

### 依赖项

- Python 3.6+
- graphviz (可选，用于可视化DAG)
- requests (用于发送告警通知)

### 安装方法

1. 克隆仓库
```bash
git clone <repository-url>
cd scheduler_dag
```

2. 安装依赖
```bash
pip install -e .
```

## 使用指南

### 1. 工作流配置文件

工作流使用JSON格式进行配置，包含以下主要部分：

```json
{
    "name": "工作流名称",
    "description": "工作流描述",
    "params": {
        "param1": "值1",
        "param2": "值2"
    },
    "tasks": [
        {
            "task_id": "task1",
            "type": "shell",
            "command": "echo 'Hello ${param1}'",
            "params": {
                "param1": "${param1}"
            }
        },
        {
            "task_id": "task2",
            "type": "spark-sql",
            "sql_file": "./sql/query.sql",
            "params": {
                "day_id": "${day_id}"
            },
            "spark_config": {
                "spark.app.name": "任务_${day_id}",
                "spark.executor.memory": "4g"
            }
        }
    ],
    "dependencies": [
        {
            "from": "task1",
            "to": "task2"
        }
    ],
    "alert": {
        "type": "feishu",
        "webhook_url": "https://open.feishu.cn/open-apis/bot/v2/hook/your-webhook-token",
        "at_all": true,
        "fail_fast": true
    }
}
```

### 2. 通用命令行工具使用

scheduler_cli.py 是一个通用命令行工具，用于工作流调度和数据回溯。

#### 2.1 执行单个工作流

```bash
python scheduler_cli.py run --config workflow.json [--params params.json] [--job_ids job1,job2]
```

参数说明：
- `--config`：工作流配置文件路径（必须）
- `--params`：运行时参数配置文件路径（可选）
- `--job_ids`：要执行的任务ID列表，多个ID用逗号分隔（可选，默认执行所有任务）

#### 2.2 执行批量数据回溯

```bash
python scheduler_cli.py backfill --config workflow.json --backfill_params backfill_params.json [--job_ids job1,job2]
```

参数说明：
- `--config`：工作流配置文件路径（必须）
- `--backfill_params`：回溯参数配置文件路径（必须）
- `--job_ids`：要回溯的任务ID列表，多个ID用逗号分隔（可选，默认回溯所有任务）

回溯参数配置支持多种方式：

1. **按天回溯(默认)**：
```json
{
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "date_granularity": "day",
    "date_param_name": "day_id",
    "dry_run": false,
    "params": {
        "data_path": "/data/backfill/daily_data"
    }
}
```

2. **按周回溯**：
```json
{
    "start_date": "2024-01-01",
    "end_date": "2024-03-31",
    "date_granularity": "week",
    "date_param_name": "week_start_date",
    "dry_run": false,
    "params": {
        "data_path": "/data/backfill/weekly_stats"
    }
}
```

3. **按月回溯**：
```json
{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "date_granularity": "month",
    "date_param_name": "month_start_date",
    "dry_run": false,
    "params": {
        "data_path": "/data/backfill/monthly_report"
    }
}
```

4. **自定义日期列表回溯**：
```json
{
    "custom_dates": [
        "2024-01-01",
        "2024-01-15",
        "2024-02-01",
        "2024-03-01"
    ],
    "date_param_name": "batch_date",
    "dry_run": false,
    "params": {
        "data_path": "/data/backfill/custom_batch"
    }
}
```

5. **多日期参数和格式回溯**：
```json
{
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "date_granularity": "day",
    "date_param_names": ["day_id", "batch_date", "data_date"],
    "date_param_formats": {
        "day_id": "%Y-%m-%d",
        "batch_date": "%Y%m%d",
        "data_date": "%Y/%m/%d"
    },
    "dry_run": false,
    "params": {
        "data_path": "/data/backfill/multi_format"
    }
}
```

回溯参数说明：
- `start_date` 和 `end_date`：回溯的起止日期（使用日期范围时必须）
- `date_granularity`：日期粒度，支持"day"、"week"、"month"（默认为"day"）
- `date_param_name`：传递给任务的单个日期参数名（默认为"day_id"）
- `date_param_names`：传递给任务的多个日期参数名数组（优先于date_param_name）
- `date_param_formats`：每个日期参数对应的格式，使用Python的日期格式规范（如%Y-%m-%d）
- `custom_dates`：自定义日期列表，优先级高于日期范围
- `dry_run`：是否为空运行，不实际执行任务
- `params`：传递给任务的其他参数

对于每个日期参数，系统会自动生成以下参数：
- 原始参数名（如`day_id`）：使用原始日期格式，默认为`YYYY-MM-DD`
- 无破折号参数（如`day_id_no_dash`）：移除日期中的破折号，如`YYYYMMDD`
- 自定义格式参数（如`day_id_fmt`）：如果在`date_param_formats`中定义了格式，会生成带`_fmt`后缀的参数

#### 2.3 可视化工作流

```bash
python scheduler_cli.py visualize --config workflow.json [--output workflow.png] [--params params.json]
```

参数说明：
- `--config`：工作流配置文件路径（必须）
- `--output`：输出文件路径（可选，默认保存在output目录）
- `--params`：参数配置文件路径（可选）

#### 2.4 查看工作流信息

```bash
python scheduler_cli.py info --config workflow.json
```

参数说明：
- `--config`：工作流配置文件路径（必须）

### 3. 支持的任务类型

1. **ShellTask**: 执行Shell命令或脚本
   ```json
   {
       "task_id": "task1",
       "type": "shell",
       "command": "echo 'Hello ${param1}'",
       "working_dir": "/path/to/dir",
       "params": {
           "param1": "${param1}"
       }
   }
   ```

2. **PythonTask**: 执行Python函数或脚本
   ```json
   {
       "task_id": "task2",
       "type": "python",
       "script_path": "./scripts/data_quality_check.py",
       "working_dir": "/path/to/dir",
       "params": {
           "day_id": "${day_id}",
           "table_list": "table1,table2"
       }
   }
   ```

3. **PySparkTask**: 执行PySpark脚本
   ```json
   {
       "task_id": "task3",
       "type": "pyspark",
       "script_path": "./scripts/process_data.py",
       "params": {
           "day_id": "${day_id}"
       },
       "spark_config": {
           "spark.app.name": "数据处理_${day_id}",
           "spark.executor.memory": "4g",
           "spark.executor.instances": "10"
       }
   }
   ```

4. **SparkSQLTask**: 执行Spark SQL查询
   ```json
   {
       "task_id": "task4",
       "type": "spark-sql",
       "sql_file": "./sql/query.sql",
       "params": {
           "day_id": "${day_id}"
       },
       "spark_config": {
           "spark.app.name": "SQL查询_${day_id}",
           "spark.executor.memory": "4g"
       }
   }
   ```

5. **HiveSQLTask**: 执行Hive SQL查询
   ```json
   {
       "task_id": "task5",
       "type": "hive-sql",
       "sql_file": "./sql/hive_query.sql",
       "params": {
           "day_id": "${day_id}"
       },
       "hive_config": {
           "hive.execution.engine": "tez"
       }
   }
   ```

### 4. 参数替换

支持以 `${param_name}` 格式引用参数，例如:

```
SELECT * FROM table WHERE dt = '${day_id}'
```

还支持时间类参数的自动解析，例如:

```
${yyyy-MM-dd-1}  # 前一天的日期，格式为 yyyy-MM-dd
${yyyyMMdd+7}    # 7天后的日期，格式为 yyyyMMdd
```

## 高级用法和错误处理

### 1. 命令行参数说明

在使用 `scheduler_cli.py` 工具时，`--job_ids` 参数会被映射到 `workflow.execute()` 方法的 `only_tasks` 参数，用于指定仅执行特定任务。例如：

```bash
python scheduler_cli.py run --config workflow.json --job_ids task1,task2
```

这会执行工作流中的 task1 和 task2 任务，其他任务将被跳过。

### 2. 错误处理机制

工作流执行过程中的常见错误和处理方式：

- **任务依赖不存在**：系统会检查依赖关系，如果设置了不存在的任务依赖，会在执行前抛出错误。
- **任务执行失败**：默认情况下，任务失败会中断整个工作流（`fail_fast=True`）。可以通过设置 `"fail_fast": false` 允许其他并行任务继续执行。
- **参数替换错误**：如果参数替换过程中引用了不存在的参数，系统会保留原始形式（如 `${missing_param}`）。
- **报告错误**：当使用 `info` 命令查看工作流信息时，系统会提供正确的任务列表、依赖关系和参数信息。

### 3. 并行执行

当前版本的工作流执行是顺序的，但框架已为并行执行做好准备：

- 拓扑排序将任务分组为多个层级，同一层级的任务理论上可以并行执行
- 可以基于此扩展开发真正的并行执行功能

## 示例与测试用例

### 命令行工具使用示例

查看 `examples/cli_examples.py` 文件，该文件展示了如何使用命令行工具进行各种操作：
- 执行单个工作流
- 使用参数执行工作流
- 只执行特定任务
- 使用不同方式回溯数据（按天、按周、按月、自定义日期列表）
- 查看工作流信息
- 可视化工作流

```bash
# 查看示例
python examples/cli_examples.py

# 测试执行特定命令
python scheduler_cli.py info --config ./config/screenrecog_workflow.json
```

### 编程方式使用回溯功能

查看 `examples/backfill_example.py` 文件，该文件展示了如何在Python程序中集成回溯功能：
- 按天回溯
- 按周回溯
- 按月回溯
- 使用自定义日期列表回溯
- 回溯特定任务
- 直接使用API而不通过临时文件

```bash
# 运行回溯示例
python examples/backfill_example.py
```

### 多日期参数回溯示例

查看 `examples/multi_date_format_backfill_example.py` 文件，该文件展示了如何进行多日期参数回溯：
- 使用多个日期参数名
- 为不同参数指定不同日期格式
- 展示各种日期格式示例
- 从配置文件加载多日期参数配置

```bash
# 运行多日期参数回溯示例
python examples/multi_date_format_backfill_example.py
```

### 其他示例

项目还包含其他示例文件，演示工作流的不同用法：

- `example_workflow.py`: 基本工作流示例
- `config_workflow_example.py`: 基于配置文件的工作流示例
- `rerun_workflow.py`: 任务重跑示例
- `alert_example.py`: 告警功能示例

### 示例配置文件

在 `config/` 目录中可以找到各种示例配置文件：

- `screenrecog_workflow.json`: 工作流配置示例
- `params_example.json`: 参数配置示例
- `backfill_params_example.json`: 按天回溯参数示例
- `backfill_params_week_example.json`: 按周回溯参数示例
- `backfill_params_month_example.json`: 按月回溯参数示例
- `backfill_params_custom_dates_example.json`: 自定义日期回溯参数示例

## 常见问题

### 1. 回溯参数和任务参数如何对应？

在回溯配置中，可以通过 `date_param_name` 指定传递给任务的日期参数名称，例如：

- 默认情况下，日期参数名为 `day_id`，会自动添加 `day_id_no_dash` 参数（日期中的短横线被移除）
- 如果设置 `"date_param_name": "week_start_date"`，则日期参数会以 `week_start_date` 和 `week_start_date_no_dash` 传递给任务
- 当使用自定义日期列表时，可以设置任意参数名，如 `"date_param_name": "batch_date"`
- 使用 `date_param_names` 可以指定多个日期参数，每个参数都会生成对应的无破折号版本和格式化版本

### 2. 如何按不同粒度回溯数据？

- **按天回溯**：设置 `"date_granularity": "day"`（默认），每天执行一次工作流
- **按周回溯**：设置 `"date_granularity": "week"`，以每周一为起点，每周执行一次工作流
- **按月回溯**：设置 `"date_granularity": "month"`，以每月1号为起点，每月执行一次工作流
- **自定义日期**：提供 `custom_dates` 数组，按指定的日期列表执行工作流

### 3. 如何使用多日期参数和格式？

在回溯配置中添加以下参数：

```json
{
    "date_param_names": ["day_id", "batch_date", "data_date"],
    "date_param_formats": {
        "day_id": "%Y-%m-%d",
        "batch_date": "%Y%m%d",
        "data_date": "%Y/%m/%d"
    }
}
```

回溯时，系统会为每个日期点生成以下参数：
- `day_id`: "2024-01-01"
- `day_id_no_dash`: "20240101"
- `day_id_fmt`: "2024-01-01"（使用格式化）
- `batch_date`: "2024-01-01"
- `batch_date_no_dash`: "20240101"
- `batch_date_fmt`: "20240101"（使用格式化）
- `data_date`: "2024-01-01"
- `data_date_no_dash`: "20240101"
- `data_date_fmt`: "2024/01/01"（使用格式化）

可以通过空运行模式查看参数：`"dry_run": true`

### 4. 如何在Python程序中集成回溯功能？

查看 `examples/backfill_example.py` 了解详细用法，基本步骤如下：

```python
from scheduler_cli import run_backfill

# 方法1：使用参数文件
config_file = "path/to/workflow.json"
backfill_params_file = "path/to/backfill_params.json"
job_ids = ["task1", "task2"]  # 可选
success = run_backfill(config_file, backfill_params_file, job_ids)

# 方法2：创建临时参数文件
backfill_params = {
    "start_date": "2024-01-01",
    "end_date": "2024-01-10",
    "date_granularity": "day",
    "date_param_name": "day_id",
    "dry_run": False
}
# 将backfill_params写入临时文件，然后调用run_backfill函数
```

### 5. 如何处理工作流执行错误？

执行工作流时，如果遇到错误，可以按照以下步骤处理：

1. 首先确认错误信息，常见错误包括：
   - `'Workflow' object has no attribute 'tasks'`：工作流对象结构问题
   - `'DAG' object has no attribute 'get_dependencies'`：DAG对象方法缺失
   - 参数错误：确认参数格式是否正确

2. 如果是接口调用问题，请确认：
   - `workflow.execute()` 的参数应为 `only_tasks`，而非 `target_task_ids`
   - 访问任务列表应使用 `workflow.dag.tasks`，而非 `workflow.tasks`
   - 访问参数应使用 `workflow.dag.param_manager.params`

3. 对于告警功能问题，使用全局变量 `alert_manager`：
   ```python
   from scheduler.alert import alert_manager
   
   # 检查告警是否启用
   if alert_manager.enabled:
       print("告警已启用")
   ```

## 许可证

[MIT 许可证] 