# 处理使用位置参数的Python脚本

在工作流中运行使用位置参数而不是命名参数的Python脚本时，有两种主要方法可以选择。

## 问题描述

假设我们有一个Python脚本，它接受位置参数而不是命名参数：

```bash
python table_partition_check.py 20240714 tranai_dw.table_a,tranai_dw.table_b
```

而不是：

```bash
python table_partition_check.py --day_id=20240714 --table_list=tranai_dw.table_a,tranai_dw.table_b
```

在这种情况下，标准的PythonTask配置无法正常工作，因为它总是以 `--key=value` 格式传递参数。

## 解决方案

### 方案1: 使用ShellTask

最简单的方法是使用ShellTask而不是PythonTask：

```json
{
    "task_id": "positional_args_task",
    "type": "shell",
    "command": "python table_partition_check.py ${day_id} ${table_list}",
    "params": {
        "day_id": "${day_id}",
        "table_list": "${table_list}"
    },
    "working_dir": "./scripts"
}
```

**优点**：
- 简单直接
- 无需修改代码

**缺点**：
- 无法利用PythonTask的一些特性
- 不够优雅

### 方案2: 使用PythonTask的custom_command参数

更推荐的方法是使用PythonTask的custom_command参数，它允许你自定义命令行格式：

```json
{
    "task_id": "custom_python_cmd",
    "type": "python",
    "script_path": "./scripts/table_partition_check.py",
    "custom_command": "python {script_path} {params.day_id} {params.table_list}",
    "params": {
        "day_id": "${day_id}",
        "table_list": "${table_list}"
    },
    "working_dir": "."
}
```

**优点**：
- 更优雅
- 保持了PythonTask的语义
- 更容易理解和维护

**缺点**：
- 需要scheduler_dag框架支持custom_command参数

## 最佳实践

1. **优先使用命名参数**：如果可能，修改你的Python脚本以接受命名参数而不是位置参数。这是最干净的解决方案。

2. **使用自定义命令模板**：如果必须使用位置参数，首选PythonTask的custom_command方法。

3. **作为备选，使用ShellTask**：如果由于某种原因无法使用custom_command，可以回退到ShellTask方案。

4. **记录参数顺序**：无论使用哪种方案，都要在配置中清晰地记录位置参数的顺序和含义。

## 参考配置示例

查看以下配置文件以获取完整示例：

- `config/python_positional_task_example.json`：Shell任务方法
- `config/positional_args_python_task_example.json`：自定义Python命令方法
- `config/best_practice_python_positional_args.json`：两种方法的对比

## 编写接受位置参数的Python脚本最佳实践

如果你正在编写新的Python脚本，请考虑以下最佳实践：

1. **优先使用命名参数**：使用argparse库，设计脚本接受命名参数：

```python
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--day_id", required=True, help="数据日期")
parser.add_argument("--table_list", required=True, help="表名列表")
args = parser.parse_args()

day_id = args.day_id
table_list = args.table_list
```

2. **如果必须使用位置参数，同时支持命名参数**：

```python
import argparse
import sys

# 先尝试解析命名参数
parser = argparse.ArgumentParser()
parser.add_argument("--day_id", help="数据日期")
parser.add_argument("--table_list", help="表名列表")
args, unknown = parser.parse_known_args()

# 如果没有提供命名参数，尝试使用位置参数
if args.day_id is None or args.table_list is None:
    if len(sys.argv) >= 3:
        day_id = sys.argv[1]
        table_list = sys.argv[2]
    else:
        print("错误：需要提供day_id和table_list参数")
        sys.exit(1)
else:
    day_id = args.day_id
    table_list = args.table_list
```

这样的脚本既可以接受位置参数，也可以接受命名参数，从而兼容各种使用场景。 