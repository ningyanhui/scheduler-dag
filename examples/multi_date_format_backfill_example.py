#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-06-11 10:30:00
# @Author  : yanhui.ning
# @File    : multi_date_format_backfill_example.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：演示如何使用多种日期参数和格式进行回填

输入：
- 日期范围（开始日期和结束日期）
- 日期粒度（日、周、月）
- 多个日期参数名及其格式

输出：
- 回填执行结果日志

使用方法：
python multi_date_format_backfill_example.py

依赖项：
- scheduler_cli模块
- 配置文件

注意事项：
- 确保配置文件中的参数正确设置
- 回填前建议先执行dry_run检查任务

版本历史：
- v1.0 (2024-06-11): 初始版本
"""

import json
import os
from datetime import datetime
from scheduler_dag.scheduler_cli import run_backfill


def backfill_with_multi_date_formats():
    """使用多种日期格式进行回填的示例"""
    print("开始执行多日期格式回填示例...")
    
    # 方法1：通过代码构建多日期参数配置
    backfill_params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
        "date_granularity": "day",
        "date_param_name": "day_id",  # 主日期参数名
        "date_param_names": ["day_id", "batch_date", "data_date"],  # 所有需要填充的日期参数名
        "date_param_formats": {
            "day_id": "%Y-%m-%d",      # 例如: 2024-01-01
            "batch_date": "%Y%m%d",    # 例如: 20240101
            "data_date": "%Y/%m/%d"    # 例如: 2024/01/01
        },
        "dry_run": True,  # 先执行干运行，查看任务情况
        "params": {
            "data_path": "/data/backfill/example",
            "target_region": "all",
            "debug_mode": True,
            "max_retry": 2
        }
    }
    
    print("\n方法1：使用代码构建的配置参数:")
    print(json.dumps(backfill_params, indent=2))
    
    # 执行回填
    run_backfill(
        workflow_file="workflows/example_workflow.yaml",
        backfill_params=backfill_params
    )
    
    # 方法2：从配置文件加载多日期参数配置
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                              "config", "backfill_params_example.json")
    
    print(f"\n方法2：从配置文件 {config_path} 加载参数:")
    with open(config_path, 'r') as f:
        file_params = json.load(f)
        print(json.dumps(file_params, indent=2))
    
    # 执行基于配置文件的回填
    run_backfill(
        workflow_file="workflows/example_workflow.yaml",
        backfill_params_file=config_path
    )
    
    print("\n回填执行完成!")


def print_formatted_dates_example():
    """打印不同格式的日期示例，帮助理解日期格式参数"""
    sample_date = datetime.now()
    formats = {
        "%Y-%m-%d": "标准ISO格式 (例如: 2024-01-01)",
        "%Y%m%d": "无分隔符格式 (例如: 20240101)",
        "%Y/%m/%d": "斜线分隔格式 (例如: 2024/01/01)",
        "%d-%m-%Y": "日-月-年格式 (例如: 01-01-2024)",
        "%b %d, %Y": "月名-日-年格式 (例如: Jan 01, 2024)",
        "%Y-%m-%dT%H:%M:%S": "ISO带时间格式 (例如: 2024-01-01T00:00:00)"
    }
    
    print("\n不同日期格式示例 (当前日期):")
    print("-" * 60)
    print(f"{'格式':20} | {'示例':20} | 描述")
    print("-" * 60)
    
    for fmt, desc in formats.items():
        formatted = sample_date.strftime(fmt)
        print(f"{fmt:20} | {formatted:20} | {desc}")


if __name__ == "__main__":
    print_formatted_dates_example()
    backfill_with_multi_date_formats() 