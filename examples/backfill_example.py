#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/4/25 10:30
# @Author  : yanhui.ning
# @File    : backfill_example.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：数据回溯示例脚本

本脚本展示了如何以编程方式使用scheduler_cli.py中的数据回溯功能，
而不是通过命令行调用。适合需要在Python程序中集成回溯功能的场景。

包含示例：
1. 按天回溯
2. 按周回溯
3. 按月回溯
4. 使用自定义日期列表回溯
5. 指定任务ID回溯

使用方法：
python backfill_example.py

依赖项：
- scheduler_cli: 通用调度命令行工具模块

版本历史：
- v1.0 (2024/4/25): 初始版本
"""

import os
import sys
import json
from datetime import datetime, timedelta

# 将项目根目录添加到路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# 导入回溯相关函数
from scheduler_cli import (
    run_backfill, 
    get_date_range, 
    get_week_range,
    get_month_range,
    load_json_file
)


def example_1_backfill_by_day():
    """按天回溯示例"""
    print("\n=== 示例1: 按天回溯 ===")
    
    # 构建回溯参数
    backfill_params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-05",
        "date_granularity": "day",
        "date_param_name": "day_id",
        "dry_run": True,  # 设置为True时不会实际执行任务
        "params": {
            "data_path": "/data/backfill/daily_data",
            "debug_mode": True
        }
    }
    
    # 将参数写入临时文件
    params_file = os.path.join(project_root, "temp_day_params.json")
    with open(params_file, 'w', encoding='utf-8') as f:
        json.dump(backfill_params, f, indent=4)
    
    try:
        # 执行回溯
        config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
        success = run_backfill(config_file, params_file)
        
        print(f"按天回溯{'成功' if success else '失败'}")
    finally:
        # 删除临时文件
        if os.path.exists(params_file):
            os.remove(params_file)


def example_2_backfill_by_week():
    """按周回溯示例"""
    print("\n=== 示例2: 按周回溯 ===")
    
    # 构建回溯参数
    backfill_params = {
        "start_date": "2024-01-01",
        "end_date": "2024-02-15",
        "date_granularity": "week",
        "date_param_name": "week_start_date",
        "dry_run": True,
        "params": {
            "data_path": "/data/backfill/weekly_stats"
        }
    }
    
    # 将参数写入临时文件
    params_file = os.path.join(project_root, "temp_week_params.json")
    with open(params_file, 'w', encoding='utf-8') as f:
        json.dump(backfill_params, f, indent=4)
    
    try:
        # 执行回溯
        config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
        success = run_backfill(config_file, params_file)
        
        print(f"按周回溯{'成功' if success else '失败'}")
    finally:
        # 删除临时文件
        if os.path.exists(params_file):
            os.remove(params_file)


def example_3_backfill_by_month():
    """按月回溯示例"""
    print("\n=== 示例3: 按月回溯 ===")
    
    # 构建回溯参数
    backfill_params = {
        "start_date": "2024-01-01",
        "end_date": "2024-06-30",
        "date_granularity": "month",
        "date_param_name": "month_start_date",
        "dry_run": True,
        "params": {
            "data_path": "/data/backfill/monthly_report"
        }
    }
    
    # 将参数写入临时文件
    params_file = os.path.join(project_root, "temp_month_params.json")
    with open(params_file, 'w', encoding='utf-8') as f:
        json.dump(backfill_params, f, indent=4)
    
    try:
        # 执行回溯
        config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
        success = run_backfill(config_file, params_file)
        
        print(f"按月回溯{'成功' if success else '失败'}")
    finally:
        # 删除临时文件
        if os.path.exists(params_file):
            os.remove(params_file)


def example_4_backfill_with_custom_dates():
    """使用自定义日期列表回溯示例"""
    print("\n=== 示例4: 使用自定义日期列表回溯 ===")
    
    # 构建回溯参数
    backfill_params = {
        "custom_dates": [
            "2024-01-01",
            "2024-01-15",
            "2024-02-01",
            "2024-03-01"
        ],
        "date_param_name": "batch_date",
        "dry_run": True,
        "params": {
            "data_path": "/data/backfill/custom_batch"
        }
    }
    
    # 将参数写入临时文件
    params_file = os.path.join(project_root, "temp_custom_dates_params.json")
    with open(params_file, 'w', encoding='utf-8') as f:
        json.dump(backfill_params, f, indent=4)
    
    try:
        # 执行回溯
        config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
        success = run_backfill(config_file, params_file)
        
        print(f"自定义日期列表回溯{'成功' if success else '失败'}")
    finally:
        # 删除临时文件
        if os.path.exists(params_file):
            os.remove(params_file)


def example_5_backfill_specific_tasks():
    """回溯特定任务示例"""
    print("\n=== 示例5: 回溯特定任务 ===")
    
    # 构建回溯参数
    backfill_params = {
        "start_date": "2024-01-01",
        "end_date": "2024-01-03",
        "date_param_name": "day_id",
        "dry_run": True,
        "params": {
            "data_path": "/data/backfill/specific_tasks"
        }
    }
    
    # 将参数写入临时文件
    params_file = os.path.join(project_root, "temp_specific_tasks_params.json")
    with open(params_file, 'w', encoding='utf-8') as f:
        json.dump(backfill_params, f, indent=4)
    
    try:
        # 执行回溯，指定特定任务ID
        config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
        job_ids = "data_quality_check,notify_completion"
        success = run_backfill(config_file, params_file, job_ids.split(","))
        
        print(f"回溯特定任务{'成功' if success else '失败'}")
    finally:
        # 删除临时文件
        if os.path.exists(params_file):
            os.remove(params_file)


def example_6_direct_backfill():
    """直接使用参数回溯，不使用临时文件"""
    print("\n=== 示例6: 直接使用参数回溯 ===")
    
    # 加载工作流配置
    config_file = os.path.join(project_root, "config/screenrecog_workflow.json")
    workflow_config = load_json_file(config_file)
    
    # 获取日期范围
    date_range = get_date_range("2024-01-01", "2024-01-03")
    print(f"生成日期范围: {date_range}")
    
    # 获取按周日期范围
    week_range = get_week_range("2024-01-01", "2024-02-15")
    print(f"生成周范围: {week_range}")
    
    # 获取按月日期范围
    month_range = get_month_range("2024-01-01", "2024-06-30")
    print(f"生成月范围: {month_range}")
    
    print("此示例展示了如何直接使用API而不是通过临时文件")


def main():
    """运行所有示例"""
    print("开始运行数据回溯示例...")
    
    # 运行示例
    example_1_backfill_by_day()
    example_2_backfill_by_week()
    example_3_backfill_by_month()
    example_4_backfill_with_custom_dates()
    example_5_backfill_specific_tasks()
    example_6_direct_backfill()
    
    print("\n所有示例运行完成!")


if __name__ == "__main__":
    main() 