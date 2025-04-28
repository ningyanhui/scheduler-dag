#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/4/25 10:00
# @Author  : yanhui.ning
# @File    : cli_examples.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：命令行工具使用示例

本脚本展示了如何使用scheduler_cli.py命令行工具执行各种操作：
1. 执行单个工作流
2. 执行数据回溯（按天、按周、按月、自定义日期列表）
3. 可视化工作流
4. 查看工作流信息

通过示例命令和说明帮助用户理解如何使用命令行工具。

使用方法：
查看本文件中的示例命令，根据需要修改参数后在命令行中执行

依赖项：
- scheduler_cli.py: 通用调度命令行工具

注意事项：
- 所有示例命令均假设在scheduler_dag目录下执行
- 命令参数需要根据实际情况修改

版本历史：
- v1.0 (2024/4/25): 初始版本
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

"""
以下为命令行工具使用示例，包括各种常见场景的命令和解释
"""

def print_command(cmd, description):
    """打印命令及其描述"""
    print(f"\n{'=' * 80}")
    print(f"# {description}")
    print(f"{'=' * 80}")
    print(f"{cmd}")
    print(f"{'=' * 80}")


def main():
    # 示例工作流配置文件
    workflow_config = "./config/screenrecog_workflow.json"
    
    # 1. 执行单个工作流
    print_command(
        f"python scheduler_cli.py run --config {workflow_config}",
        "基本用法：执行整个工作流"
    )
    
    # 2. 使用参数执行工作流
    print_command(
        f"python scheduler_cli.py run --config {workflow_config} --params ./config/params_example.json",
        "使用参数文件执行工作流"
    )
    
    # 3. 只执行特定任务
    print_command(
        f"python scheduler_cli.py run --config {workflow_config} --job_ids screen_event_detail,user_behavior_analyze",
        "只执行指定的任务"
    )
    
    # 4. 按天回溯数据
    print_command(
        f"python scheduler_cli.py backfill --config {workflow_config} --backfill_params ./config/backfill_params_example.json",
        "按天回溯数据"
    )
    
    # 5. 按周回溯数据
    print_command(
        f"python scheduler_cli.py backfill --config {workflow_config} --backfill_params ./config/backfill_params_week_example.json",
        "按周回溯数据"
    )
    
    # 6. 按月回溯数据
    print_command(
        f"python scheduler_cli.py backfill --config {workflow_config} --backfill_params ./config/backfill_params_month_example.json",
        "按月回溯数据"
    )
    
    # 7. 使用自定义日期列表回溯数据
    print_command(
        f"python scheduler_cli.py backfill --config {workflow_config} --backfill_params ./config/backfill_params_custom_dates_example.json",
        "使用自定义日期列表回溯数据"
    )
    
    # 8. 回溯时只执行特定任务
    print_command(
        f"python scheduler_cli.py backfill --config {workflow_config} --backfill_params ./config/backfill_params_example.json --job_ids data_quality_check",
        "回溯时只执行特定任务"
    )
    
    # 9. 可视化工作流
    print_command(
        f"python scheduler_cli.py visualize --config {workflow_config}",
        "可视化工作流（默认输出到output目录）"
    )
    
    # 10. 可视化并指定输出路径
    print_command(
        f"python scheduler_cli.py visualize --config {workflow_config} --output ./output/my_workflow.png",
        "可视化工作流并指定输出路径"
    )
    
    # 11. 查看工作流信息
    print_command(
        f"python scheduler_cli.py info --config {workflow_config}",
        "查看工作流信息"
    )
    
    # 12. 帮助命令
    print_command(
        "python scheduler_cli.py -h",
        "查看命令行工具帮助信息"
    )
    
    print_command(
        "python scheduler_cli.py run -h",
        "查看run子命令帮助信息"
    )
    
    print_command(
        "python scheduler_cli.py backfill -h",
        "查看backfill子命令帮助信息"
    )


if __name__ == "__main__":
    main() 