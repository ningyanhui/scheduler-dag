#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-07-28 15:30:00
# @Author  : yanhui.ning
# @File    : backfill_with_different_date_formats.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：展示如何使用多种日期格式和参数名进行回填操作

输入：
- 日期范围（开始日期和结束日期）
- 日期粒度（日、周、月）
- 多个日期参数名及其格式

输出：
- 回填执行过程日志
- 不同日期格式示例

使用方法：
python backfill_with_different_date_formats.py

依赖项：
- scheduler_cli模块
- datetime模块
- json模块
- os模块

注意事项：
- 确保配置文件中的参数正确设置
- 回填前建议先执行dry_run检查任务
- 可以通过config_mode参数选择配置方式

版本历史：
- v1.0 (2024-07-28): 初始版本
"""

import json
import os
from datetime import datetime, timedelta
from scheduler_dag.scheduler_cli import run_backfill


def backfill_with_different_date_formats(config_mode='json'):
    """使用多种日期格式进行回填的详细示例
    
    Args:
        config_mode: 配置方式，'json'使用JSON文件，'code'使用代码定义
    """
    print("开始执行多日期格式回填示例...")
    
    if config_mode == 'json':
        # 使用JSON配置文件
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                  "config", "backfill_params_example.json")
        
        print(f"\n从配置文件 {config_path} 加载参数:")
        with open(config_path, 'r') as f:
            backfill_params = json.load(f)
            print(json.dumps(backfill_params, indent=2))
        
        # 执行基于配置文件的回填
        run_backfill(
            workflow_file="workflows/example_workflow.yaml",
            backfill_params_file=config_path
        )
    else:
        # 通过代码构建多日期参数配置
        backfill_params = {
            "start_date": "2024-07-01",
            "end_date": "2024-07-05",
            "date_granularity": "day",
            # 主日期参数名称
            "date_param_name": "day_id",
            # 所有需要填充的日期参数名
            "date_param_names": [
                "day_id",           # 标准格式
                "batch_date",       # 无分隔符格式
                "data_date",        # 斜线分隔格式
                "report_date",      # 反向格式(日月年)
                "process_time",     # 带时间戳
                "fiscal_period"     # 年月格式
            ],
            # 每个参数对应的日期格式
            "date_param_formats": {
                "day_id": "%Y-%m-%d",          # 2024-07-01
                "batch_date": "%Y%m%d",        # 20240701
                "data_date": "%Y/%m/%d",       # 2024/07/01
                "report_date": "%d-%m-%Y",     # 01-07-2024
                "process_time": "%Y-%m-%dT%H:%M:%S",  # 2024-07-01T00:00:00
                "fiscal_period": "%Y%m"        # 202407
            },
            "dry_run": True,  # 先执行干运行，查看任务情况
            "params": {
                "data_path": "/data/backfill/example",
                "target_region": "all",
                "debug_mode": True,
                "max_retry": 2
            }
        }
        
        print("\n使用代码构建的配置参数:")
        print(json.dumps(backfill_params, indent=2))
        
        # 执行回填
        run_backfill(
            workflow_file="workflows/example_workflow.yaml",
            backfill_params=backfill_params
        )
    
    print("\n回填参数配置完成!")


def print_date_formats_examples():
    """打印多种日期格式示例，帮助理解日期格式参数"""
    # 示例日期
    dates = [
        datetime.now(),
        datetime.now() - timedelta(days=1),
        datetime.now() + timedelta(days=1)
    ]
    
    date_descriptions = [
        "当前日期",
        "昨天",
        "明天"
    ]
    
    # 日期格式及其描述
    formats = {
        "%Y-%m-%d": "标准ISO格式 (例如: 2024-07-01)",
        "%Y%m%d": "无分隔符格式 (例如: 20240701)",
        "%Y/%m/%d": "斜线分隔格式 (例如: 2024/07/01)",
        "%d-%m-%Y": "日-月-年格式 (例如: 01-07-2024)",
        "%b %d, %Y": "月名-日-年格式 (例如: Jul 01, 2024)",
        "%Y-%m-%dT%H:%M:%S": "ISO带时间格式 (例如: 2024-07-01T00:00:00)",
        "%Y%m": "年月格式 (例如: 202407)",
        "%Y-Q%q": "季度格式 (需手动处理, 例如: 2024-Q3)",
        "%Y-W%W": "年-周格式 (例如: 2024-W30)"
    }
    
    print("\n不同日期格式示例:")
    print("-" * 100)
    print(f"{'格式':22} | {'描述':40} | {'当前日期':20} | {'昨天':20} | {'明天':20}")
    print("-" * 100)
    
    for fmt, desc in formats.items():
        try:
            formatted_dates = [d.strftime(fmt) for d in dates]
            print(f"{fmt:22} | {desc:40} | {formatted_dates[0]:20} | {formatted_dates[1]:20} | {formatted_dates[2]:20}")
        except Exception as e:
            if "%q" in fmt:  # 季度格式需要特殊处理
                # 手动计算季度
                quarters = []
                for d in dates:
                    quarter = (d.month - 1) // 3 + 1
                    quarters.append(f"{d.year}-Q{quarter}")
                print(f"{fmt:22} | {desc:40} | {quarters[0]:20} | {quarters[1]:20} | {quarters[2]:20}")
            else:
                print(f"{fmt:22} | {desc:40} | 格式错误: {str(e):60}")


def create_sample_config():
    """创建示例配置文件"""
    config = {
        "start_date": "2024-07-01",
        "end_date": "2024-07-05",
        "date_granularity": "day",
        "date_param_names": [
            "day_id", 
            "batch_date", 
            "data_date", 
            "report_date",
            "process_time",
            "fiscal_period"
        ],
        "date_param_formats": {
            "day_id": "%Y-%m-%d",
            "batch_date": "%Y%m%d",
            "data_date": "%Y/%m/%d",
            "report_date": "%d-%m-%Y",
            "process_time": "%Y-%m-%dT%H:%M:%S",
            "fiscal_period": "%Y%m"
        },
        "dry_run": True,
        "params": {
            "data_path": "/data/backfill/sample",
            "target_region": "all",
            "debug_mode": True,
            "max_retry": 2
        }
    }
    
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
    os.makedirs(config_dir, exist_ok=True)
    
    config_path = os.path.join(config_dir, "date_formats_sample.json")
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"\n示例配置文件已创建: {config_path}")


def explain_date_param_config():
    """解释日期参数配置的使用方法"""
    print("\n日期参数配置说明:")
    print("-" * 80)
    print("1. date_param_names: 列表，包含所有需要在回填中使用的日期参数名")
    print("   - 每个参数名将根据对应的格式自动生成日期字符串")
    print("   - 回填过程中这些参数会自动传递给工作流任务")
    print()
    print("2. date_param_formats: 字典，定义每个日期参数使用的格式")
    print("   - 键: 与date_param_names中的参数名对应")
    print("   - 值: Python日期格式字符串 (strftime格式)")
    print()
    print("3. 日期参数可以在工作流任务中通过以下方式访问:")
    print("   - 原始格式: ${param_name}")
    print("   - 无破折号格式: ${param_name_no_dash} (自动移除'-')")
    print("   - 自定义格式: ${param_name_fmt} (按照date_param_formats中的格式)")
    print()
    print("4. 常用日期格式:")
    print("   - %Y-%m-%d: 2024-07-01")
    print("   - %Y%m%d: 20240701")
    print("   - %Y/%m/%d: 2024/07/01")
    print("   - %d-%m-%Y: 01-07-2024")
    print("   - %Y-%m-%dT%H:%M:%S: 2024-07-01T00:00:00")
    print("-" * 80)


if __name__ == "__main__":
    print("====== 多日期格式参数回填示例程序 ======")
    print_date_formats_examples()
    explain_date_param_config()
    create_sample_config()
    
    # 询问用户选择配置方式
    print("\n请选择配置方式:")
    print("1. 使用JSON配置文件")
    print("2. 使用代码定义配置")
    choice = input("请输入选项 (1/2, 默认为1): ").strip() or "1"
    
    config_mode = 'json' if choice == "1" else 'code'
    backfill_with_different_date_formats(config_mode=config_mode) 