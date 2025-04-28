#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-07-20 10:00:00
# @Author  : yanhui.ning
# @File    : run_backfill_with_date_formats.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：
展示如何使用多种日期格式进行DAG的回填操作，并提供日期格式的示例和说明。

输入：
- 日期范围（开始日期和结束日期）
- 日期粒度（天、小时等）
- 多个日期参数名称及其对应的格式配置

输出：
- 回填执行日志
- 不同日期格式的示例展示

使用方法：
1. 直接运行脚本：python run_backfill_with_date_formats.py
2. 选择配置方式（JSON配置文件或代码定义的配置）
3. 查看回填操作的执行过程和不同日期格式的展示

依赖项：
- scheduler_dag模块
- json标准库
- datetime标准库
- os标准库

注意事项：
- 确保scheduler_dag模块已安装，且配置文件路径正确
- 日期格式必须遵循Python的datetime模块格式规范

版本历史：
- v1.0 (2024-07-20): 初始版本
"""

import json
import os
import datetime
from scheduler_dag.scheduler_cli import run_backfill


def backfill_with_different_date_formats(config_mode='json'):
    """
    使用不同的日期格式执行回填操作
    
    Args:
        config_mode (str): 配置模式，'json'使用JSON配置文件，'code'使用代码定义的配置
    """
    print("=" * 80)
    print("开始执行回填操作示例 - 使用多种日期格式")
    print("=" * 80)
    
    if config_mode.lower() == 'json':
        # 使用JSON配置文件
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                  'config', 'date_formats_sample.json')
        
        print(f"使用配置文件: {config_path}")
        
        if not os.path.exists(config_path):
            print("配置文件不存在，正在创建示例配置文件...")
            create_sample_config()
            print(f"已创建示例配置文件: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # 从配置文件加载参数
        start_date = config.get('start_date')
        end_date = config.get('end_date')
        date_granularity = config.get('date_granularity', 'day')
        date_param_names = config.get('date_param_names', [])
        date_param_formats = config.get('date_param_formats', {})
        dry_run = config.get('dry_run', True)
        params = config.get('params', {})
    else:
        # 使用代码定义的配置
        print("使用代码定义的配置")
        
        start_date = "2024-07-01"
        end_date = "2024-07-05"
        date_granularity = "day"
        date_param_names = [
            "day_id", "batch_date", "data_date", 
            "report_date", "process_time", "fiscal_period"
        ]
        date_param_formats = {
            "day_id": "%Y-%m-%d",       # 例如: 2024-07-01
            "batch_date": "%Y%m%d",     # 例如: 20240701
            "data_date": "%Y/%m/%d",    # 例如: 2024/07/01
            "report_date": "%d-%m-%Y",  # 例如: 01-07-2024
            "process_time": "%Y-%m-%dT%H:%M:%S",  # 例如: 2024-07-01T00:00:00
            "fiscal_period": "%Y%m"     # 例如: 202407
        }
        dry_run = True
        params = {
            "data_path": "/data/backfill/sample",
            "target_region": "all",
            "debug_mode": True,
            "max_retry": 2
        }
    
    print("\n配置信息:")
    print(f"  - 开始日期: {start_date}")
    print(f"  - 结束日期: {end_date}")
    print(f"  - 日期粒度: {date_granularity}")
    print(f"  - 干运行模式: {'是' if dry_run else '否'}")
    print(f"  - 日期参数名称: {', '.join(date_param_names)}")
    print("  - 日期参数格式:")
    for param, format_str in date_param_formats.items():
        print(f"    * {param}: {format_str}")
    
    # 展示不同日期格式的示例
    print_date_formats_examples(date_param_formats)
    
    # 显示参数配置说明
    explain_date_param_config()
    
    print("\n开始执行回填操作...")
    # 执行回填
    try:
        run_backfill(
            start_date=start_date,
            end_date=end_date,
            date_granularity=date_granularity,
            date_param_names=date_param_names,
            date_param_formats=date_param_formats,
            dry_run=dry_run,
            **params
        )
        print("回填操作完成!")
    except Exception as e:
        print(f"回填操作失败: {str(e)}")
    
    print("=" * 80)


def print_date_formats_examples(formats_dict):
    """
    打印不同日期格式的示例
    
    Args:
        formats_dict (dict): 包含参数名和日期格式的字典
    """
    print("\n日期格式示例:")
    print("-" * 60)
    print("| 参数名称        | 格式字符串         | 示例               | 说明                      |")
    print("-" * 60)
    
    date_obj = datetime.datetime(2024, 7, 1, 12, 30, 45)
    
    for param_name, format_str in formats_dict.items():
        try:
            example = date_obj.strftime(format_str)
            
            # 提供格式说明
            if format_str == "%Y-%m-%d":
                description = "年-月-日"
            elif format_str == "%Y%m%d":
                description = "年月日(无分隔符)"
            elif format_str == "%Y/%m/%d":
                description = "年/月/日"
            elif format_str == "%d-%m-%Y":
                description = "日-月-年"
            elif format_str == "%Y-%m-%dT%H:%M:%S":
                description = "ISO格式日期时间"
            elif format_str == "%Y%m":
                description = "年月(财务期间)"
            else:
                description = "自定义格式"
            
            print(f"| {param_name:<15} | {format_str:<18} | {example:<18} | {description:<25} |")
        except ValueError:
            print(f"| {param_name:<15} | {format_str:<18} | 格式错误           | 无效的日期格式          |")
    
    print("-" * 60)
    print("注: 以上示例基于日期 2024-07-01 12:30:45")


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
    
    config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
    os.makedirs(config_dir, exist_ok=True)
    
    config_path = os.path.join(config_dir, 'date_formats_sample.json')
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def explain_date_param_config():
    """展示日期参数配置的使用说明"""
    print("\n日期参数配置使用说明:")
    print("-" * 80)
    print("1. date_param_names: 定义在回填过程中需要使用的所有日期参数名称")
    print("   - 这些参数将在回填过程中为每个执行日期生成对应格式的值")
    print("   - 例如: ['day_id', 'batch_date', 'process_time']")
    print()
    print("2. date_param_formats: 定义每个日期参数的格式")
    print("   - 键是参数名称，值是对应的日期格式字符串")
    print("   - 格式遵循Python的datetime.strftime()方法的格式规范")
    print("   - 例如: {'day_id': '%Y-%m-%d', 'batch_date': '%Y%m%d'}")
    print()
    print("3. 在工作流任务中访问这些参数:")
    print("   - 通过context或kwargs等机制访问这些参数")
    print("   - 示例: ")
    print("     ```python")
    print("     def my_task(**kwargs):")
    print("         day_id = kwargs.get('day_id')  # 例如: '2024-07-01'")
    print("         batch_date = kwargs.get('batch_date')  # 例如: '20240701'")
    print("         # 使用这些参数处理业务逻辑")
    print("     ```")
    print("-" * 80)


if __name__ == "__main__":
    print("日期格式回填操作示例程序")
    print("\n请选择配置方式:")
    print("1. 使用JSON配置文件 (默认)")
    print("2. 使用代码定义的配置")
    
    choice = input("\n请输入选项 (1-2, 默认为1): ")
    
    if choice == "2":
        backfill_with_different_date_formats(config_mode='code')
    else:
        backfill_with_different_date_formats(config_mode='json') 