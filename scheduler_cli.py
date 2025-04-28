#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/4/24 17:37
# @Author  : yanhui.ning
# @File    : scheduler_cli.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：通用工作流调度与回溯命令行工具

输入：
- 工作流配置文件路径
- 运行时参数文件路径（可选）
- 回溯参数文件路径（可选）
- 需要执行的任务ID列表（可选）
- 开始执行的任务ID（可选）

输出：
- 工作流执行结果
- 工作流可视化图表（当使用visualize命令时）
- 工作流信息（当使用info命令时）

使用方法：
1. 执行工作流：
   python scheduler_cli.py run --config workflow.json [--params params.json] [--job_ids job1,job2] [--start_from start_task_id]

2. 执行数据回溯：
   python scheduler_cli.py backfill --config workflow.json --backfill_params backfill_params.json [--job_ids job1,job2] [--start_from start_task_id]

3. 可视化工作流：
   python scheduler_cli.py visualize --config workflow.json [--output workflow.png] [--params params.json]

4. 查看工作流信息：
   python scheduler_cli.py info --config workflow.json

依赖项：
- argparse: 命令行参数解析
- json: JSON文件解析
- os: 文件路径处理
- datetime: 日期时间处理
- scheduler: 本地工作流调度引擎

注意事项：
- 工作流配置文件必须是有效的JSON格式
- 回溯任务时必须提供回溯参数文件
- 使用--start_from参数时，将从指定任务开始，执行该任务及其所有下游任务

版本历史：
- v1.0 (2024/4/24): 初始版本
- v1.1 (2024/4/25): 增加对不同日期粒度和自定义日期列表的支持
- v1.2 (2024/7/26): 增加实时日志输出功能
- v1.3 (2024/7/26): 增加多时间参数支持，可以配置多个日期参数名称和格式
- v1.4 (2024/7/29): 增加支持从指定节点开始执行功能
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# 导入本地模块
from scheduler.dag import Workflow
from scheduler.config import load_workflow_from_config
from scheduler.params import ParamsManager


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="通用工作流调度与回溯命令行工具")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # run 命令 - 执行单个工作流
    run_parser = subparsers.add_parser("run", help="执行工作流")
    run_parser.add_argument("--config", required=True, help="工作流配置文件路径")
    run_parser.add_argument("--params", help="参数配置文件路径")
    run_parser.add_argument("--job_ids", help="要执行的任务ID列表，多个ID用逗号分隔")
    run_parser.add_argument("--start_from", help="从指定任务开始执行，将执行该任务及其所有下游任务")
    
    # backfill 命令 - 执行数据回溯
    backfill_parser = subparsers.add_parser("backfill", help="执行数据回溯")
    backfill_parser.add_argument("--config", required=True, help="工作流配置文件路径")
    backfill_parser.add_argument("--backfill_params", required=True, help="回溯参数配置文件路径")
    backfill_parser.add_argument("--job_ids", help="要回溯的任务ID列表，多个ID用逗号分隔")
    backfill_parser.add_argument("--start_from", help="从指定任务开始回溯，将回溯该任务及其所有下游任务")
    backfill_parser.add_argument("--auto_confirm", action="store_true", help="自动确认回溯计划，不需要交互式输入")
    
    # visualize 命令 - 可视化工作流
    visualize_parser = subparsers.add_parser("visualize", help="可视化工作流")
    visualize_parser.add_argument("--config", required=True, help="工作流配置文件路径")
    visualize_parser.add_argument("--output", help="输出文件路径")
    visualize_parser.add_argument("--params", help="参数配置文件路径")
    
    # info 命令 - 查看工作流信息
    info_parser = subparsers.add_parser("info", help="查看工作流信息")
    info_parser.add_argument("--config", required=True, help="工作流配置文件路径")
    
    return parser.parse_args()


def load_json_file(file_path):
    """加载JSON文件内容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"错误: 无法加载文件 {file_path}: {str(e)}")
        sys.exit(1)


def get_date_range(start_date_str, end_date_str):
    """获取按天的日期范围列表"""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        if end_date < start_date:
            print("错误: 结束日期不能早于开始日期")
            sys.exit(1)
            
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)
        
        return date_range
    except ValueError as e:
        print(f"错误: 日期格式无效 (应为 YYYY-MM-DD): {str(e)}")
        sys.exit(1)


def get_week_range(start_date_str, end_date_str):
    """获取按周的日期范围列表，每周以周一为起始日"""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        if end_date < start_date:
            print("错误: 结束日期不能早于开始日期")
            sys.exit(1)
        
        # 调整到周一
        start_weekday = start_date.weekday()
        if start_weekday > 0:  # 如果不是周一，则调整到这周的周一
            start_date = start_date - timedelta(days=start_weekday)
        
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=7)  # 增加一周
        
        return date_range
    except ValueError as e:
        print(f"错误: 日期格式无效 (应为 YYYY-MM-DD): {str(e)}")
        sys.exit(1)


def get_month_range(start_date_str, end_date_str):
    """获取按月的日期范围列表，每月以1号为起始日"""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        if end_date < start_date:
            print("错误: 结束日期不能早于开始日期")
            sys.exit(1)
        
        # 调整到每月1号
        start_date = start_date.replace(day=1)
        
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date.strftime("%Y-%m-%d"))
            
            # 移动到下月1号
            month = current_date.month + 1
            year = current_date.year
            if month > 12:
                month = 1
                year += 1
            current_date = current_date.replace(year=year, month=month, day=1)
        
        return date_range
    except ValueError as e:
        print(f"错误: 日期格式无效 (应为 YYYY-MM-DD): {str(e)}")
        sys.exit(1)


def run_workflow(config_path, params_path=None, job_ids=None, start_from=None):
    """执行单个工作流"""
    print(f"开始执行工作流 [{config_path}]")
    
    try:
        # 先显示工作流信息
        print("\n========== 工作流信息 ==========")
        print(f"正在加载工作流信息...")
        show_workflow_info(config_path)
        print("========== 工作流信息结束 ==========\n")
    except Exception as e:
        print(f"显示工作流信息时出错: {str(e)}")
        # 继续执行，不因显示信息错误而中断流程
    
    # 加载工作流配置
    print("正在加载工作流配置...")
    workflow_config = load_json_file(config_path)
    
    # 加载运行时参数
    runtime_params = {}
    if params_path:
        print(f"正在加载运行时参数: {params_path}")
        runtime_params = load_json_file(params_path)
        print(f"已加载运行时参数: {params_path}")
    
    # 创建工作流
    print("正在创建工作流...")
    workflow = load_workflow_from_config(workflow_config)
    
    # 设置参数
    if runtime_params:
        print("正在设置工作流参数...")
        workflow.set_params(runtime_params)
    
    # 如果同时指定了job_ids和start_from，优先使用job_ids
    if job_ids and start_from:
        print(f"警告: 同时指定了--job_ids和--start_from，将优先使用--job_ids")
        start_from = None
    
    # 如果指定了任务ID，只执行这些任务
    if job_ids:
        task_ids = [tid.strip() for tid in job_ids.split(",")]
        print(f"将仅执行以下任务: {', '.join(task_ids)}")
        start_from = None
    else:
        task_ids = None
    
    # 如果指定了开始任务，从该任务开始执行
    if start_from:
        print(f"将从任务 {start_from} 开始执行（包括该任务及其所有下游任务）")
    else:
        if not job_ids:
            print("将执行全部任务")
    
    # 执行工作流
    print("\n开始执行工作流任务...")
    start_time = time.time()
    try:
        results = workflow.execute(only_tasks=task_ids, start_from=start_from)
        success = all(result["status"] == "success" for result in results.values())
        elapsed = time.time() - start_time
        
        print(f"\n执行结果汇总:")
        print(f"总共执行时间: {elapsed:.2f} 秒")
        print(f"状态: {'成功' if success else '失败'}")
        
        # 打印每个任务的执行结果
        for task_id, result in results.items():
            status = "✅" if result["status"] == "success" else "❌"
            duration = result.get("duration", 0)
            print(f"{status} {task_id} - 用时: {duration:.2f}秒")
            
            if result["status"] != "success":
                print(f"   错误信息: {result.get('error', '未知错误')}")
        
        # 如果执行失败，获取并显示未完成的任务列表
        if not success:
            # 确定应该执行的任务集合
            tasks_to_execute = set()
            
            # 如果指定了特定任务
            if task_ids:
                tasks_to_execute = set(task_ids)
            # 如果从特定任务开始
            elif start_from:
                # 获取start_from及其所有下游任务
                downstream_tasks = workflow.dag._get_downstream_tasks(start_from)
                downstream_tasks.add(start_from)
                tasks_to_execute = downstream_tasks
            # 如果执行全部任务
            else:
                tasks_to_execute = set(workflow.dag.tasks.keys())
            
            # 获取已完成任务ID
            completed_task_ids = set()
            failed_task_ids = set()
            for task_id, result in results.items():
                if result["status"] == "success":
                    completed_task_ids.add(task_id)
                else:
                    failed_task_ids.add(task_id)
            
            # 计算未完成任务ID - 应该执行但未出现在结果中的任务
            uncompleted_task_ids = tasks_to_execute - completed_task_ids - failed_task_ids
            
            # 显示未完成任务信息
            print("\n任务执行摘要:")
            print(f"已完成任务: {', '.join(sorted(completed_task_ids)) if completed_task_ids else '无'}")
            print(f"失败任务: {', '.join(sorted(failed_task_ids)) if failed_task_ids else '无'}")
            if uncompleted_task_ids:
                print(f"未执行任务: {', '.join(sorted(uncompleted_task_ids))}")
                print("原因: 由于上游任务失败，这些任务未被执行")
            
        return success
    except Exception as e:
        print(f"工作流执行失败: {str(e)}")
        return False


def run_backfill(config_path, backfill_params_path, job_ids=None, start_from=None, auto_confirm=False):
    """执行数据回溯"""
    print(f"开始执行数据回溯 [{config_path}]")
    
    # 加载工作流配置
    workflow_config = load_json_file(config_path)
    
    # 加载回溯参数
    backfill_config = load_json_file(backfill_params_path)
    
    # 获取日期参数名称 - 支持单个参数名或多个参数名列表
    date_param_name = backfill_config.get("date_param_name", "day_id")
    
    # 支持多日期参数名和格式配置
    date_param_names = backfill_config.get("date_param_names", [])
    date_param_formats = backfill_config.get("date_param_formats", {})
    
    # 如果只配置了单个参数名，转为列表便于统一处理
    if not date_param_names and date_param_name:
        date_param_names = [date_param_name]
    
    # 确定日期范围
    # 1. 优先使用自定义日期列表
    if "custom_dates" in backfill_config and backfill_config["custom_dates"]:
        date_range = backfill_config["custom_dates"]
        print(f"使用自定义日期列表，共 {len(date_range)} 个日期")
    
    # 2. 否则使用日期范围和粒度生成日期列表
    elif "start_date" in backfill_config and "end_date" in backfill_config:
        granularity = backfill_config.get("date_granularity", "day").lower()
        
        if granularity == "day":
            date_range = get_date_range(backfill_config["start_date"], backfill_config["end_date"])
            print(f"使用按天粒度生成日期范围，从 {backfill_config['start_date']} 到 {backfill_config['end_date']}，共 {len(date_range)} 天")
        elif granularity == "week":
            date_range = get_week_range(backfill_config["start_date"], backfill_config["end_date"])
            print(f"使用按周粒度生成日期范围，从 {backfill_config['start_date']} 到 {backfill_config['end_date']}，共 {len(date_range)} 周")
        elif granularity == "month":
            date_range = get_month_range(backfill_config["start_date"], backfill_config["end_date"])
            print(f"使用按月粒度生成日期范围，从 {backfill_config['start_date']} 到 {backfill_config['end_date']}，共 {len(date_range)} 月")
        else:
            print(f"错误: 不支持的日期粒度: {granularity}")
            return False
    else:
        print("错误: 必须提供 custom_dates 或者 start_date+end_date")
        return False
    
    # 检查是否为空运行
    dry_run = backfill_config.get("dry_run", False)
    if dry_run:
        print("注意: 这是一次空运行，不会实际执行任务")
    
    # 获取自定义参数
    custom_params = backfill_config.get("params", {})
    
    # 如果同时指定了job_ids和start_from，优先使用job_ids
    if job_ids and start_from:
        print(f"警告: 同时指定了--job_ids和--start_from，将优先使用--job_ids")
        start_from = None
    
    # 如果指定了任务ID，只执行这些任务
    if job_ids:
        task_ids = [tid.strip() for tid in job_ids.split(",")]
        print(f"将仅回溯以下任务: {', '.join(task_ids)}")
        start_from = None
    else:
        task_ids = None
    
    # 如果指定了开始任务，从该任务开始执行
    if start_from:
        print(f"将从任务 {start_from} 开始回溯（包括该任务及其所有下游任务）")
    
    # 显示回溯计划
    print(f"回溯计划:")
    print(f"日期范围: {date_range[0]} 至 {date_range[-1]} (共 {len(date_range)} 个日期点)")
    
    # 显示日期参数名称
    if date_param_names:
        print(f"日期参数名: {', '.join(date_param_names)}")
        
        # 如果存在自定义格式，显示格式信息
        if date_param_formats:
            print("日期参数格式:")
            for name, fmt in date_param_formats.items():
                print(f"  - {name}: {fmt}")
    
    if task_ids:
        print(f"目标任务: {', '.join(task_ids)}")
    elif start_from:
        print(f"从任务 {start_from} 开始回溯所有下游任务")
    else:
        print("目标任务: 所有任务")
    
    # 确认执行
    if not dry_run and not auto_confirm:
        try:
            confirm = input("确认执行以上回溯计划? (y/n): ").strip().lower()
            if confirm != 'y':
                print("回溯已取消")
                return False
        except Exception as e:
            print(f"获取确认输入失败: {str(e)}")
            print("如果在非交互式环境中运行，请使用 --auto_confirm 参数自动确认回溯计划")
            return False
    elif auto_confirm:
        print("已自动确认回溯计划")
    
    # 开始执行回溯
    total_start_time = time.time()
    success_count = 0
    failure_count = 0
    # 记录失败的日期点
    failed_date_points = []
    
    # 检查是否存在任务使用custom_command
    custom_command_tasks = {}
    for task_config in workflow_config.get("tasks", []):
        if "custom_command" in task_config:
            task_id = task_config.get("task_id")
            custom_command = task_config.get("custom_command")
            custom_command_tasks[task_id] = custom_command
    
    for date_point in date_range:
        print(f"\n[{date_point}] 开始回溯...")
        
        # 创建工作流配置副本，避免修改原始配置
        workflow_config_copy = json.loads(json.dumps(workflow_config))
        
        # 准备日期参数 - 根据配置生成不同格式的日期参数
        date_obj = datetime.strptime(date_point, "%Y-%m-%d")
        date_params = {}
        
        # 处理日期参数名列表
        for param_name in date_param_names:
            # 如果参数名有指定格式，则使用该格式
            if param_name in date_param_formats:
                format_str = date_param_formats[param_name]
                try:
                    # 将日期格式化为指定格式
                    formatted_date = date_obj.strftime(format_str)
                    date_params[param_name] = formatted_date
                except Exception as e:
                    print(f"警告: 格式化日期 {param_name} 失败: {str(e)}")
                    # 如果格式化失败，使用默认格式
                    date_params[param_name] = date_point
            else:
                # 如果没有指定格式，使用默认YYYY-MM-DD格式
                date_params[param_name] = date_point
        
        # 添加特殊参数 - 无破折号格式
        for param_name in date_param_names:
            if f"{param_name}_no_dash" not in date_params:
                if param_name in date_params:
                    no_dash_value = date_params[param_name].replace("-", "")
                    date_params[f"{param_name}_no_dash"] = no_dash_value
        
        # 直接修改工作流配置中的参数，处理日期变量
        if "params" in workflow_config_copy:
            workflow_params = workflow_config_copy.get("params", {})
            for key, value in workflow_params.items():
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    # 处理"${yyyy-MM-dd-N}"形式的日期变量，N表示偏移天数
                    param_value = value[2:-1]  # 去掉${}
                    
                    # 检测日期格式和偏移量
                    offset_days = 0
                    date_format = param_value
                    
                    # 更强大的偏移量检测 - 支持更多格式
                    # 先尝试找最常见的偏移模式
                    offset_patterns = ["-1", "-2", "-3", "-7", "-30", "+1", "+2", "+3", "+7", "+30"]
                    pattern_found = False
                    
                    for pattern in offset_patterns:
                        if pattern in param_value:
                            offset_days = int(pattern)
                            date_format = param_value.replace(pattern, "")
                            pattern_found = True
                            print(f"  检测到预定义偏移量: {pattern}, 日期格式: {date_format}")
                            break
                    
                    # 如果没找到预定义模式，尝试更复杂的正则表达式匹配
                    if not pattern_found:
                        import re
                        # 匹配如yyyy-MM-dd-1, yyyy-MM-dd+1等格式
                        offset_match = re.search(r'([+-]\d+)$', param_value)
                        if offset_match:
                            offset_str = offset_match.group(1)
                            try:
                                offset_days = int(offset_str)
                                # 移除偏移部分，保留日期格式
                                date_format = param_value[:param_value.rfind(offset_str)]
                                print(f"  检测到自定义偏移量: {offset_str}, 日期格式: {date_format}")
                            except ValueError:
                                print(f"  警告: 无法解析偏移量: {offset_str}, 使用默认值0")
                    
                    print(f"  处理日期变量: {param_value} -> 偏移量: {offset_days}, 格式: {date_format}")
                    
                    # 计算偏移后的日期
                    offset_date = date_obj + timedelta(days=offset_days)
                    
                    # 根据格式化字符串生成日期
                    date_value = None
                    # 处理标准化的日期格式
                    if date_format.lower() == "yyyy-mm-dd":
                        date_value = offset_date.strftime("%Y-%m-%d")
                    elif date_format.lower() == "yyyymmdd":
                        date_value = offset_date.strftime("%Y%m%d")
                    elif date_format.lower() == "yyyy/mm/dd":
                        date_value = offset_date.strftime("%Y/%m/%d")
                    else:
                        # 尝试转换自定义格式
                        try:
                            # 将yyyy-MM-dd转换为%Y-%m-%d等Python日期格式
                            python_format = date_format.lower()
                            python_format = python_format.replace("yyyy", "%Y")
                            python_format = python_format.replace("yy", "%y")  # 支持两位年份
                            python_format = python_format.replace("mm", "%m")
                            python_format = python_format.replace("dd", "%d")
                            python_format = python_format.replace("hh", "%H")  # 支持小时
                            python_format = python_format.replace("mi", "%M")  # 支持分钟
                            python_format = python_format.replace("ss", "%S")  # 支持秒
                            date_value = offset_date.strftime(python_format)
                        except Exception as e:
                            print(f"  警告: 日期格式'{date_format}'解析失败，使用默认格式: {e}")
                            date_value = offset_date.strftime("%Y-%m-%d")
                    
                    print(f"  解析结果: {key} = {date_value} (原始值: {value})")
                    
                    # 更新工作流配置参数值和回溯参数
                    workflow_params[key] = date_value
                    date_params[key] = date_value
                    
                    # 添加无破折号格式 (如果是日期格式)
                    if "-" in date_value:
                        no_dash_value = date_value.replace("-", "")
                        date_params[f"{key}_no_dash"] = no_dash_value
                        print(f"  添加无破折号格式: {key}_no_dash = {no_dash_value}")

        # 更新工作流配置中任务的参数，确保任务级别参数也被正确替换
        for task_config in workflow_config_copy.get("tasks", []):
            if "params" in task_config:
                task_params = task_config.get("params", {})
                for param_key, param_value in list(task_params.items()):  # 使用list创建副本以便安全修改字典
                    # 处理任务参数中的变量引用
                    if isinstance(param_value, str):
                        # 1. 处理对全局参数的直接引用，如${day_id}
                        if param_value.startswith("${") and param_value.endswith("}"):
                            referred_param = param_value[2:-1]  # 去掉${}
                            if referred_param in date_params:
                                old_value = task_params[param_key]
                                task_params[param_key] = date_params[referred_param]
                                print(f"  任务参数替换: {task_config.get('task_id')}.{param_key} = {old_value} -> {task_params[param_key]}")
                            else:
                                print(f"  警告: 任务 {task_config.get('task_id')} 引用了未定义的参数: {referred_param}")
                        
                        # 2. 处理包含参数引用的复杂表达式，如"prefix_${day_id}_suffix"
                        elif "${" in param_value and "}" in param_value:
                            # 替换所有${param}形式的引用
                            import re
                            old_value = param_value
                            # 找出所有${xxx}格式的变量引用
                            pattern = r'\${([^}]+)}'
                            matches = re.findall(pattern, param_value)
                            new_value = param_value
                            
                            for match in matches:
                                if match in date_params:
                                    placeholder = "${" + match + "}"
                                    new_value = new_value.replace(placeholder, str(date_params[match]))
                                    print(f"  部分参数替换: {placeholder} -> {date_params[match]}")
                                else:
                                    print(f"  警告: 任务 {task_config.get('task_id')} 引用了未定义的参数: {match}")
                            
                            # 更新任务参数
                            if new_value != old_value:
                                task_params[param_key] = new_value
                                print(f"  任务参数替换: {task_config.get('task_id')}.{param_key} = {old_value} -> {new_value}")
        
        # 修正特定任务的自定义命令
        for task_id, custom_command in custom_command_tasks.items():
            # 查找任务配置
            for task_config in workflow_config_copy.get("tasks", []):
                if task_config.get("task_id") == task_id:
                    # 创建一个新的自定义命令，替换所有参数
                    new_command = custom_command
                    
                    # 替换命令中的所有参数引用
                    for param_name, param_value in date_params.items():
                        placeholder = "{params." + param_name + "}"
                        new_command = new_command.replace(placeholder, param_value)
                    
                    # 替换命令中的特殊参数引用如{script_path}
                    if "{script_path}" in new_command:
                        script_path = task_config.get("script_path", "")
                        new_command = new_command.replace("{script_path}", script_path)
                    
                    # 更新任务配置
                    task_config["custom_command"] = new_command
                    break
        
        # 设置参数 - 合并日期参数和自定义参数
        day_params = {**date_params, **custom_params}
        
        # 输出调试信息
        print(f"\n[{date_point}] 全局参数列表:")
        for key, value in day_params.items():
            print(f"  - {key}: {value}")
        
        # 输出每个任务的参数情况
        print(f"\n[{date_point}] 任务参数详情:")
        for task_config in workflow_config_copy.get("tasks", []):
            task_id = task_config.get("task_id")
            print(f"  任务 {task_id} 参数:")
            
            # 获取任务特定参数
            task_params = task_config.get("params", {})
            
            # 输出该任务的参数
            if task_params:
                for param_key, param_value in sorted(task_params.items()):
                    print(f"    - {param_key}: {param_value}")
            else:
                print("    - 仅使用全局参数")
        
        # 基于更新后的配置创建工作流
        workflow = load_workflow_from_config(workflow_config_copy)
        
        # 设置参数
        workflow.set_params(day_params)
        
        # 执行工作流
        if not dry_run:
            start_time = time.time()
            try:
                results = workflow.execute(only_tasks=task_ids, start_from=start_from)
                success = all(result["status"] == "success" for result in results.values())
                elapsed = time.time() - start_time
                
                if success:
                    success_count += 1
                    print(f"[{date_point}] ✅ 回溯成功 (用时: {elapsed:.2f}秒)")
                else:
                    failure_count += 1
                    print(f"[{date_point}] ❌ 回溯失败 (用时: {elapsed:.2f}秒)")
                    # 记录失败的日期点
                    failed_date_points.append(date_point)
                    
                    # 确定应该执行的任务集合
                    tasks_to_execute = set()
                    
                    # 如果指定了特定任务
                    if task_ids:
                        tasks_to_execute = set(task_ids)
                    # 如果从特定任务开始
                    elif start_from:
                        # 获取start_from及其所有下游任务
                        downstream_tasks = workflow.dag._get_downstream_tasks(start_from)
                        downstream_tasks.add(start_from)
                        tasks_to_execute = downstream_tasks
                    # 如果执行全部任务
                    else:
                        tasks_to_execute = set(workflow.dag.tasks.keys())
                    
                    # 获取已完成和失败的任务ID
                    completed_task_ids = set()
                    failed_task_ids = set()
                    for task_id, result in results.items():
                        if result["status"] == "success":
                            completed_task_ids.add(task_id)
                        else:
                            failed_task_ids.add(task_id)
                            # 提供更详细的错误信息
                            error_msg = result.get("error", "未知错误")
                            print(f"   - 任务 {task_id} 失败: {error_msg}")
                            
                            # 如果有额外的错误详情，打印出来
                            if "error_details" in result:
                                print(f"     错误详情: {result['error_details']}")
                            
                            # 打印任务执行时使用的参数
                            if "params" in result:
                                print(f"     执行参数:")
                                for param_key, param_value in result["params"].items():
                                    print(f"       {param_key}: {param_value}")
                    
                    # 计算未完成任务ID
                    uncompleted_task_ids = tasks_to_execute - completed_task_ids - failed_task_ids
                    
                    # 如果有未执行的任务，显示它们
                    if uncompleted_task_ids:
                        print(f"   未执行任务: {', '.join(sorted(uncompleted_task_ids))}")
                        print("   原因: 由于上游任务失败，这些任务未被执行")
            except Exception as e:
                failure_count += 1
                print(f"[{date_point}] ❌ 回溯失败: {str(e)}")
                # 记录失败的日期点和错误信息
                failed_date_points.append(date_point)
        else:
            print(f"[{date_point}] 空运行，跳过实际执行")
            # 如果是空运行，显示将要传递的参数
            print(f"  将传递以下参数:")
            for key, value in day_params.items():
                print(f"  - {key}: {value}")
    
    # 汇总结果
    total_elapsed = time.time() - total_start_time
    print(f"\n回溯执行完成:")
    print(f"总共执行时间: {total_elapsed:.2f} 秒")
    if not dry_run:
        print(f"成功: {success_count} 个日期点")
        print(f"失败: {failure_count} 个日期点")
        
        # 如果有失败的日期点，列出它们
        if failure_count > 0:
            print("\n失败日期点详情:")
            for failed_date in failed_date_points:
                print(f"  - {failed_date}")
            
            # 如果需要重新执行失败的日期点，提供命令示例
            if len(failed_date_points) > 0:
                print("\n要重新执行失败的日期点，可以使用以下命令:")
                backfill_params_path_base = os.path.basename(backfill_params_path)
                failed_params_path = f"{os.path.splitext(backfill_params_path_base)[0]}_failed.json"
                print(f"""
创建包含以下内容的文件 {failed_params_path}:
{{
    "custom_dates": {failed_date_points},
    "date_param_names": {date_param_names},
    "date_param_formats": {json.dumps(date_param_formats, indent=2)},
    "dry_run": false
}}

然后执行:
python scheduler_cli.py backfill --config {config_path} --backfill_params {failed_params_path}""")
            
        return failure_count == 0
    else:
        print("这是一次空运行，未实际执行任务")
        return True


def visualize_workflow(config_path, output_path=None, params_path=None):
    """可视化工作流"""
    try:
        # 检查是否安装了graphviz
        from scheduler.utils import visualize_workflow as viz_workflow
    except ImportError:
        print("错误: 可视化功能需要安装 graphviz 库")
        print("请运行: pip install graphviz")
        return False
    
    print(f"开始可视化工作流 [{config_path}]")
    
    # 加载工作流配置
    workflow_config = load_json_file(config_path)
    
    # 加载参数
    params = {}
    if params_path:
        params = load_json_file(params_path)
    
    # 创建工作流
    workflow = load_workflow_from_config(workflow_config)
    
    # 设置参数
    if params:
        workflow.set_params(params)
    
    # 设置输出路径
    if not output_path:
        base_name = os.path.splitext(os.path.basename(config_path))[0]
        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{base_name}_workflow.png")
    
    # 生成可视化图表
    try:
        viz_workflow(workflow, output_path)
        print(f"工作流可视化图表已保存至: {output_path}")
        return True
    except Exception as e:
        print(f"工作流可视化失败: {str(e)}")
        return False


def show_workflow_info(config_path):
    """显示工作流信息"""
    print(f"工作流信息 [{config_path}]")
    
    # 加载工作流配置
    workflow_config = load_json_file(config_path)
    
    # 创建工作流
    workflow = load_workflow_from_config(workflow_config)
    
    # 显示基本信息
    print(f"名称: {workflow.name}")
    print(f"描述: {workflow.description}")
    
    # 显示任务信息
    print(f"\n任务列表 (共 {len(workflow.dag.tasks)} 个):")
    for task_id, task in workflow.dag.tasks.items():
        task_type = task.__class__.__name__
        print(f"- {task_id} ({task_type})")
    
    # 显示依赖关系
    print("\n依赖关系:")
    dependencies = workflow.dag.get_dependencies()
    if dependencies:
        for source, targets in dependencies.items():
            for target in targets:
                print(f"- {source} -> {target}")
    else:
        print("- 无依赖关系")
    
    # 显示任务层级(拓扑排序)
    levels = workflow.dag.topological_sort()
    print("\n任务执行层级 (拓扑排序):")
    for i, level in enumerate(levels):
        print(f"- 层级 {i+1} (并行执行): {', '.join(level)}")
    
    # 显示全局参数
    print("\n全局参数:")
    if workflow.dag.param_manager.params:
        for key, value in workflow.dag.param_manager.params.items():
            print(f"- {key}: {value}")
    else:
        print("- 无全局参数")
    
    # 显示告警配置
    print("\n告警配置:")
    # 导入告警管理器
    from scheduler.alert import alert_manager
    if alert_manager.enabled:
        print(f"- 类型: 飞书告警")
        print(f"- 失败快速终止: {'是' if workflow.fail_fast else '否'}")
    else:
        print("- 告警功能未启用")
    
    return True


def main():
    """主函数"""
    args = parse_args()
    
    # 根据命令执行相应的功能
    if args.command == "run":
        success = run_workflow(args.config, args.params, args.job_ids, args.start_from)
    elif args.command == "backfill":
        success = run_backfill(args.config, args.backfill_params, args.job_ids, args.start_from, args.auto_confirm)
    elif args.command == "visualize":
        success = visualize_workflow(args.config, args.output, args.params)
    elif args.command == "info":
        success = show_workflow_info(args.config)
    else:
        print("错误: 未指定有效的命令")
        print("可用命令: run, backfill, visualize, info")
        print("使用 -h 或 --help 查看帮助")
        sys.exit(1)
    
    # 根据执行结果设置退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 