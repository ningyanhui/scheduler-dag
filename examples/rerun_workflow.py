#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : rerun_workflow.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：展示工作流重跑和回溯功能的示例

输入：无

输出：执行结果

使用方法：python rerun_workflow.py

依赖项：scheduler_dag包

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import os
import sys
import time
import logging
import argparse
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scheduler.dag import Workflow
from scheduler.task import ShellTask, PythonTask
from scheduler.utils import get_date, ensure_dir


def task1_func(**kwargs):
    """任务1：生成数据文件"""
    data_path = kwargs.get("data_path")
    day_id = kwargs.get("day_id")
    
    ensure_dir(data_path)
    output_file = os.path.join(data_path, f"data_{day_id}.txt")
    
    with open(output_file, 'w') as f:
        f.write(f"示例数据生成于 {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"数据日期: {day_id}\n")
        
    print(f"任务1: 数据已生成到 {output_file}")
    return {
        "output_file": output_file,
        "rows": 2
    }


def task2_func(**kwargs):
    """任务2：处理数据文件"""
    upstream_results = kwargs.get('upstream_results', {})
    task1_result = upstream_results.get('task1', {})
    input_file = task1_result.get('output_file')
    
    if not input_file or not os.path.exists(input_file):
        raise ValueError(f"输入文件不存在: {input_file}")
        
    data_path = kwargs.get("data_path")
    day_id = kwargs.get("day_id")
    
    output_file = os.path.join(data_path, f"processed_{day_id}.txt")
    
    # 读取输入文件
    with open(input_file, 'r') as f:
        lines = f.readlines()
        
    # 处理数据并写入输出文件
    with open(output_file, 'w') as f:
        f.write(f"处理后的数据，处理时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"原始行数: {len(lines)}\n")
        for line in lines:
            f.write(f"处理: {line}")
            
    print(f"任务2: 数据已处理并写入到 {output_file}")
    return {
        "input_file": input_file,
        "output_file": output_file,
        "processed_rows": len(lines)
    }


def task3_func(**kwargs):
    """任务3：汇总数据"""
    upstream_results = kwargs.get('upstream_results', {})
    task2_result = upstream_results.get('task2', {})
    input_file = task2_result.get('output_file')
    
    if not input_file or not os.path.exists(input_file):
        raise ValueError(f"输入文件不存在: {input_file}")
        
    data_path = kwargs.get("data_path")
    day_id = kwargs.get("day_id")
    
    output_file = os.path.join(data_path, f"summary_{day_id}.txt")
    
    # 读取输入文件
    with open(input_file, 'r') as f:
        lines = f.readlines()
        
    # 汇总数据并写入输出文件
    with open(output_file, 'w') as f:
        f.write(f"数据汇总，汇总时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"数据日期: {day_id}\n")
        f.write(f"总行数: {len(lines)}\n")
        f.write("=" * 50 + "\n")
        f.write("".join(lines))
            
    print(f"任务3: 数据已汇总并写入到 {output_file}")
    return {
        "input_file": input_file,
        "output_file": output_file,
        "summary_rows": len(lines)
    }


def task4_shell(**kwargs):
    """任务4：执行shell命令"""
    data_path = kwargs.get("data_path")
    day_id = kwargs.get("day_id")
    
    # 创建Shell任务
    cmd = f"echo '数据处理完成，日期: {day_id}' > {data_path}/complete_{day_id}.flag"
    
    return {
        "command": cmd
    }


def create_workflow(data_path, day_id):
    """创建工作流"""
    # 确保数据目录存在
    ensure_dir(data_path)
    
    # 创建工作流
    workflow = Workflow(
        name="数据处理工作流",
        description=f"数据处理示例，日期: {day_id}"
    )
    
    # 设置全局参数
    workflow.set_params({
        "day_id": day_id,
        "data_path": data_path
    })
    
    # 创建任务
    task1 = PythonTask(
        task_id="task1",
        python_callable=task1_func,
        params={"data_path": "${data_path}", "day_id": "${day_id}"}
    )
    
    task2 = PythonTask(
        task_id="task2",
        python_callable=task2_func,
        params={"data_path": "${data_path}", "day_id": "${day_id}"}
    )
    
    task3 = PythonTask(
        task_id="task3",
        python_callable=task3_func,
        params={"data_path": "${data_path}", "day_id": "${day_id}"}
    )
    
    task4 = ShellTask(
        task_id="task4",
        command="echo '数据处理完成，日期: ${day_id}' > ${data_path}/complete_${day_id}.flag",
        params={"data_path": "${data_path}", "day_id": "${day_id}"}
    )
    
    # 添加任务到工作流
    workflow.add_task(task1)
    workflow.add_task(task2)
    workflow.add_task(task3)
    workflow.add_task(task4)
    
    # 设置任务依赖
    workflow.set_dependency("task1", "task2")
    workflow.set_dependency("task2", "task3")
    workflow.set_dependency("task3", "task4")
    
    return workflow


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="工作流重跑和回溯示例")
    parser.add_argument("--data_path", default="/tmp/workflow_data", help="数据目录路径")
    parser.add_argument("--day_id", default=get_date("%Y-%m-%d"), help="数据日期")
    parser.add_argument("--start_from", help="从指定任务开始执行")
    parser.add_argument("--end_at", help="执行到指定任务")
    parser.add_argument("--only_tasks", help="仅执行指定任务，用逗号分隔")
    parser.add_argument("--visualize", action="store_true", help="是否可视化工作流")
    return parser.parse_args()


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建工作流
    workflow = create_workflow(args.data_path, args.day_id)
    
    # 可视化工作流
    if args.visualize:
        try:
            print("可视化工作流...")
            workflow.visualize(os.path.join(args.data_path, "workflow"))
        except Exception as e:
            print(f"可视化失败: {str(e)}")
    
    try:
        # 解析只执行特定任务的参数
        only_tasks = None
        if args.only_tasks:
            only_tasks = [task.strip() for task in args.only_tasks.split(",")]
            
        # 执行工作流
        print(f"\n开始执行工作流，日期: {args.day_id}")
        print(f"  数据目录: {args.data_path}")
        if args.start_from:
            print(f"  从任务开始: {args.start_from}")
        if args.end_at:
            print(f"  执行到任务: {args.end_at}")
        if only_tasks:
            print(f"  仅执行任务: {only_tasks}")
            
        # 执行工作流
        results = workflow.execute(
            start_from=args.start_from, 
            end_at=args.end_at, 
            only_tasks=only_tasks
        )
        
        # 打印结果
        print("\n工作流执行结果:")
        for task_id, result in results.items():
            print(f"{task_id}: {result}")
            
        # 打印执行历史
        print("\n执行历史:")
        for i, history in enumerate(workflow.get_execution_history()):
            print(f"执行 {i+1}:")
            print(f"  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(history['start_time']))}")
            print(f"  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(history['end_time']))}")
            print(f"  耗时: {history['duration']:.2f} 秒")
            print(f"  状态: {history['status']}")
            if history.get('start_from'):
                print(f"  从任务开始: {history['start_from']}")
            if history.get('end_at'):
                print(f"  执行到任务: {history['end_at']}")
            if history.get('only_tasks'):
                print(f"  仅执行任务: {history['only_tasks']}")
                
    except Exception as e:
        print(f"工作流执行失败: {str(e)}")
        raise
        
    print("\n工作流执行完成。")
    print(f"可以尝试以下命令重跑特定任务:")
    print(f"python {__file__} --start_from task2 --day_id {args.day_id}")
    print(f"python {__file__} --only_tasks task2,task3 --day_id {args.day_id}")


if __name__ == "__main__":
    main() 