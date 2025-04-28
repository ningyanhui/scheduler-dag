#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : alert_example.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：飞书告警示例，展示工作流失败时的告警功能

输入：无

输出：工作流执行结果和告警

使用方法：python alert_example.py --webhook_url <飞书机器人webhook地址>

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
    """任务1：成功任务"""
    print(f"任务1执行成功，参数: {kwargs}")
    return {"status": "success", "data": "任务1结果"}


def task2_func(**kwargs):
    """任务2：成功任务"""
    print(f"任务2执行成功，参数: {kwargs}")
    return {"status": "success", "data": "任务2结果"}


def task3_func(**kwargs):
    """任务3：失败任务"""
    print(f"任务3即将失败...")
    # 模拟失败
    if kwargs.get("force_fail", True):
        raise Exception("任务3故意抛出的异常，用于测试告警功能")
    return {"status": "success", "data": "任务3结果"}


def task4_func(**kwargs):
    """任务4：永远不会执行到的任务"""
    print(f"任务4执行成功，参数: {kwargs}")
    return {"status": "success", "data": "任务4结果"}


def create_workflow():
    """创建工作流"""
    # 创建工作流
    workflow = Workflow(
        name="告警测试工作流",
        description="测试工作流失败时的告警功能"
    )
    
    # 设置全局参数
    workflow.set_params({
        "day_id": get_date("%Y-%m-%d"),
        "data_path": "/tmp/alert_test"
    })
    
    # 创建任务
    task1 = PythonTask(
        task_id="task1",
        python_callable=task1_func,
        params={"day_id": "${day_id}"}
    )
    
    task2 = PythonTask(
        task_id="task2",
        python_callable=task2_func,
        params={"day_id": "${day_id}"}
    )
    
    task3 = PythonTask(
        task_id="task3",
        python_callable=task3_func,
        params={"day_id": "${day_id}", "force_fail": True}
    )
    
    task4 = PythonTask(
        task_id="task4",
        python_callable=task4_func,
        params={"day_id": "${day_id}"}
    )
    
    # 添加任务到工作流
    workflow.add_task(task1)
    workflow.add_task(task2)
    workflow.add_task(task3)
    workflow.add_task(task4)
    
    # 设置任务依赖: 串行和并行结合
    # task1 -> task2 -> task4
    #       -> task3 /
    workflow.set_dependency("task1", "task2")
    workflow.set_dependency("task1", "task3")
    workflow.set_dependency("task2", "task4")
    workflow.set_dependency("task3", "task4")
    
    return workflow


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="飞书告警示例")
    parser.add_argument("--webhook_url", help="飞书机器人webhook地址",
                        default="https://open.feishu.cn/open-apis/bot/v2/hook/9f6ec07e-b53c-469e-b5ea-6df712daeaca")
    parser.add_argument("--visualize", action="store_true", help="是否可视化工作流")
    parser.add_argument("--at_all", action="store_true", help="是否@所有人")
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
    workflow = create_workflow()
    
    # 启用飞书告警
    workflow.enable_feishu_alert(args.webhook_url, at_all=args.at_all)
    
    # 可视化工作流
    if args.visualize:
        try:
            print("可视化工作流...")
            workflow.visualize("alert_workflow")
        except Exception as e:
            print(f"可视化失败: {str(e)}")
    
    try:
        # 输出工作流的串并联关系
        print("\n工作流的任务依赖关系:")
        task_levels = workflow.dag.topological_sort()
        for i, level in enumerate(task_levels):
            print(f"  第{i+1}层 (并行执行): {level}")
            
        # 执行工作流
        print("\n开始执行工作流...")
        workflow.execute()
        
    except Exception as e:
        print(f"\n工作流执行失败: {str(e)}")
        
        # 打印执行历史
        history = workflow.get_execution_history()[-1]  # 最后一次执行记录
        print("\n执行历史:")
        print(f"  开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(history['start_time']))}")
        print(f"  结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(history['end_time']))}")
        print(f"  耗时: {history['duration']:.2f} 秒")
        print(f"  状态: {history['status']}")
        print(f"  已完成任务: {history['completed_tasks']}")
        print(f"  失败任务: {history['failed_task_id']}")
        print(f"  错误信息: {history['error_message']}")
        
        print("\n飞书告警已发送，请检查机器人消息。")


if __name__ == "__main__":
    main() 