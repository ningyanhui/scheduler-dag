#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : example_workflow.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：示例工作流，展示如何使用scheduler_dag框架

输入：无

输出：执行结果

使用方法：python example_workflow.py

依赖项：scheduler_dag包

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import os
import sys
import logging
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scheduler.dag import Workflow
from scheduler.task import (
    ShellTask, 
    PythonTask, 
    SparkSQLTask, 
    HiveSQLTask, 
    PySparkTask
)


def sample_python_function(**kwargs):
    """示例Python函数，打印参数并返回结果"""
    print(f"执行Python函数，参数: {kwargs}")
    # 获取上游任务结果
    upstream_results = kwargs.get('upstream_results', {})
    if upstream_results:
        print(f"上游任务结果: {upstream_results}")
        
    # 返回一些结果
    return {
        "status": "success",
        "message": "Python函数执行成功",
        "data": {"value": 42}
    }


def create_example_workflow():
    """创建示例工作流"""
    
    # 创建工作流
    workflow = Workflow(
        name="示例工作流",
        description="展示各种任务类型和依赖关系的示例工作流"
    )
    
    # 设置全局参数
    workflow.set_params({
        "day_id": "${yyyy-MM-dd-1}",
        "data_path": "/tmp/example_data",
        "database": "tranai_dw"
    })
    
    # 创建Shell任务
    task1 = ShellTask(
        task_id="prepare_data",
        command="mkdir -p ${data_path} && echo '${day_id}' > ${data_path}/data.txt",
        params={"data_path": "${data_path}", "day_id": "${day_id}"}
    )
    
    # 创建Python任务
    task2 = PythonTask(
        task_id="process_data",
        python_callable=sample_python_function,
        params={"input_path": "${data_path}/data.txt", "day_id": "${day_id}"}
    )
    
    # 创建SQL任务（仅示例，不会实际执行）
    task3 = SparkSQLTask(
        task_id="analyze_data",
        sql="""
        -- 这是一个示例SQL，不会实际执行
        SELECT * FROM ${database}.dwd_screenrecog_black_device_df
        WHERE day_id = '${day_id}' limit 10
        """,
        params={"database": "${database}", "day_id": "${day_id}"},
        spark_config={"spark.app.name": "analyze_data_test"}
    )
    
    # 创建Shell任务作为最后一步
    task4 = ShellTask(
        task_id="cleanup",
        command="echo '清理临时数据 ${data_path}'",
        params={"data_path": "${data_path}"}
    )
    
    # 添加任务到工作流
    workflow.add_task(task1)
    workflow.add_task(task2)
    workflow.add_task(task3)
    workflow.add_task(task4)
    
    # 设置任务依赖关系
    workflow.set_dependency("prepare_data", "process_data")
    workflow.set_dependency("process_data", "analyze_data")
    workflow.set_dependency("analyze_data", "cleanup")
    
    return workflow


def main():
    """主函数"""
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建工作流
    workflow = create_example_workflow()
    
    try:
        # 可视化工作流（如果安装了graphviz）
        workflow.visualize()
        
        # 执行工作流
        print("\n开始执行工作流...")
        results = workflow.execute()
        
        # 打印结果
        print("\n工作流执行结果:")
        for task_id, result in results.items():
            print(f"{task_id}: {result}")
            
        # 也可以指定从某个任务开始执行
        print("\n从process_data开始执行工作流...")
        results = workflow.execute(start_from="process_data")
        
        # 打印结果
        print("\n部分执行结果:")
        for task_id, result in results.items():
            print(f"{task_id}: {result}")
            
    except Exception as e:
        print(f"工作流执行失败: {str(e)}")
        raise


if __name__ == "__main__":
    main() 