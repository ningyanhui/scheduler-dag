#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-07-15 16:00
# @Author  : yanhui.ning
# @File    : config_workflow_example.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：演示如何使用配置文件创建和执行工作流

输入：配置文件路径

输出：工作流执行结果

使用方法：
python config_workflow_example.py --config ../config/screenrecog_workflow.json [--day_id 2024-07-15]

版本历史：
- v1.0 (2024-07-15): 初始版本
"""

import os
import sys
import argparse
import datetime
import logging
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scheduler.config import WorkflowConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="基于配置文件的工作流执行示例")
    parser.add_argument("--config", default="../config/screenrecog_workflow.json", help="工作流配置文件路径")
    parser.add_argument("--day_id", help="数据日期 (YYYY-MM-DD)，默认为昨天")
    parser.add_argument("--visualize", action="store_true", help="是否可视化工作流")
    parser.add_argument("--output_dir", default="./output", help="输出目录")
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
    logger = logging.getLogger("config_workflow")
    
    try:
        # 获取日期参数
        day_id = args.day_id
        if not day_id:
            # 默认使用昨天的日期
            day_id = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            
        logger.info(f"使用日期: {day_id}")
        
        # 加载配置文件
        config_path = os.path.abspath(args.config)
        logger.info(f"加载配置文件: {config_path}")
        
        workflow_config = WorkflowConfig.from_json(config_path)
        logger.info(f"成功加载工作流配置: {workflow_config.name}")
        
        # 创建工作流实例
        override_params = {
            "day_id": day_id,
            "day_id_no_dash": day_id.replace("-", "")
        }
        
        workflow = workflow_config.create_workflow(override_params=override_params)
        logger.info(f"成功创建工作流: {workflow.name}")
        
        # 获取任务列表
        task_ids = list(workflow._tasks.keys())
        logger.info(f"工作流包含的任务: {task_ids}")
        
        # 可视化工作流
        if args.visualize:
            try:
                output_dir = os.path.abspath(args.output_dir)
                os.makedirs(output_dir, exist_ok=True)
                
                vis_path = os.path.join(output_dir, f"workflow_{day_id.replace('-', '')}")
                logger.info(f"正在生成工作流可视化: {vis_path}")
                
                workflow.visualize(vis_path)
                logger.info(f"可视化文件已保存到: {vis_path}.png")
            except Exception as e:
                logger.error(f"可视化失败: {str(e)}")
        
        # 执行工作流
        logger.info("开始执行工作流...")
        results = workflow.execute()
        
        # 打印结果
        logger.info("工作流执行完成")
        for task_id, task_result in results.items():
            status = "成功" if task_result.get("success", False) else "失败"
            logger.info(f"任务 {task_id}: {status}")
            
            if not task_result.get("success", False):
                error = task_result.get("error", "未知错误")
                logger.error(f"  错误: {error}")
        
    except Exception as e:
        logger.error(f"执行过程中出错: {str(e)}")
        sys.exit(1)
        
    sys.exit(0)


if __name__ == "__main__":
    main() 