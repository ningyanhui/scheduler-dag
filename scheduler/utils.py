#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : utils.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：工具函数实现，提供日期处理、文件操作等辅助功能

输入：各种参数

输出：处理结果

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import os
import re
import datetime
import logging
import json
from typing import Dict, Any, Optional, Union, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("utils")


def get_date(date_format: str = '%Y-%m-%d', days_delta: int = 0) -> str:
    """
    获取日期字符串
    
    Args:
        date_format: 日期格式
        days_delta: 天数偏移量，可正可负
        
    Returns:
        日期字符串
    """
    target_date = datetime.datetime.now() + datetime.timedelta(days=days_delta)
    return target_date.strftime(date_format)


def parse_date_expr(date_expr: str) -> str:
    """
    解析日期表达式，支持 'yyyy-MM-dd-1' 形式
    
    Args:
        date_expr: 日期表达式
        
    Returns:
        解析后的日期字符串
    """
    # 日期模式匹配: 格式字符串后跟+或-和数字
    pattern = r'([a-zA-Z\-]+)([\+\-])(\d+)'
    match = re.match(pattern, date_expr)
    
    if match:
        # 解析日期格式
        date_format = match.group(1)
        operation = match.group(2)
        days = int(match.group(3))
        
        # 调整天数
        if operation == '+':
            delta_days = days
        else:  # operation == '-'
            delta_days = -days
            
        # 将自定义日期格式转换为Python格式
        py_format = convert_date_format(date_format)
        
        # 获取日期
        return get_date(py_format, delta_days)
    
    # 如果不是日期表达式，原样返回
    return date_expr


def convert_date_format(format_str: str) -> str:
    """
    将自定义日期格式转换为Python的datetime格式
    
    Args:
        format_str: 自定义日期格式
        
    Returns:
        Python日期格式字符串
    """
    # 映射字典
    mappings = {
        'yyyy': '%Y',
        'MM': '%m',
        'dd': '%d',
        'HH': '%H',
        'mm': '%M',
        'ss': '%S'
    }
    
    # 替换映射
    result = format_str
    for key, value in mappings.items():
        result = result.replace(key, value)
        
    return result


def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    加载JSON文件
    
    Args:
        file_path: 文件路径
        
    Returns:
        JSON解析后的字典
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"加载JSON文件失败: {str(e)}")
        raise
        

def save_json_file(data: Dict[str, Any], file_path: str, indent: int = 2) -> None:
    """
    保存JSON文件
    
    Args:
        data: 要保存的数据
        file_path: 文件路径
        indent: 缩进空格数
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
    except Exception as e:
        logger.error(f"保存JSON文件失败: {str(e)}")
        raise


def ensure_dir(directory: str) -> None:
    """
    确保目录存在，不存在则创建
    
    Args:
        directory: 目录路径
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def split_sql_file(sql_file: str) -> List[str]:
    """
    分割SQL文件为多个SQL语句
    
    Args:
        sql_file: SQL文件路径
        
    Returns:
        SQL语句列表
    """
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 按分号分割，跳过注释
        statements = []
        current_statement = []
        in_comment = False
        in_quote = False
        quote_char = None
        
        for line in content.split('\n'):
            # 跳过纯注释行
            if line.strip().startswith('--') or line.strip().startswith('#'):
                continue
                
            # 处理多行注释
            if '/*' in line and '*/' not in line:
                in_comment = True
                line = line.split('/*')[0]
                
            if '*/' in line and in_comment:
                in_comment = False
                line = line.split('*/')[1]
                
            if in_comment:
                continue
                
            # 清理行内注释
            comment_pos = -1
            for i, char in enumerate(line):
                if char in ['"', "'"]:
                    if not in_quote:
                        in_quote = True
                        quote_char = char
                    elif quote_char == char:
                        in_quote = False
                elif char == '-' and i + 1 < len(line) and line[i + 1] == '-' and not in_quote:
                    comment_pos = i
                    break
                    
            if comment_pos >= 0:
                line = line[:comment_pos]
                
            # 添加到当前语句
            if line.strip():
                current_statement.append(line)
                
            # 检查是否有分号
            if ';' in line:
                statements.append('\n'.join(current_statement))
                current_statement = []
                
        # 添加最后一个语句（如果没有分号结尾）
        if current_statement:
            statements.append('\n'.join(current_statement))
            
        return [stmt for stmt in statements if stmt.strip()]
    except Exception as e:
        logger.error(f"分割SQL文件失败: {str(e)}")
        raise


def get_timestamp() -> str:
    """
    获取当前时间戳字符串
    
    Returns:
        时间戳字符串，格式为 yyyyMMddHHmmss
    """
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S') 


def visualize_workflow(workflow, output_path: str = "workflow.png") -> None:
    """
    生成工作流的可视化图像
    
    Args:
        workflow: 工作流对象
        output_path: 输出图像文件路径
        
    Returns:
        None
    
    Raises:
        ImportError: 如果graphviz库未安装
    """
    try:
        import graphviz
    except ImportError:
        raise ImportError("缺少依赖: graphviz。请使用 pip install graphviz 安装。")
    
    # 创建图形对象
    dot = graphviz.Digraph(
        comment=f"Workflow: {workflow.name}",
        format="png",
        engine="dot"
    )
    
    # 设置图形属性
    dot.attr("graph", 
        rankdir="LR",  # 从左到右布局
        fontname="SimSun",
        fontsize="16",
        label=f"工作流: {workflow.name}",
        labelloc="t"  # 标签位置为顶部
    )
    
    # 设置节点默认属性
    dot.attr("node", 
        shape="box", 
        style="filled", 
        fillcolor="lightblue",
        fontname="SimSun"
    )
    
    # 设置边的默认属性
    dot.attr("edge", 
        fontname="SimSun",
        fontsize="10",
        arrowsize="0.5"
    )
    
    # 添加任务节点
    for task_id, task in workflow.tasks.items():
        task_type = task.__class__.__name__
        dot.node(task_id, f"{task_id}\n({task_type})")
    
    # 添加依赖边
    dependencies = workflow.dag.get_dependencies()
    for from_node, to_nodes in dependencies.items():
        for to_node in to_nodes:
            dot.edge(from_node, to_node)
    
    # 保存图像
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            ensure_dir(output_dir)
            
        # 保存图形
        dot.render(output_path, view=False, cleanup=True)
        
        # 如果输出文件指定为没有扩展名的路径，graphviz会自动添加.png
        # 这里检查并移除多余的扩展名
        if not output_path.endswith('.png'):
            rendered_path = f"{output_path}.png"
            if os.path.exists(rendered_path) and os.path.exists(output_path):
                os.rename(rendered_path, output_path)
                
        logger.info(f"工作流可视化已保存到: {output_path}")
    except Exception as e:
        logger.error(f"保存工作流可视化图像失败: {str(e)}")
        raise 