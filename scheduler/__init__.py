#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : __init__.py
# @Project : scheduler_dag
# @Description: 
"""
任务调度器包初始化

功能描述：
基于有向无环图(DAG)的任务调度工具，支持以工作流形式串并联执行任务，
支持多种任务类型，支持任务重跑和回溯，支持参数传递和替换。

主要特性：
1. 支持以工作流形式串并联job任务
2. 支持多种job类型：spark-sql, python, pyspark, shell, hive-sql
3. 支持工作流、job级别的任务重跑和回溯
4. 支持工作流级别全局传参和job级别传参
5. 支持参数替换功能，支持日期类型参数自动解析

版本历史：
- v1.0.0 (2024-07-11): 初始版本
"""

from .dag import DAG, Workflow
from .task import (
    Task, 
    ShellTask, 
    PythonTask, 
    PySparkTask, 
    SparkSQLTask, 
    HiveSQLTask
)
from .params import ParamManager
from .utils import (
    get_date,
    parse_date_expr,
    ensure_dir,
    load_json_file,
    save_json_file
)

__version__ = '1.0.0'

__all__ = [
    'DAG', 
    'Workflow',
    'Task', 
    'ShellTask', 
    'PythonTask', 
    'PySparkTask', 
    'SparkSQLTask', 
    'HiveSQLTask',
    'ParamManager',
    'get_date',
    'parse_date_expr',
    'ensure_dir',
    'load_json_file',
    'save_json_file'
] 