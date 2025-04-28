#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024-07-15 14:30
# @Author  : yanhui.ning
# @File    : config.py
# @Project : scheduler_dag
# @Software: PyCharm
# @Description: 
"""
功能描述：工作流配置解析模块

输入：配置文件路径

输出：工作流配置对象

使用方法：
from scheduler.config import WorkflowConfig
config = WorkflowConfig.from_json("workflow_config.json")
workflow = config.create_workflow()

版本历史：
- v1.0 (2024-07-15): 初始版本
- v1.1 (2024-07-16): 添加对PythonTask自定义命令的支持
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union

from .dag import Workflow
from .task import (
    Task, ShellTask, PythonTask, PySparkTask, 
    SparkSQLTask, HiveSQLTask
)

logger = logging.getLogger(__name__)


class WorkflowConfig:
    """工作流配置类，用于解析和加载工作流配置"""
    
    def __init__(
        self, 
        name: str,
        description: str = "",
        params: Optional[Dict[str, Any]] = None,
        tasks: Optional[List[Dict[str, Any]]] = None,
        dependencies: Optional[List[Dict[str, str]]] = None,
        alert: Optional[Dict[str, Any]] = None
    ):
        """
        初始化工作流配置
        
        Args:
            name: 工作流名称
            description: 工作流描述
            params: 工作流参数
            tasks: 任务配置列表
            dependencies: 任务依赖关系列表
            alert: 告警配置
        """
        self.name = name
        self.description = description
        self.params = params or {}
        self.tasks = tasks or []
        self.dependencies = dependencies or []
        self.alert = alert or {}
        
    @classmethod
    def from_json(cls, json_file: str) -> 'WorkflowConfig':
        """
        从JSON文件加载工作流配置
        
        Args:
            json_file: JSON配置文件路径
            
        Returns:
            工作流配置对象
        """
        if not os.path.exists(json_file):
            raise FileNotFoundError(f"配置文件不存在: {json_file}")
            
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                
            # 验证基本字段
            if 'name' not in config_data:
                raise ValueError("配置文件缺少必要字段: name")
                
            return cls(
                name=config_data.get('name'),
                description=config_data.get('description', ''),
                params=config_data.get('params', {}),
                tasks=config_data.get('tasks', []),
                dependencies=config_data.get('dependencies', []),
                alert=config_data.get('alert', {})
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON格式错误: {str(e)}")
        except Exception as e:
            raise ValueError(f"加载配置文件失败: {str(e)}")
    
    def create_workflow(self, override_params: Optional[Dict[str, Any]] = None) -> Workflow:
        """
        根据配置创建工作流对象
        
        Args:
            override_params: 覆盖配置中的参数
            
        Returns:
            工作流对象
        """
        # 创建工作流
        workflow = Workflow(
            name=self.name,
            description=self.description
        )
        
        # 设置参数
        params = dict(self.params)
        if override_params:
            params.update(override_params)
        
        workflow.set_params(params)
        
        # 创建任务
        for task_config in self.tasks:
            task = self._create_task(task_config)
            workflow.add_task(task)
            
        # 设置依赖关系
        for dep in self.dependencies:
            if 'from' in dep and 'to' in dep:
                # 处理上游任务，支持逗号分隔的多任务
                upstream_task_ids = [tid.strip() for tid in dep['from'].split(',')]
                # 处理下游任务，支持逗号分隔的多任务
                downstream_task_ids = [tid.strip() for tid in dep['to'].split(',')]
                
                # 设置多对多的依赖关系
                for upstream_id in upstream_task_ids:
                    for downstream_id in downstream_task_ids:
                        workflow.set_dependency(upstream_id, downstream_id)
                
        # 设置告警
        if self.alert:
            alert_type = self.alert.get('type')
            if alert_type == 'feishu':
                webhook_url = self.alert.get('webhook_url', '')
                at_all = self.alert.get('at_all', False)
                at_users = self.alert.get('at_users', [])
                
                if webhook_url:
                    workflow.enable_feishu_alert(
                        webhook_url=webhook_url,
                        at_all=at_all,
                        at_users=at_users
                    )
                    
            # 设置失败行为
            fail_fast = self.alert.get('fail_fast', True)  # 默认为True
            workflow.set_fail_fast(fail_fast)
                
        return workflow
    
    def _create_task(self, task_config: Dict[str, Any]) -> Task:
        """
        根据配置创建任务对象
        
        Args:
            task_config: 任务配置
            
        Returns:
            任务对象
        """
        task_id = task_config.get('task_id')
        task_type = task_config.get('type')
        
        if not task_id:
            raise ValueError("任务配置缺少必要字段: task_id")
        if not task_type:
            raise ValueError(f"任务 {task_id} 配置缺少必要字段: type")
            
        params = task_config.get('params', {})
        
        # 根据任务类型创建相应的任务对象
        if task_type == 'shell':
            command = task_config.get('command')
            if not command:
                raise ValueError(f"Shell任务 {task_id} 配置缺少必要字段: command")
                
            return ShellTask(
                task_id=task_id,
                command=command,
                params=params,
                working_dir=task_config.get('working_dir')
            )
            
        elif task_type == 'python':
            return PythonTask(
                task_id=task_id,
                script_path=task_config.get('script_path'),
                script_content=task_config.get('script_content'),
                params=params,
                custom_command=task_config.get('custom_command'),
                working_dir=task_config.get('working_dir')
            )
            
        elif task_type == 'pyspark':
            return PySparkTask(
                task_id=task_id,
                script_path=task_config.get('script_path'),
                script_content=task_config.get('script_content'),
                params=params,
                spark_config=task_config.get('spark_config', {}),
                working_dir=task_config.get('working_dir')
            )
            
        elif task_type == 'spark-sql':
            return SparkSQLTask(
                task_id=task_id,
                sql=task_config.get('sql'),
                sql_file=task_config.get('sql_file'),
                params=params,
                spark_config=task_config.get('spark_config', {}),
                working_dir=task_config.get('working_dir'),
                init_script=task_config.get('init_script')
            )
            
        elif task_type == 'hive-sql':
            return HiveSQLTask(
                task_id=task_id,
                sql=task_config.get('sql'),
                sql_file=task_config.get('sql_file'),
                params=params,
                hive_config=task_config.get('hive_config', {}),
                working_dir=task_config.get('working_dir'),
                init_script=task_config.get('init_script')
            )
            
        else:
            raise ValueError(f"不支持的任务类型: {task_type}")
            
    def get_task_ids(self) -> List[str]:
        """
        获取所有任务ID
        
        Returns:
            任务ID列表
        """
        return [task.get('task_id') for task in self.tasks if 'task_id' in task] 


def load_workflow_from_config(config_data: Union[Dict[str, Any], str]) -> Workflow:
    """
    从配置数据或配置文件路径创建工作流对象
    
    Args:
        config_data: 配置数据字典或配置文件路径
        
    Returns:
        工作流对象
        
    Raises:
        ValueError: 配置数据无效
        FileNotFoundError: 配置文件不存在
    """
    if isinstance(config_data, str):
        # 从文件路径加载
        workflow_config = WorkflowConfig.from_json(config_data)
    elif isinstance(config_data, dict):
        # 从配置字典直接创建
        if 'name' not in config_data:
            raise ValueError("配置数据缺少必要字段: name")
            
        workflow_config = WorkflowConfig(
            name=config_data.get('name'),
            description=config_data.get('description', ''),
            params=config_data.get('params', {}),
            tasks=config_data.get('tasks', []),
            dependencies=config_data.get('dependencies', []),
            alert=config_data.get('alert', {})
        )
    else:
        raise ValueError("无效的配置数据类型，必须是字典或文件路径")
    
    # 创建工作流
    return workflow_config.create_workflow() 