#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : dag.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：有向无环图(DAG)引擎实现，用于管理任务依赖关系和执行顺序

输入：任务及其依赖关系

输出：按依赖关系排序的执行计划

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import copy
import logging
import time
from collections import defaultdict, deque
from typing import Dict, List, Set, Any, Optional, Union, Callable

from .task import Task
from .params import ParamManager
from .alert import alert_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dag")


class DAG:
    """DAG引擎，管理任务依赖关系及执行顺序"""
    
    def __init__(self, name: str, description: str = ""):
        """
        初始化DAG对象
        
        Args:
            name: DAG名称
            description: DAG描述
        """
        self.name = name
        self.description = description
        self.tasks: Dict[str, Task] = {}  # 存储任务的字典，键为任务ID
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)  # 存储任务依赖，键为任务ID，值为其依赖的任务集合
        self.reverse_dependencies: Dict[str, Set[str]] = defaultdict(set)  # 反向依赖，键为任务ID，值为依赖于它的任务集合
        self.param_manager = ParamManager()  # 参数管理器
        
    def add_task(self, task: Task) -> 'DAG':
        """
        添加任务到DAG
        
        Args:
            task: 任务对象
            
        Returns:
            当前DAG对象，用于链式调用
        """
        if task.task_id in self.tasks:
            logger.warning(f"任务 {task.task_id} 已存在，将被覆盖")
        
        self.tasks[task.task_id] = task
        return self
    
    def set_dependency(self, upstream_task_id: str, downstream_task_id: str) -> 'DAG':
        """
        设置任务依赖关系
        
        Args:
            upstream_task_id: 上游任务ID
            downstream_task_id: 下游任务ID
            
        Returns:
            当前DAG对象，用于链式调用
        """
        if upstream_task_id not in self.tasks:
            raise ValueError(f"上游任务 {upstream_task_id} 不存在")
        if downstream_task_id not in self.tasks:
            raise ValueError(f"下游任务 {downstream_task_id} 不存在")
        
        # 添加依赖关系
        self.dependencies[downstream_task_id].add(upstream_task_id)
        # 添加反向依赖关系
        self.reverse_dependencies[upstream_task_id].add(downstream_task_id)
        
        return self
    
    def set_upstream(self, task_id: str, upstream_task_id: str) -> 'DAG':
        """
        设置上游任务
        
        Args:
            task_id: 当前任务ID
            upstream_task_id: 上游任务ID
            
        Returns:
            当前DAG对象，用于链式调用
        """
        return self.set_dependency(upstream_task_id, task_id)
    
    def set_downstream(self, task_id: str, downstream_task_id: str) -> 'DAG':
        """
        设置下游任务
        
        Args:
            task_id: 当前任务ID
            downstream_task_id: 下游任务ID
            
        Returns:
            当前DAG对象，用于链式调用
        """
        return self.set_dependency(task_id, downstream_task_id)
    
    def get_task(self, task_id: str) -> Task:
        """
        获取任务对象
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务对象
        """
        if task_id not in self.tasks:
            raise ValueError(f"任务 {task_id} 不存在")
        return self.tasks[task_id]
    
    def topological_sort(self) -> List[List[str]]:
        """
        拓扑排序，将任务划分为按层次排序的组，同一层次的任务可以并行执行
        
        Returns:
            任务ID的列表的列表，每个子列表为一个可并行执行的任务组
        """
        # 计算每个任务的入度（依赖任务数量）
        in_degree = {task_id: len(deps) for task_id, deps in self.dependencies.items()}
        # 为未出现在dependencies中的任务设置入度为0
        for task_id in self.tasks:
            if task_id not in in_degree:
                in_degree[task_id] = 0
        
        # 初始化结果列表和队列
        result = []
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        
        while queue:
            # 当前层级的任务
            current_level = list(queue)
            queue.clear()
            result.append(current_level)
            
            # 处理当前层级的每个任务
            for task_id in current_level:
                # 减少依赖于当前任务的任务的入度
                for downstream_id in self.reverse_dependencies[task_id]:
                    in_degree[downstream_id] -= 1
                    # 如果入度变为0，加入队列
                    if in_degree[downstream_id] == 0:
                        queue.append(downstream_id)
        
        # 检查是否存在环
        if sum(len(level) for level in result) != len(self.tasks):
            raise ValueError("DAG中存在环路，无法执行拓扑排序")
            
        return result
    
    def get_dependencies(self) -> Dict[str, Set[str]]:
        """
        获取任务之间的依赖关系
        
        Returns:
            依赖关系字典，键为上游任务ID，值为下游任务ID集合
        """
        # 将reverse_dependencies转换为上游->下游的格式
        dependencies = {}
        for upstream_id, downstream_ids in self.reverse_dependencies.items():
            if downstream_ids:  # 只添加有下游任务的任务
                dependencies[upstream_id] = set(downstream_ids)
        return dependencies
    
    def set_params(self, params: Dict[str, Any]) -> 'DAG':
        """
        设置DAG级别的全局参数
        
        Args:
            params: 参数字典
            
        Returns:
            当前DAG对象，用于链式调用
        """
        self.param_manager.set_params(params)
        return self
    
    def execute(self, start_from: Optional[str] = None, end_at: Optional[str] = None,
               only_tasks: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        执行DAG
        
        Args:
            start_from: 开始执行的任务ID，如果提供，则从该任务开始执行
            end_at: 结束执行的任务ID，如果提供，则执行到该任务为止
            only_tasks: 只执行指定的任务列表
            
        Returns:
            执行结果字典，键为任务ID，值为任务执行结果
        """
        # 获取拓扑排序后的任务组
        task_levels = self.topological_sort()
        
        # 确定要执行的任务
        tasks_to_execute = set()
        if only_tasks:
            # 如果只执行特定任务，验证这些任务存在
            for task_id in only_tasks:
                if task_id not in self.tasks:
                    raise ValueError(f"任务 {task_id} 不存在")
                tasks_to_execute.add(task_id)
        else:
            # 如果没有指定特定任务，则执行全部任务
            # 但可能根据start_from和end_at进行筛选
            tasks_to_execute = set(self.tasks.keys())
        
        # 如果指定了start_from，过滤出该任务及其所有下游任务
        if start_from:
            if start_from not in self.tasks:
                raise ValueError(f"开始任务 {start_from} 不存在")
                
            # 从start_from开始的所有下游任务
            downstream_tasks = self._get_downstream_tasks(start_from)
            # 添加start_from本身
            downstream_tasks.add(start_from)
            
            # 更新要执行的任务集合
            tasks_to_execute &= downstream_tasks
        
        # 如果指定了end_at，过滤出该任务及其所有上游任务
        if end_at:
            if end_at not in self.tasks:
                raise ValueError(f"结束任务 {end_at} 不存在")
                
            # 获取end_at及其所有上游任务
            upstream_tasks = self._get_upstream_tasks(end_at)
            # 添加end_at本身
            upstream_tasks.add(end_at)
            
            # 更新要执行的任务集合
            tasks_to_execute &= upstream_tasks
        
        # 执行筛选后的任务
        results = {}
        for level in task_levels:
            level_tasks = [task_id for task_id in level if task_id in tasks_to_execute]
            
            if not level_tasks:
                continue
                
            logger.info(f"执行任务级别: {level_tasks}")
            
            # 这里可以实现并行执行逻辑，目前是顺序执行
            for task_id in level_tasks:
                task = self.tasks[task_id]
                # 获取所有上游任务结果传递给当前任务
                upstream_results = {up_id: results[up_id] for up_id in self.dependencies.get(task_id, set()) if up_id in results}
                
                # 替换任务中的参数
                task.resolve_params(self.param_manager)
                
                # 执行任务
                logger.info(f"开始执行任务: {task_id}")
                start_time = time.time()
                try:
                    task_result = task.execute(upstream_results)
                    results[task_id] = task_result
                    duration = time.time() - start_time
                    logger.info(f"任务 {task_id} 执行成功，耗时 {duration:.2f} 秒")
                except Exception as e:
                    duration = time.time() - start_time
                    logger.error(f"任务 {task_id} 执行失败，耗时 {duration:.2f} 秒: {str(e)}")
                    raise
                
        return results
    
    def _get_downstream_tasks(self, task_id: str) -> Set[str]:
        """
        获取任务的所有下游任务（包括间接下游）
        
        Args:
            task_id: 任务ID
            
        Returns:
            下游任务ID集合
        """
        result = set()
        queue = deque([task_id])
        visited = set()
        
        while queue:
            current = queue.popleft()
            visited.add(current)
            
            for downstream in self.reverse_dependencies.get(current, set()):
                result.add(downstream)
                if downstream not in visited:
                    queue.append(downstream)
                    
        return result
    
    def _get_upstream_tasks(self, task_id: str) -> Set[str]:
        """
        获取任务的所有上游任务（包括间接上游）
        
        Args:
            task_id: 任务ID
            
        Returns:
            上游任务ID集合
        """
        result = set()
        queue = deque([task_id])
        visited = set()
        
        while queue:
            current = queue.popleft()
            visited.add(current)
            
            for upstream in self.dependencies.get(current, set()):
                result.add(upstream)
                if upstream not in visited:
                    queue.append(upstream)
                    
        return result
    
    def visualize(self, filename: str = None) -> None:
        """
        可视化DAG（需要安装graphviz）
        
        Args:
            filename: 输出文件名，不包括扩展名
        """
        try:
            import graphviz
        except ImportError:
            logger.warning("无法导入graphviz库，无法生成可视化图形。请安装：pip install graphviz")
            return
            
        dot = graphviz.Digraph(comment=f'DAG: {self.name}')
        
        # 添加节点
        for task_id, task in self.tasks.items():
            dot.node(task_id, label=f"{task_id}\n({task.task_type})")
            
        # 添加边
        for downstream, upstreams in self.dependencies.items():
            for upstream in upstreams:
                dot.edge(upstream, downstream)
                
        # 渲染
        if filename:
            dot.render(filename, view=True)
        else:
            print(dot.source)


class Workflow:
    """工作流类，包装DAG并提供更多扩展功能"""
    
    def __init__(self, name: str, description: str = ""):
        """
        初始化工作流
        
        Args:
            name: 工作流名称
            description: 工作流描述
        """
        self.name = name
        self.description = description
        self.dag = DAG(name, description)
        self.execution_history = []
        self.fail_fast = True  # 默认失败即终止整个工作流
        self.send_alert_on_failure = False  # 默认不发送告警
        
    def add_task(self, task: Task) -> 'Workflow':
        """
        添加任务
        
        Args:
            task: 任务对象
            
        Returns:
            当前工作流对象，用于链式调用
        """
        self.dag.add_task(task)
        return self
        
    def set_dependency(self, upstream_task_id: str, downstream_task_id: str) -> 'Workflow':
        """
        设置任务依赖
        
        Args:
            upstream_task_id: 上游任务ID
            downstream_task_id: 下游任务ID
            
        Returns:
            当前工作流对象，用于链式调用
        """
        self.dag.set_dependency(upstream_task_id, downstream_task_id)
        return self
        
    def set_params(self, params: Dict[str, Any]) -> 'Workflow':
        """
        设置全局参数
        
        Args:
            params: 参数字典
            
        Returns:
            当前工作流对象，用于链式调用
        """
        self.dag.set_params(params)
        return self
    
    def set_fail_fast(self, fail_fast: bool) -> 'Workflow':
        """
        设置是否快速失败
        
        Args:
            fail_fast: 当任务失败时是否终止整个工作流
            
        Returns:
            当前工作流对象，用于链式调用
        """
        self.fail_fast = fail_fast
        return self
    
    def enable_feishu_alert(self, webhook_url: str, **kwargs) -> 'Workflow':
        """
        启用飞书告警
        
        Args:
            webhook_url: 飞书webhook URL
            **kwargs: 其他配置项
            
        Returns:
            当前工作流对象，用于链式调用
        """
        alert_manager.enable_feishu_alert(webhook_url, **kwargs)
        self.send_alert_on_failure = True
        return self
    
    def disable_alert(self) -> 'Workflow':
        """
        禁用告警
        
        Returns:
            当前工作流对象，用于链式调用
        """
        self.send_alert_on_failure = False
        return self
        
    def execute(self, start_from: Optional[str] = None, end_at: Optional[str] = None,
               only_tasks: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        执行工作流
        
        Args:
            start_from: 开始执行的任务ID
            end_at: 结束执行的任务ID
            only_tasks: 只执行指定的任务列表
            
        Returns:
            执行结果字典
        """
        start_time = time.time()
        logger.info(f"开始执行工作流: {self.name}")
        completed_tasks = []
        failed_task_id = None
        error_message = None
        
        try:
            # 获取拓扑排序后的任务组
            task_levels = self.dag.topological_sort()
            
            # 确定要执行的任务
            tasks_to_execute = set()
            if only_tasks:
                # 如果只执行特定任务，验证这些任务存在
                for task_id in only_tasks:
                    if task_id not in self.dag.tasks:
                        raise ValueError(f"任务 {task_id} 不存在")
                    tasks_to_execute.add(task_id)
            else:
                # 如果没有指定特定任务，则执行全部任务
                # 但可能根据start_from和end_at进行筛选
                tasks_to_execute = set(self.dag.tasks.keys())
            
            # 如果指定了start_from，过滤出该任务及其所有下游任务
            if start_from:
                if start_from not in self.dag.tasks:
                    raise ValueError(f"开始任务 {start_from} 不存在")
                    
                # 从start_from开始的所有下游任务
                downstream_tasks = self.dag._get_downstream_tasks(start_from)
                # 添加start_from本身
                downstream_tasks.add(start_from)
                
                # 更新要执行的任务集合
                tasks_to_execute &= downstream_tasks
            
            # 如果指定了end_at，过滤出该任务及其所有上游任务
            if end_at:
                if end_at not in self.dag.tasks:
                    raise ValueError(f"结束任务 {end_at} 不存在")
                    
                # 获取end_at及其所有上游任务
                upstream_tasks = self.dag._get_upstream_tasks(end_at)
                # 添加end_at本身
                upstream_tasks.add(end_at)
                
                # 更新要执行的任务集合
                tasks_to_execute &= upstream_tasks
            
            # 存储一份最初计划执行的任务集合（用于后续计算未执行任务）
            planned_tasks = tasks_to_execute.copy()
            
            # 执行筛选后的任务
            results = {}
            for level in task_levels:
                level_tasks = [task_id for task_id in level if task_id in tasks_to_execute]
                
                if not level_tasks:
                    continue
                    
                logger.info(f"执行任务级别: {level_tasks}")
                
                # 这里可以实现并行执行逻辑，目前是顺序执行
                for task_id in level_tasks:
                    task = self.dag.tasks[task_id]
                    # 获取所有上游任务结果传递给当前任务
                    upstream_results = {up_id: results[up_id] for up_id in self.dag.dependencies.get(task_id, set()) if up_id in results}
                    
                    # 替换任务中的参数
                    task.resolve_params(self.dag.param_manager)
                    
                    # 执行任务
                    logger.info(f"开始执行任务: {task_id}")
                    task_start_time = time.time()
                    try:
                        task_result = task.execute(upstream_results)
                        results[task_id] = task_result
                        completed_tasks.append(task_id)
                        duration = time.time() - task_start_time
                        logger.info(f"任务 {task_id} 执行成功，耗时 {duration:.2f} 秒")
                    except Exception as e:
                        duration = time.time() - task_start_time
                        error_message = str(e)
                        failed_task_id = task_id
                        logger.error(f"任务 {task_id} 执行失败，耗时 {duration:.2f} 秒: {error_message}")
                        
                        # 如果设置了快速失败，则立即中断整个工作流
                        if self.fail_fast:
                            raise
            
            status = "SUCCESS"
            return results
        except Exception as e:
            logger.error(f"工作流执行失败: {str(e)}")
            if not error_message:
                error_message = str(e)
            status = "FAILED"
            
            # 如果启用了告警并且有失败任务，发送告警
            if self.send_alert_on_failure and failed_task_id:
                # 计算未执行的任务
                executed_tasks = set(completed_tasks + [failed_task_id])
                uncompleted_tasks = sorted(list(planned_tasks - executed_tasks))
                
                # 发送告警，包含未执行任务列表
                alert_manager.send_workflow_failed_alert(
                    workflow_name=self.name,
                    start_time=start_time,
                    failed_task_id=failed_task_id,
                    failed_reason=error_message,
                    completed_tasks=completed_tasks,
                    uncompleted_tasks=uncompleted_tasks
                )
                
            raise
        finally:
            end_time = time.time()
            duration = end_time - start_time
            
            # 记录执行历史
            execution_record = {
                "start_time": start_time,
                "end_time": end_time,
                "duration": duration,
                "status": status,
                "params": copy.deepcopy(self.dag.param_manager.params),
                "start_from": start_from,
                "end_at": end_at,
                "only_tasks": only_tasks,
                "completed_tasks": completed_tasks,
                "failed_task_id": failed_task_id,
                "error_message": error_message
            }
            self.execution_history.append(execution_record)
            
            logger.info(f"工作流 {self.name} 执行完成，状态: {status}，耗时: {duration:.2f} 秒")
            
        return results
    
    def get_execution_history(self) -> List[Dict[str, Any]]:
        """
        获取工作流执行历史
        
        Returns:
            执行历史记录列表
        """
        return self.execution_history
    
    def visualize(self, filename: str = None) -> None:
        """
        可视化工作流
        
        Args:
            filename: 输出文件名，不包括扩展名
        """
        return self.dag.visualize(filename) 