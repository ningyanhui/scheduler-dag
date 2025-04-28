#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : test_basic.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：基本功能单元测试

输入：无

输出：测试结果

使用方法：pytest -v test_basic.py

依赖项：pytest

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scheduler.dag import DAG, Workflow
from scheduler.task import ShellTask, PythonTask
from scheduler.params import ParamManager


class TestParamManager(unittest.TestCase):
    """测试参数管理器"""
    
    def test_param_resolution(self):
        """测试参数解析功能"""
        param_manager = ParamManager()
        param_manager.set_params({
            "day_id": "2024-07-11",
            "nested_param": "${day_id}"
        })
        
        # 测试普通参数
        self.assertEqual(param_manager.get_param("day_id"), "2024-07-11")
        
        # 测试嵌套参数
        resolved = param_manager.resolve_value("数据日期: ${nested_param}")
        self.assertEqual(resolved, "数据日期: 2024-07-11")
        
        # 测试不存在的参数
        resolved = param_manager.resolve_value("${unknown_param}")
        self.assertEqual(resolved, "${unknown_param}")
        
        # 测试日期表达式
        # 注意：由于使用当前日期计算，这里只测试格式正确性
        date_expr = param_manager.resolve_value("${yyyy-MM-dd+1}")
        self.assertRegex(date_expr, r'\d{4}-\d{2}-\d{2}')


class TestDAG(unittest.TestCase):
    """测试DAG引擎"""
    
    def test_topological_sort(self):
        """测试拓扑排序"""
        dag = DAG("test_dag")
        
        # 创建任务
        task1 = PythonTask("task1", python_callable=lambda: "task1")
        task2 = PythonTask("task2", python_callable=lambda: "task2")
        task3 = PythonTask("task3", python_callable=lambda: "task3")
        task4 = PythonTask("task4", python_callable=lambda: "task4")
        
        # 添加任务
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        dag.add_task(task4)
        
        # 设置依赖关系
        # task1 -> task2 -> task4
        #       -> task3 /
        dag.set_dependency("task1", "task2")
        dag.set_dependency("task1", "task3")
        dag.set_dependency("task2", "task4")
        dag.set_dependency("task3", "task4")
        
        # 拓扑排序
        levels = dag.topological_sort()
        
        # 验证结果
        self.assertEqual(len(levels), 3)  # 应有3个层级
        self.assertIn("task1", levels[0])  # 第一层是task1
        # 第二层是task2和task3，顺序不重要
        self.assertTrue(
            ("task2" in levels[1] and "task3" in levels[1]) or 
            ("task2" in levels[1] and "task3" in levels[1])
        )
        self.assertIn("task4", levels[2])  # 第三层是task4
        
    def test_execution_filtering(self):
        """测试执行过滤功能"""
        dag = DAG("test_dag")
        
        # 创建测试任务
        mock_task1 = MagicMock()
        mock_task1.task_id = "task1"
        mock_task1.task_type = "test"
        mock_task1.execute.return_value = "result1"
        mock_task1.resolve_params = MagicMock()
        
        mock_task2 = MagicMock()
        mock_task2.task_id = "task2"
        mock_task2.task_type = "test"
        mock_task2.execute.return_value = "result2"
        mock_task2.resolve_params = MagicMock()
        
        mock_task3 = MagicMock()
        mock_task3.task_id = "task3"
        mock_task3.task_type = "test"
        mock_task3.execute.return_value = "result3"
        mock_task3.resolve_params = MagicMock()
        
        # 添加任务
        dag.add_task(mock_task1)
        dag.add_task(mock_task2)
        dag.add_task(mock_task3)
        
        # 设置依赖
        dag.set_dependency("task1", "task2")
        dag.set_dependency("task2", "task3")
        
        # 测试start_from参数
        results = dag.execute(start_from="task2")
        self.assertNotIn("task1", results)
        self.assertIn("task2", results)
        self.assertIn("task3", results)
        
        # 重置mock
        mock_task1.execute.reset_mock()
        mock_task2.execute.reset_mock()
        mock_task3.execute.reset_mock()
        
        # 测试end_at参数
        results = dag.execute(end_at="task2")
        self.assertIn("task1", results)
        self.assertIn("task2", results)
        self.assertNotIn("task3", results)
        
        # 重置mock
        mock_task1.execute.reset_mock()
        mock_task2.execute.reset_mock()
        mock_task3.execute.reset_mock()
        
        # 测试only_tasks参数
        results = dag.execute(only_tasks=["task1", "task3"])
        self.assertIn("task1", results)
        self.assertNotIn("task2", results)
        self.assertIn("task3", results)


class TestShellTask(unittest.TestCase):
    """测试Shell任务"""
    
    def test_shell_task_execution(self):
        """测试Shell任务执行"""
        # 创建临时目录
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "output.txt")
            
            # 创建Shell任务
            task = ShellTask(
                task_id="test_shell",
                command=f"echo 'Hello World' > {output_file}"
            )
            
            # 执行任务
            result = task.execute()
            
            # 验证结果
            self.assertEqual(result["exit_code"], 0)
            self.assertTrue(os.path.exists(output_file))
            
            # 验证文件内容
            with open(output_file, "r") as f:
                content = f.read().strip()
                self.assertEqual(content, "Hello World")
                
    def test_shell_task_param_replacement(self):
        """测试Shell任务参数替换"""
        # 创建临时目录
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "output.txt")
            
            # 创建Shell任务
            task = ShellTask(
                task_id="test_shell",
                command="echo '${message}' > ${output_file}",
                params={
                    "message": "Hello World",
                    "output_file": output_file
                }
            )
            
            # 执行任务
            result = task.execute()
            
            # 验证结果
            self.assertEqual(result["exit_code"], 0)
            self.assertTrue(os.path.exists(output_file))
            
            # 验证文件内容
            with open(output_file, "r") as f:
                content = f.read().strip()
                self.assertEqual(content, "Hello World")


class TestPythonTask(unittest.TestCase):
    """测试Python任务"""
    
    def test_python_callable(self):
        """测试Python可调用函数"""
        # 定义测试函数
        def test_func(**kwargs):
            return {
                "message": "Hello World",
                "params": kwargs
            }
            
        # 创建Python任务
        task = PythonTask(
            task_id="test_python",
            python_callable=test_func,
            params={"param1": "value1", "param2": "value2"}
        )
        
        # 执行任务
        result = task.execute()
        
        # 验证结果
        self.assertEqual(result["message"], "Hello World")
        self.assertEqual(result["params"]["param1"], "value1")
        self.assertEqual(result["params"]["param2"], "value2")
        
    def test_python_task_param_replacement(self):
        """测试Python任务参数替换"""
        # 定义测试函数
        def test_func(**kwargs):
            return kwargs
            
        # 创建参数管理器
        param_manager = ParamManager()
        param_manager.set_params({
            "global_param": "global_value"
        })
        
        # 创建Python任务
        task = PythonTask(
            task_id="test_python",
            python_callable=test_func,
            params={
                "local_param": "local_value",
                "global_ref": "${global_param}"
            }
        )
        
        # 解析参数
        task.resolve_params(param_manager)
        
        # 执行任务
        result = task.execute()
        
        # 验证结果
        self.assertEqual(result["local_param"], "local_value")
        self.assertEqual(result["global_ref"], "global_value")


class TestWorkflow(unittest.TestCase):
    """测试工作流"""
    
    def test_workflow_execution(self):
        """测试工作流执行"""
        # 创建工作流
        workflow = Workflow("test_workflow")
        
        # 定义测试函数
        def task1_func(**kwargs):
            return {"value": 1}
            
        def task2_func(**kwargs):
            upstream_results = kwargs.get('upstream_results', {})
            task1_result = upstream_results.get('task1', {})
            value = task1_result.get('value', 0)
            return {"value": value + 1}
            
        def task3_func(**kwargs):
            upstream_results = kwargs.get('upstream_results', {})
            task2_result = upstream_results.get('task2', {})
            value = task2_result.get('value', 0)
            return {"value": value + 1}
            
        # 创建任务
        task1 = PythonTask("task1", python_callable=task1_func)
        task2 = PythonTask("task2", python_callable=task2_func)
        task3 = PythonTask("task3", python_callable=task3_func)
        
        # 添加任务到工作流
        workflow.add_task(task1)
        workflow.add_task(task2)
        workflow.add_task(task3)
        
        # 设置依赖
        workflow.set_dependency("task1", "task2")
        workflow.set_dependency("task2", "task3")
        
        # 执行工作流
        results = workflow.execute()
        
        # 验证结果
        self.assertEqual(results["task1"]["value"], 1)
        self.assertEqual(results["task2"]["value"], 2)
        self.assertEqual(results["task3"]["value"], 3)
        
        # 验证执行历史
        history = workflow.get_execution_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["status"], "SUCCESS")
        

if __name__ == "__main__":
    unittest.main() 