#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : task.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：任务基类及各类型任务实现

输入：任务配置和参数

输出：任务执行结果

版本历史：
- v1.0 (2024-07-11): 初始版本
- v1.1 (2024-07-15): 添加PythonTask的custom_command参数支持
- v1.2 (2024-07-26): 添加实时日志输出功能
"""

import os
import re
import sys
import logging
import subprocess
import tempfile
import threading
import queue
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union, Tuple
import shlex

from .params import ParamManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("task")


def stream_output(process, task_id):
    """
    实时处理子进程的输出流，同时收集完整输出

    Args:
        process: subprocess.Popen对象
        task_id: 任务ID，用于日志前缀

    Returns:
        Tuple[str, str]: 完整的stdout和stderr内容
    """
    def reader(pipe, pipe_name, output_queue):
        try:
            with pipe:
                for line in iter(pipe.readline, b''):
                    if not line:
                        break
                    line_str = line.decode('utf-8', errors='replace').rstrip()
                    # 将数据放入队列
                    output_queue.put((pipe_name, line_str))
        except (ValueError, IOError) as e:
            logger.error(f"读取{pipe_name}出错: {str(e)}")

    # 创建队列和线程
    output_queue = queue.Queue()
    stdout_thread = threading.Thread(
        target=reader, 
        args=(process.stdout, "stdout", output_queue)
    )
    stderr_thread = threading.Thread(
        target=reader, 
        args=(process.stderr, "stderr", output_queue)
    )
    
    # 启动线程
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()
    
    # 收集完整输出
    stdout_lines = []
    stderr_lines = []
    
    # 等待线程结束或进程结束
    while stdout_thread.is_alive() or stderr_thread.is_alive() or not output_queue.empty():
        try:
            # 从队列获取数据并处理
            pipe_name, line = output_queue.get(timeout=0.1)
            if pipe_name == "stdout":
                stdout_lines.append(line)
                print(f"[{task_id}] {line}")
            else:  # stderr
                stderr_lines.append(line)
                print(f"[{task_id}][ERROR] {line}", file=sys.stderr)
            output_queue.task_done()
        except queue.Empty:
            # 队列为空，检查进程是否结束
            if process.poll() is not None and output_queue.empty():
                break
        except Exception as e:
            logger.error(f"处理输出时出错: {str(e)}")
    
    # 确保线程结束
    stdout_thread.join()
    stderr_thread.join()
    
    # 返回完整的输出
    return '\n'.join(stdout_lines), '\n'.join(stderr_lines)


class Task(ABC):
    """任务抽象基类，定义了所有任务的通用接口"""
    
    def __init__(self, task_id: str, task_type: str, params: Optional[Dict[str, Any]] = None):
        """
        初始化任务
        
        Args:
            task_id: 任务ID
            task_type: 任务类型
            params: 任务参数
        """
        self.task_id = task_id
        self.task_type = task_type
        self.params = params or {}
        
    def set_param(self, key: str, value: Any) -> 'Task':
        """
        设置任务参数
        
        Args:
            key: 参数名
            value: 参数值
            
        Returns:
            任务对象本身，用于链式调用
        """
        self.params[key] = value
        return self
        
    def get_param(self, key: str, default: Any = None) -> Any:
        """
        获取任务参数
        
        Args:
            key: 参数名
            default: 默认值
            
        Returns:
            参数值或默认值
        """
        return self.params.get(key, default)
    
    def resolve_params(self, param_manager: ParamManager) -> None:
        """
        解析任务中的参数引用，使用参数管理器替换变量引用
        
        Args:
            param_manager: 参数管理器
        """
        # 遍历所有参数键值对
        for key, value in list(self.params.items()):
            if isinstance(value, str):
                # 替换字符串中的变量引用
                self.params[key] = param_manager.resolve_value(value)
    
    @abstractmethod
    def execute(self, upstream_results: Dict[str, Any] = None) -> Any:
        """
        执行任务
        
        Args:
            upstream_results: 上游任务的执行结果字典
            
        Returns:
            任务执行结果
        """
        pass


class ShellTask(Task):
    """Shell脚本任务，执行Shell命令或脚本"""
    
    def __init__(self, task_id: str, command: str, params: Optional[Dict[str, Any]] = None,
                working_dir: Optional[str] = None):
        """
        初始化Shell任务
        
        Args:
            task_id: 任务ID
            command: Shell命令或脚本路径
            params: 任务参数
            working_dir: 工作目录
        """
        super().__init__(task_id, "shell", params)
        self.command = command
        self.working_dir = working_dir
        
    def execute(self, upstream_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        执行Shell命令或脚本
        
        Args:
            upstream_results: 上游任务的执行结果
            
        Returns:
            执行结果字典，包含退出码和输出
        """
        # 解析命令中的参数
        resolved_command = self._resolve_command()
        
        # 执行Shell命令
        logger.info(f"执行Shell命令: {resolved_command}")
        try:
            # 创建子进程，设置stdout和stderr为管道
            process = subprocess.Popen(
                resolved_command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                universal_newlines=False,  # 使用二进制模式读取
                bufsize=1
            )
            
            # 实时处理输出
            stdout, stderr = stream_output(process, self.task_id)
            exit_code = process.returncode
            
            if exit_code != 0:
                logger.error(f"Shell命令执行失败: {stderr}")
                raise Exception(f"Shell命令退出码 {exit_code}: {stderr}")
                
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr
            }
        except Exception as e:
            logger.error(f"Shell命令执行异常: {str(e)}")
            raise
    
    def _resolve_command(self) -> str:
        """
        解析命令中的参数引用
        
        Returns:
            解析后的命令
        """
        # 替换命令中的参数引用
        resolved_command = self.command
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, self.command)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_command = resolved_command.replace(match.group(0), str(param_value))
                
        return resolved_command


class PythonTask(Task):
    """Python脚本任务，执行Python脚本"""
    
    def __init__(self, task_id: str, script_path: Optional[str] = None, 
                 python_callable: Optional[callable] = None, script_content: Optional[str] = None,
                 params: Optional[Dict[str, Any]] = None, custom_command: Optional[str] = None,
                 working_dir: Optional[str] = None):
        """
        初始化Python任务
        
        Args:
            task_id: 任务ID
            script_path: Python脚本路径
            python_callable: 可调用的Python函数
            script_content: Python脚本内容
            params: 任务参数
            custom_command: 自定义命令模板，用于支持位置参数
                例如: "python {script_path} {params.day_id} {params.table_list}"
            working_dir: 工作目录
        """
        super().__init__(task_id, "python", params)
        
        # 检查参数，至少提供一种执行方式
        if not any([script_path, python_callable, script_content]):
            raise ValueError("必须提供script_path、python_callable或script_content中的至少一个")
        
        self.script_path = script_path
        self.python_callable = python_callable
        self.script_content = script_content
        self.custom_command = custom_command
        self.working_dir = working_dir
        
    def execute(self, upstream_results: Dict[str, Any] = None) -> Any:
        """
        执行Python脚本或函数
        
        Args:
            upstream_results: 上游任务的执行结果
            
        Returns:
            执行结果
        """
        # 如果提供了可调用函数，直接调用
        if self.python_callable:
            logger.info(f"执行Python函数: {self.python_callable.__name__}")
            try:
                # 将任务参数和上游结果传递给函数
                kwargs = {**self.params}
                if upstream_results:
                    kwargs['upstream_results'] = upstream_results
                    
                return self.python_callable(**kwargs)
            except Exception as e:
                logger.error(f"Python函数执行失败: {str(e)}")
                raise
        
        # 如果提供了自定义命令模板，使用模板构建命令
        elif self.custom_command:
            # 构建自定义命令
            resolved_command = self._resolve_custom_command()
            
            # 执行命令
            logger.info(f"执行自定义Python命令: {resolved_command}")
            try:
                process = subprocess.Popen(
                    resolved_command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.working_dir,
                    universal_newlines=False,  # 使用二进制模式读取
                    bufsize=1
                )
                
                # 实时处理输出
                stdout, stderr = stream_output(process, self.task_id)
                exit_code = process.returncode
                
                if exit_code != 0:
                    logger.error(f"自定义Python命令执行失败: {stderr}")
                    raise Exception(f"自定义Python命令退出码 {exit_code}: {stderr}")
                    
                return {
                    "exit_code": exit_code,
                    "stdout": stdout,
                    "stderr": stderr
                }
            except Exception as e:
                logger.error(f"自定义Python命令执行异常: {str(e)}")
                raise
        
        # 如果提供了脚本路径，使用stream_output函数进行实时输出处理
        elif self.script_path:
            # 解析脚本路径
            # 如果设置了工作目录且脚本路径是相对路径，则基于工作目录解析
            script_path = self.script_path
            if self.working_dir and not os.path.isabs(script_path):
                script_path = os.path.join(self.working_dir, script_path)
            else:
                script_path = os.path.abspath(script_path)
            
            # 构建命令
            command = [
                "python", 
                script_path
            ]
            
            # 添加参数
            for key, value in self.params.items():
                command.append(f"--{key}={value}")
                
            # 执行命令
            logger.info(f"执行Python脚本: {' '.join(command)}")
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.working_dir,
                    universal_newlines=False,  # 使用二进制模式读取
                    bufsize=1
                )
                
                # 实时处理输出
                stdout, stderr = stream_output(process, self.task_id)
                exit_code = process.returncode
                
                if exit_code != 0:
                    logger.error(f"Python脚本执行失败: {stderr}")
                    raise Exception(f"Python脚本退出码 {exit_code}: {stderr}")
                    
                return {
                    "exit_code": exit_code,
                    "stdout": stdout,
                    "stderr": stderr
                }
            except Exception as e:
                logger.error(f"Python脚本执行异常: {str(e)}")
                raise
                
        # 如果提供了脚本内容，创建临时文件执行
        elif self.script_content:
            # 替换脚本内容中的参数引用
            resolved_script = self._resolve_script_content()
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as temp_file:
                temp_file_path = temp_file.name
                temp_file.write(resolved_script.encode('utf-8'))
                
            try:
                # 构建命令
                command = ["python", temp_file_path]
                
                # 添加参数
                for key, value in self.params.items():
                    command.append(f"--{key}={value}")
                    
                # 执行命令
                logger.info(f"执行临时Python脚本: {' '.join(command)}")
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.working_dir,
                    universal_newlines=False,  # 使用二进制模式读取
                    bufsize=1
                )
                
                # 实时处理输出
                stdout, stderr = stream_output(process, self.task_id)
                exit_code = process.returncode
                
                if exit_code != 0:
                    logger.error(f"临时Python脚本执行失败: {stderr}")
                    raise Exception(f"临时Python脚本退出码 {exit_code}: {stderr}")
                    
                return {
                    "exit_code": exit_code,
                    "stdout": stdout,
                    "stderr": stderr
                }
            except Exception as e:
                logger.error(f"临时Python脚本执行异常: {str(e)}")
                raise
            finally:
                # 删除临时文件
                os.unlink(temp_file_path)
    
    def _resolve_script_content(self) -> str:
        """
        解析脚本内容中的参数引用
        
        Returns:
            解析后的脚本内容
        """
        if not self.script_content:
            return ""
            
        # 替换脚本内容中的参数引用
        resolved_script = self.script_content
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, self.script_content)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_script = resolved_script.replace(match.group(0), str(param_value))
                
        return resolved_script
        
    def _resolve_custom_command(self) -> str:
        """
        解析自定义命令模板中的参数引用
        
        Returns:
            解析后的命令
        """
        if not self.custom_command:
            return ""
            
        # 创建格式化参数字典
        # 解析脚本路径，如果设置了工作目录且路径是相对路径，则基于工作目录解析
        script_path = ''
        if self.script_path:
            if self.working_dir and not os.path.isabs(self.script_path):
                script_path = os.path.join(self.working_dir, self.script_path)
            else:
                script_path = os.path.abspath(self.script_path)
                
        format_args = {
            'script_path': script_path
        }
        
        # 添加params.PARAM_NAME格式的参数
        params_dict = {}
        for key, value in self.params.items():
            params_dict[key] = value
            
        format_args['params'] = type('ParamsNamespace', (), params_dict)
        
        # 格式化命令
        try:
            resolved_command = self.custom_command.format(**format_args)
            return resolved_command
        except KeyError as e:
            logger.error(f"自定义命令模板中存在未知参数: {e}")
            raise ValueError(f"自定义命令模板中存在未知参数: {e}")
        except Exception as e:
            logger.error(f"解析自定义命令模板失败: {e}")
            raise ValueError(f"解析自定义命令模板失败: {e}")


class PySparkTask(Task):
    """PySpark任务，执行PySpark脚本"""
    
    def __init__(self, task_id: str, script_path: Optional[str] = None, 
                 script_content: Optional[str] = None, params: Optional[Dict[str, Any]] = None,
                 spark_config: Optional[Dict[str, str]] = None, working_dir: Optional[str] = None):
        """
        初始化PySpark任务
        
        Args:
            task_id: 任务ID
            script_path: PySpark脚本路径
            script_content: PySpark脚本内容
            params: 任务参数
            spark_config: Spark配置参数
            working_dir: 工作目录
        """
        super().__init__(task_id, "pyspark", params)
        
        # 检查参数，必须提供script_path或script_content中的一个
        if not script_path and not script_content:
            raise ValueError("必须提供script_path或script_content")
            
        self.script_path = script_path
        self.script_content = script_content
        self.spark_config = spark_config or {}
        self.working_dir = working_dir
        
    def execute(self, upstream_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        执行PySpark脚本
        
        Args:
            upstream_results: 上游任务的执行结果
            
        Returns:
            执行结果
        """
        # 如果提供了脚本内容，创建临时文件
        if self.script_content:
            # 替换脚本内容中的参数引用
            resolved_script = self._resolve_script_content()
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile(suffix='.py', delete=False) as temp_file:
                script_path = temp_file.name
                temp_file.write(resolved_script.encode('utf-8'))
        else:
            # 解析脚本路径
            script_path = self.script_path
            if self.working_dir and not os.path.isabs(script_path):
                script_path = os.path.join(self.working_dir, script_path)
            else:
                script_path = os.path.abspath(script_path)
                
        try:
            # 构建spark-submit命令
            command = ["spark-submit"]
            
            # 处理spark_config中的参数引用
            resolved_spark_config = {}
            for key, value in self.spark_config.items():
                if isinstance(value, str):
                    # 替换字符串中的参数引用
                    resolved_value = self._resolve_value(value)
                    resolved_spark_config[key] = resolved_value
                else:
                    resolved_spark_config[key] = value
            
            # 添加Spark配置
            for key, value in resolved_spark_config.items():
                command.append("--conf")
                command.append(f"{key}={value}")
                
            # 添加脚本路径
            command.append(script_path)
            
            # 添加任务参数
            for key, value in self.params.items():
                command.append(f"--{key}={value}")
                
            # 执行命令
            cmd_str = ' '.join(command)
            logger.info(f"执行PySpark命令: {cmd_str}")
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                universal_newlines=False,  # 使用二进制模式读取
                bufsize=1
            )
            
            # 实时处理输出
            stdout, stderr = stream_output(process, self.task_id)
            exit_code = process.returncode
            
            if exit_code != 0:
                logger.error(f"PySpark脚本执行失败: {stderr}")
                raise Exception(f"PySpark脚本退出码 {exit_code}: {stderr}")
                
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr
            }
        except Exception as e:
            logger.error(f"PySpark脚本执行异常: {str(e)}")
            raise
        finally:
            # 如果使用了临时文件，删除它
            if self.script_content and 'script_path' in locals():
                try:
                    os.unlink(script_path)
                except Exception:
                    pass
    
    def _resolve_value(self, value: str) -> str:
        """
        解析字符串中的参数引用
        
        Args:
            value: 包含参数引用的字符串
            
        Returns:
            解析后的字符串
        """
        if not value or not isinstance(value, str):
            return value
            
        # 替换字符串中的参数引用
        resolved_value = value
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, value)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_value = resolved_value.replace(match.group(0), str(param_value))
                
        return resolved_value
    
    def _resolve_script_content(self) -> str:
        """
        解析脚本内容中的参数引用
        
        Returns:
            解析后的脚本内容
        """
        if not self.script_content:
            return ""
            
        # 替换脚本内容中的参数引用
        resolved_script = self.script_content
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, self.script_content)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_script = resolved_script.replace(match.group(0), str(param_value))
                
        return resolved_script


class SparkSQLTask(Task):
    """Spark SQL任务，执行Spark SQL查询"""
    
    def __init__(self, task_id: str, sql: Optional[str] = None, sql_file: Optional[str] = None,
                params: Optional[Dict[str, Any]] = None, 
                spark_config: Optional[Dict[str, str]] = None, working_dir: Optional[str] = None,
                init_script: Optional[Union[str, List[str]]] = None):
        """
        初始化Spark SQL任务
        
        Args:
            task_id: 任务ID
            sql: SQL查询语句
            sql_file: SQL文件路径
            params: 任务参数
            spark_config: Spark配置参数
            working_dir: 工作目录
            init_script: 初始化脚本路径，通过-i选项加载，可以是单个脚本路径或多个脚本路径的列表
        """
        super().__init__(task_id, "spark-sql", params)
        
        # 检查参数，必须提供sql或sql_file中的一个
        if not sql and not sql_file:
            raise ValueError("必须提供sql或sql_file")
            
        self.sql = sql
        self.sql_file = sql_file
        self.spark_config = spark_config or {}
        self.working_dir = working_dir
        self.init_script = init_script
        
    def execute(self, upstream_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        执行Spark SQL查询
        
        Args:
            upstream_results: 上游任务的执行结果
            
        Returns:
            执行结果
        """
        # 如果提供了SQL语句，创建临时文件
        if self.sql:
            # 替换SQL中的参数引用
            resolved_sql = self._resolve_sql()
            logger.info(f"执行SQL查询: {resolved_sql}")
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as temp_file:
                sql_file = temp_file.name
                temp_file.write(resolved_sql.encode('utf-8'))
                
        else:
            # 解析SQL文件路径
            sql_file_path = self.sql_file
            if self.working_dir and not os.path.isabs(sql_file_path):
                sql_file_path = os.path.join(self.working_dir, sql_file_path)
            else:
                sql_file_path = os.path.abspath(sql_file_path)
                
            # 如果提供了SQL文件，读取内容并解析参数
            try:
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    
                # 替换SQL文件中的参数引用
                resolved_sql = self._resolve_sql_content(sql_content)
                
                # 重新写入临时文件
                with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as temp_file:
                    sql_file = temp_file.name
                    temp_file.write(resolved_sql.encode('utf-8'))
            except Exception as e:
                logger.error(f"读取SQL文件失败: {str(e)}")
                raise
                
        try:
            # 构建spark-sql命令
            command = ["spark-sql"]
            
            # 如果提供了初始化脚本，添加-i选项
            if self.init_script:
                # 将单个脚本转换为列表，方便统一处理
                init_scripts = self.init_script if isinstance(self.init_script, list) else [self.init_script]
                
                for script in init_scripts:
                    init_script_path = script
                    if self.working_dir and not os.path.isabs(init_script_path):
                        init_script_path = os.path.join(self.working_dir, init_script_path)
                    else:
                        init_script_path = os.path.abspath(init_script_path)
                    
                    # 确认初始化脚本文件存在
                    if not os.path.exists(init_script_path):
                        raise ValueError(f"初始化脚本文件不存在: {init_script_path}")
                    
                    command.extend(["-i", init_script_path])
                
                # 启用变量替换
                if "spark.sql.variable.substitution" not in self.spark_config:
                    self.spark_config["spark.sql.variable.substitution"] = "true"
            
            # 处理spark_config中的参数引用
            resolved_spark_config = {}
            for key, value in self.spark_config.items():
                if isinstance(value, str):
                    # 替换字符串中的参数引用
                    resolved_value = self._resolve_value(value)
                    resolved_spark_config[key] = resolved_value
                else:
                    resolved_spark_config[key] = value
            
            # 添加Spark配置
            for key, value in resolved_spark_config.items():
                command.append("--conf")
                command.append(f"{key}={value}")
                
            # 添加SQL文件
            command.extend(["-f", sql_file])
            
            # 添加任务参数
            for key, value in self.params.items():
                command.append(f"--hivevar")
                command.append(f"{key}={value}")
                
            # 执行命令
            cmd_str = ' '.join(command)
            logger.info(f"执行Spark SQL命令: {cmd_str}")
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                universal_newlines=False,  # 使用二进制模式读取
                bufsize=1
            )
            
            # 实时处理输出
            stdout, stderr = stream_output(process, self.task_id)
            exit_code = process.returncode
            
            if exit_code != 0:
                logger.error(f"Spark SQL执行失败: {stderr}")
                raise Exception(f"Spark SQL退出码 {exit_code}: {stderr}")
                
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr
            }
        except Exception as e:
            logger.error(f"Spark SQL执行异常: {str(e)}")
            raise
        finally:
            # 如果使用了临时文件，删除它
            if 'sql_file' in locals() and (self.sql or sql_file != self.sql_file):
                try:
                    os.unlink(sql_file)
                except Exception:
                    pass
    
    def _resolve_value(self, value: str) -> str:
        """
        解析字符串中的参数引用
        
        Args:
            value: 包含参数引用的字符串
            
        Returns:
            解析后的字符串
        """
        if not value or not isinstance(value, str):
            return value
            
        # 替换字符串中的参数引用
        resolved_value = value
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, value)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_value = resolved_value.replace(match.group(0), str(param_value))
                
        return resolved_value
    
    def _resolve_sql(self) -> str:
        """
        解析SQL中的参数引用
        
        Returns:
            解析后的SQL
        """
        return self._resolve_sql_content(self.sql)
        
    def _resolve_sql_content(self, sql_content: str) -> str:
        """
        解析SQL内容中的参数引用
        
        Args:
            sql_content: SQL内容
            
        Returns:
            解析后的SQL内容
        """
        if not sql_content:
            return ""
            
        # 替换SQL内容中的参数引用
        resolved_sql = sql_content
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, sql_content)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_sql = resolved_sql.replace(match.group(0), str(param_value))
                
        return resolved_sql


class HiveSQLTask(Task):
    """Hive SQL任务，执行Hive SQL查询"""
    
    def __init__(self, task_id: str, sql: Optional[str] = None, sql_file: Optional[str] = None,
                params: Optional[Dict[str, Any]] = None, 
                hive_config: Optional[Dict[str, str]] = None, working_dir: Optional[str] = None,
                init_script: Optional[Union[str, List[str]]] = None):
        """
        初始化Hive SQL任务
        
        Args:
            task_id: 任务ID
            sql: SQL查询语句
            sql_file: SQL文件路径
            params: 任务参数
            hive_config: Hive配置参数
            working_dir: 工作目录
            init_script: 初始化脚本路径，通过-i选项加载，可以是单个脚本路径或多个脚本路径的列表
        """
        super().__init__(task_id, "hive", params)
        
        # 检查参数，必须提供sql或sql_file中的一个
        if not sql and not sql_file:
            raise ValueError("必须提供sql或sql_file")
            
        self.sql = sql
        self.sql_file = sql_file
        self.hive_config = hive_config or {}
        self.working_dir = working_dir
        self.init_script = init_script
        
    def execute(self, upstream_results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        执行Hive SQL查询
        
        Args:
            upstream_results: 上游任务的执行结果
            
        Returns:
            执行结果
        """
        # 如果提供了SQL语句，创建临时文件
        if self.sql:
            # 替换SQL中的参数引用
            resolved_sql = self._resolve_sql()
            logger.info(f"执行SQL查询: {resolved_sql}")
            
            # 创建临时文件
            with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as temp_file:
                sql_file = temp_file.name
                temp_file.write(resolved_sql.encode('utf-8'))
                
        else:
            # 解析SQL文件路径
            sql_file_path = self.sql_file
            if self.working_dir and not os.path.isabs(sql_file_path):
                sql_file_path = os.path.join(self.working_dir, sql_file_path)
            else:
                sql_file_path = os.path.abspath(sql_file_path)
                
            # 如果提供了SQL文件，读取内容并解析参数
            try:
                with open(sql_file_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                    
                # 替换SQL文件中的参数引用
                resolved_sql = self._resolve_sql_content(sql_content)
                
                # 重新写入临时文件
                with tempfile.NamedTemporaryFile(suffix='.sql', delete=False) as temp_file:
                    sql_file = temp_file.name
                    temp_file.write(resolved_sql.encode('utf-8'))
            except Exception as e:
                logger.error(f"读取SQL文件失败: {str(e)}")
                raise
                
        try:
            # 构建hive命令
            command = ["hive"]
            
            # 如果提供了初始化脚本，添加-i选项
            if self.init_script:
                # 将单个脚本转换为列表，方便统一处理
                init_scripts = self.init_script if isinstance(self.init_script, list) else [self.init_script]
                
                for script in init_scripts:
                    init_script_path = script
                    if self.working_dir and not os.path.isabs(init_script_path):
                        init_script_path = os.path.join(self.working_dir, init_script_path)
                    else:
                        init_script_path = os.path.abspath(init_script_path)
                    
                    # 确认初始化脚本文件存在
                    if not os.path.exists(init_script_path):
                        raise ValueError(f"初始化脚本文件不存在: {init_script_path}")
                    
                    command.extend(["-i", init_script_path])
            
            # 处理hive_config中的参数引用
            resolved_hive_config = {}
            for key, value in self.hive_config.items():
                if isinstance(value, str):
                    # 替换字符串中的参数引用
                    resolved_value = self._resolve_value(value)
                    resolved_hive_config[key] = resolved_value
                else:
                    resolved_hive_config[key] = value
            
            # 添加Hive配置
            for key, value in resolved_hive_config.items():
                command.append(f"--hiveconf")
                command.append(f"{key}={value}")
                
            # 添加SQL文件
            command.extend(["-f", sql_file])
            
            # 添加任务参数
            for key, value in self.params.items():
                command.append(f"--hivevar")
                command.append(f"{key}={value}")
                
            # 执行命令
            cmd_str = ' '.join(command)
            logger.info(f"执行Hive SQL命令: {cmd_str}")
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.working_dir,
                universal_newlines=False,  # 使用二进制模式读取
                bufsize=1
            )
            
            # 实时处理输出
            stdout, stderr = stream_output(process, self.task_id)
            exit_code = process.returncode
            
            if exit_code != 0:
                logger.error(f"Hive SQL执行失败: {stderr}")
                raise Exception(f"Hive SQL退出码 {exit_code}: {stderr}")
                
            return {
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr
            }
        except Exception as e:
            logger.error(f"Hive SQL执行异常: {str(e)}")
            raise
        finally:
            # 如果使用了临时文件，删除它
            if 'sql_file' in locals() and (self.sql or sql_file != self.sql_file):
                try:
                    os.unlink(sql_file)
                except Exception:
                    pass
    
    def _resolve_value(self, value: str) -> str:
        """
        解析字符串中的参数引用
        
        Args:
            value: 包含参数引用的字符串
            
        Returns:
            解析后的字符串
        """
        if not value or not isinstance(value, str):
            return value
            
        # 替换字符串中的参数引用
        resolved_value = value
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, value)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_value = resolved_value.replace(match.group(0), str(param_value))
                
        return resolved_value
    
    def _resolve_sql(self) -> str:
        """
        解析SQL中的参数引用
        
        Returns:
            解析后的SQL
        """
        return self._resolve_sql_content(self.sql)
        
    def _resolve_sql_content(self, sql_content: str) -> str:
        """
        解析SQL内容中的参数引用
        
        Args:
            sql_content: SQL内容
            
        Returns:
            解析后的SQL内容
        """
        if not sql_content:
            return ""
            
        # 替换SQL内容中的参数引用
        resolved_sql = sql_content
        
        # 使用正则表达式查找所有${param}格式的参数
        pattern = r'\${([^}]+)}'
        matches = re.finditer(pattern, sql_content)
        
        for match in matches:
            param_name = match.group(1)
            if param_name in self.params:
                param_value = self.params[param_name]
                resolved_sql = resolved_sql.replace(match.group(0), str(param_value))
                
        return resolved_sql 