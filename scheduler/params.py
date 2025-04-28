#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : params.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：参数管理器实现，用于处理工作流和任务的参数传递和替换

输入：参数键值对

输出：处理后的参数值或替换后的字符串

版本历史：
- v1.0 (2024-07-11): 初始版本
"""

import re
import datetime
from typing import Dict, Any, Optional, Union


class ParamManager:
    """参数管理器，处理参数传递和替换"""
    
    def __init__(self):
        """初始化参数管理器"""
        self.params = {}
        
    def set_params(self, params: Dict[str, Any]) -> None:
        """
        设置参数字典
        
        Args:
            params: 参数字典
        """
        self.params.update(params)
        
    def get_param(self, key: str, default: Any = None) -> Any:
        """
        获取参数值
        
        Args:
            key: 参数名
            default: 默认值，如果参数不存在则返回此值
            
        Returns:
            参数值或默认值
        """
        return self.params.get(key, default)
    
    def resolve_value(self, value: str) -> str:
        """
        解析参数值中的变量引用并替换，支持${param}格式
        
        Args:
            value: 需要解析的字符串
            
        Returns:
            解析后的字符串
        """
        if not isinstance(value, str):
            return value
            
        # 使用正则表达式找出所有的${param}格式的变量引用
        pattern = r'\${([^}]+)}'
        
        def replace_var(match):
            param_name = match.group(1)
            
            # 处理日期格式参数，如${yyyyMMdd-1}或${yyyy-MM-dd+7}等
            date_pattern = r'([a-zA-Z\-]+)([\+\-])(\d+)'
            date_match = re.match(date_pattern, param_name)
            
            if date_match:
                # 解析日期格式参数
                date_format = date_match.group(1)
                operation = date_match.group(2)
                days = int(date_match.group(3))
                
                # 根据操作符调整天数
                if operation == '+':
                    delta_days = days
                else:  # operation == '-'
                    delta_days = -days
                    
                # 计算日期
                today = datetime.datetime.now()
                target_date = today + datetime.timedelta(days=delta_days)
                
                # 将格式字符串转换为Python的datetime格式
                py_format = self._convert_to_python_date_format(date_format)
                return target_date.strftime(py_format)
            
            # 常规参数查找
            if param_name in self.params:
                param_value = self.params[param_name]
                # 如果参数值是字符串，递归解析
                if isinstance(param_value, str):
                    return self.resolve_value(param_value)
                return str(param_value)
            else:
                # 如果参数不存在，保持原样
                return match.group(0)
                
        # 替换所有变量引用
        return re.sub(pattern, replace_var, value)
    
    def _convert_to_python_date_format(self, format_str: str) -> str:
        """
        将自定义日期格式转换为Python的datetime格式
        
        Args:
            format_str: 自定义日期格式
            
        Returns:
            Python日期格式字符串
        """
        # 创建映射字典
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


# 为了兼容性，创建ParamsManager作为ParamManager的别名
ParamsManager = ParamManager 