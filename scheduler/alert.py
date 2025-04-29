#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : alert.py
# @Project : scheduler_dag
# @Description: 
"""
功能描述：告警模块，用于发送工作流任务失败告警

输入：告警信息

输出：告警发送结果

版本历史：
- v1.0 (2024-07-11): 初始版本
- v1.1 (2024-07-11): 修复飞书告警标题显示问题
- v1.2 (2024-07-11): 修复飞书Markdown二级标题显示问题
- v1.3 (2024-07-29): 增加对未执行任务列表的告警显示
- v1.4 (2024-08-01): 增加工作流成功的告警功能
- v1.5 (2024-08-03): 增加回溯日期在告警中的显示
"""

import json
import time
import logging
import traceback
import requests
from typing import Dict, Any, List, Optional, Union, Set

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("alert")


class AlertManager:
    """告警管理器，负责发送各种告警"""
    
    def __init__(self):
        """初始化告警管理器"""
        self.enabled = False
        self.webhook_url = None
        self.alert_config = {}
        
    def enable_feishu_alert(self, webhook_url: str, **kwargs) -> None:
        """
        启用飞书告警
        
        Args:
            webhook_url: 飞书机器人webhook地址
            **kwargs: 其他配置项
        """
        self.enabled = True
        self.webhook_url = webhook_url
        self.alert_config.update(kwargs)
        logger.info("飞书告警已启用")
        
    def disable_alert(self) -> None:
        """禁用告警"""
        self.enabled = False
        logger.info("告警已禁用")
        
    def send_workflow_failed_alert(
        self, 
        workflow_name: str, 
        start_time: float, 
        failed_task_id: str, 
        failed_reason: str, 
        completed_tasks: List[str],
        uncompleted_tasks: Optional[List[str]] = None,
        backfill_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        发送工作流失败告警
        
        Args:
            workflow_name: 工作流名称
            start_time: 工作流开始时间戳
            failed_task_id: 失败任务ID
            failed_reason: 失败原因
            completed_tasks: 已完成的任务列表
            uncompleted_tasks: 未执行的任务列表
            backfill_date: 回溯日期，如果是回溯任务则提供该参数
            
        Returns:
            告警发送结果
        """
        if not self.enabled or not self.webhook_url:
            logger.warning("告警未启用或未配置webhook_url，无法发送告警")
            return {"status": "disabled", "message": "告警未启用或未配置webhook URL"}
            
        # 格式化开始时间
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
        
        # 准备飞书消息
        title = f"⚠️ 工作流失败告警: {workflow_name}"
        if backfill_date:
            title += f" (回溯日期: {backfill_date})"
        
        # 构建markdown消息内容
        # 注意：飞书Markdown格式要求，需要使用**粗体**来表示标题
        markdown_content = f"""
**工作流名称**: {workflow_name}
**开始时间**: {start_time_str}
**执行时长**: {time.time() - start_time:.2f}秒
"""

        # 添加回溯信息
        if backfill_date:
            markdown_content += f"**回溯日期**: {backfill_date}\n"

        markdown_content += f"""
**失败信息**
- **失败任务**: {failed_task_id}
- **失败原因**: {failed_reason}

**执行情况**
- **已完成任务**: {', '.join(completed_tasks) if completed_tasks else '无'}
"""

        # 添加未执行任务列表信息
        if uncompleted_tasks and len(uncompleted_tasks) > 0:
            markdown_content += f"- **未执行任务**: {', '.join(uncompleted_tasks)}\n  原因: 由于上游任务失败，这些任务未被执行"
        else:
            markdown_content += "- **未执行任务**: 无"
        
        return self._send_feishu_alert(title, markdown_content)
    
    def send_workflow_success_alert(
        self, 
        workflow_name: str, 
        start_time: float, 
        completed_tasks: List[str],
        backfill_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        发送工作流成功告警
        
        Args:
            workflow_name: 工作流名称
            start_time: 工作流开始时间戳
            completed_tasks: 已完成的任务列表
            backfill_date: 回溯日期，如果是回溯任务则提供该参数
            
        Returns:
            告警发送结果
        """
        if not self.enabled or not self.webhook_url:
            logger.warning("告警未启用或未配置webhook_url，无法发送告警")
            return {"status": "disabled", "message": "告警未启用或未配置webhook URL"}
            
        # 格式化开始时间
        start_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
        
        # 准备飞书消息
        title = f"✅ 工作流执行成功: {workflow_name}"
        if backfill_date:
            title += f" (回溯日期: {backfill_date})"
        
        # 构建markdown消息内容
        markdown_content = f"""
**工作流名称**: {workflow_name}
**开始时间**: {start_time_str}
**执行时长**: {time.time() - start_time:.2f}秒
"""

        # 添加回溯信息
        if backfill_date:
            markdown_content += f"**回溯日期**: {backfill_date}\n"

        markdown_content += f"""
**执行状态**: 成功

**执行情况**
- **已完成任务**: {', '.join(completed_tasks) if completed_tasks else '无'}
"""
        
        return self._send_feishu_alert(title, markdown_content, "green")
    
    def _send_feishu_alert(self, title: str, content: str, template: str = "red") -> Dict[str, Any]:
        """
        发送飞书告警
        
        Args:
            title: 告警标题
            content: 告警内容(markdown格式)
            template: 消息模板颜色，默认为red（红色），可选值：red, orange, yellow, green, blue, purple
            
        Returns:
            告警发送结果
        """
        try:
            # 飞书消息格式
            message = {
                "msg_type": "interactive",
                "card": {
                    "config": {
                        "wide_screen_mode": True
                    },
                    "header": {
                        "title": {
                            "tag": "plain_text",
                            "content": title
                        },
                        "template": template
                    },
                    "elements": [
                        {
                            "tag": "markdown",
                            "content": content
                        }
                    ]
                }
            }
            
            # 额外添加at功能
            if self.alert_config.get("at_all"):
                message["card"]["elements"].append({
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "<at id=all></at>"
                    }
                })
                
            # 发送请求
            response = requests.post(
                self.webhook_url, 
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"成功发送飞书告警: {title}")
                return {"status": "success", "response": response.json()}
            else:
                logger.error(f"发送飞书告警失败: {response.status_code} - {response.text}")
                return {"status": "failed", "code": response.status_code, "message": response.text}
                
        except Exception as e:
            error_msg = f"发送飞书告警异常: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {"status": "error", "message": error_msg}


# 全局告警管理器实例
alert_manager = AlertManager() 