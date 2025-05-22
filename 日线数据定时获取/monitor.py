#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
监控报警模块

提供系统监控和报警功能。
"""

import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any, Union, Optional

from config_loader import MONITOR_CONFIG
from logger import logger
from exceptions import MonitorError


class Monitor:
    """监控报警类，提供系统监控和报警功能"""

    def __init__(self, config: Dict = None):
        """
        初始化监控报警类

        Args:
            config (Dict, optional): 监控配置. 默认为None，使用配置文件中的值.
        """
        self.config = config or MONITOR_CONFIG
        self.enabled = self.config.enabled
        self.email_config = self.config.email
        self.email_enabled = self.email_config.enabled

    def send_alert(self, subject: str, message: str, level: str = 'error') -> bool:
        """
        发送报警

        Args:
            subject (str): 报警主题
            message (str): 报警内容
            level (str, optional): 报警级别. 默认为'error'.

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        if not self.enabled:
            logger.info(f"监控报警已禁用，不发送报警: {subject}")
            return False

        logger.info(f"发送报警: {subject}")

        # 记录报警信息
        log_method = getattr(logger, level.lower(), logger.error)
        log_method(f"报警: {subject} - {message}")

        # 发送邮件报警
        if self.email_enabled:
            return self._send_email_alert(subject, message)

        return True

    def _send_email_alert(self, subject: str, message: str) -> bool:
        """
        发送邮件报警

        Args:
            subject (str): 邮件主题
            message (str): 邮件内容

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        try:
            # 获取邮件配置
            smtp_server = self.email_config.smtp_server
            smtp_port = self.email_config.smtp_port
            sender = self.email_config.sender
            password = self.email_config.password
            recipients = self.email_config.recipients

            if not smtp_server or not sender or not password or not recipients:
                logger.error("邮件配置不完整，无法发送邮件报警")
                return False

            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"[日线数据获取系统报警] {subject}"

            # 添加时间戳
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            body = f"时间: {now}\n\n{message}"

            msg.attach(MIMEText(body, 'plain'))

            # 发送邮件
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender, password)
                server.send_message(msg)

            logger.info(f"邮件报警发送成功: {subject}")
            return True

        except Exception as e:
            logger.error(f"发送邮件报警失败: {e}")
            return False

    def alert_scheduler_failure(self, job_id: str, exception: Exception) -> bool:
        """
        报警调度器执行失败

        Args:
            job_id (str): 作业ID
            exception (Exception): 异常信息

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        subject = f"调度器执行失败: {job_id}"
        message = f"作业 {job_id} 执行失败，异常信息: {str(exception)}"
        return self.send_alert(subject, message, 'error')

    def alert_data_fetch_failure(self, data_type: str, exception: Exception) -> bool:
        """
        报警数据获取失败

        Args:
            data_type (str): 数据类型
            exception (Exception): 异常信息

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        subject = f"数据获取失败: {data_type}"
        message = f"获取 {data_type} 数据失败，异常信息: {str(exception)}"
        return self.send_alert(subject, message, 'error')

    def alert_data_process_failure(self, data_type: str, exception: Exception) -> bool:
        """
        报警数据处理失败

        Args:
            data_type (str): 数据类型
            exception (Exception): 异常信息

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        subject = f"数据处理失败: {data_type}"
        message = f"处理 {data_type} 数据失败，异常信息: {str(exception)}"
        return self.send_alert(subject, message, 'error')

    def alert_data_store_failure(self, data_type: str, exception: Exception) -> bool:
        """
        报警数据存储失败

        Args:
            data_type (str): 数据类型
            exception (Exception): 异常信息

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        subject = f"数据存储失败: {data_type}"
        message = f"存储 {data_type} 数据失败，异常信息: {str(exception)}"
        return self.send_alert(subject, message, 'error')

    def alert_connection_failure(self, connection_type: str, exception: Exception) -> bool:
        """
        报警连接检查失败

        Args:
            connection_type (str): 连接类型
            exception (Exception): 异常信息

        Returns:
            bool: 发送结果，True表示成功，False表示失败
        """
        subject = f"连接检查失败: {connection_type}"
        message = f"检查 {connection_type} 连接失败，异常信息: {str(exception)}"
        return self.send_alert(subject, message, 'error')


# 创建默认监控报警实例
monitor = Monitor()
