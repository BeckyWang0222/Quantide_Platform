#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线数据定时获取系统

从Tushare获取日线数据，通过Redis发布到消息队列，最终存入ClickHouse数据库。
"""

__version__ = '1.0.0'
__author__ = 'quantide'
