#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2025-04-24
# @Author  : yanhui.ning
# @File    : setup.py
# @Project : scheduler_dag
# @Description: 项目安装配置文件

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="scheduler_dag",
    version="1.0.0",
    author="yanhui.ning",
    author_email="your.email@example.com",
    description="基于DAG的任务调度工具",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/scheduler_dag",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "graphviz>=0.16.0",
        "requests>=2.25.0",
    ],
) 