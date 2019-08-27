#coding=utf-8
"""
系统路由设置
"""
from respond_handler import RespDomainResultHandler

urls = [
    (r'/file_/([\w-]+)', RespDomainResultHandler),  # 全量任务完成响应
    (r'/update_/([\w-]+)', RespDomainResultHandler),  # 全量任务完成响应

]
