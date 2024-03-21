# -*- coding: utf-8 -*-
import logging
import os
import struct
import threading
import socket
from sys import maxsize
from urllib.parse import urlparse, unquote, parse_qsl

ip = None
heartbeat_id = 0
invoke_id = 0
invoke_id_lock = threading.Lock()

logger = logging.getLogger('python-dubbo')


def get_invoke_id():
    """
    获取 dubbo 的调用 id
    :return:
    """
    global invoke_id
    with invoke_id_lock:
        result = invoke_id
        invoke_id += 1
        if invoke_id == maxsize:
            invoke_id = 0
    return result


def double_to_long_bits(value):
    """
    https://gist.github.com/carlozamagni/187e478f516cac926682
    :param value:
    :return:
    """
    return struct.unpack('Q', struct.pack('d', value))[0]


def num_2_byte_list(num):
    """
    convert num to byte list
    :param num:
    :return:
    """
    byte = []
    while num > 0:
        b = num & 0xff  # 获取最低位的一个字节的值
        byte.append(b)
        num = num >> 8  # 移除最低位的一个字节
    return list(reversed(byte))


def parse_url(url_str):
    """
    把url字符串解析为适合于操作的对象
    :param url_str:
    :return:
    """
    url = urlparse(unquote(url_str))
    fields = dict(parse_qsl(url.query))
    result = {
        'scheme': url.scheme,
        'host': url.netloc,
        'hostname': url.hostname,
        'port': url.port,
        'path': url.path,
        'fields': fields
    }
    return result


def get_pid():
    return os.getpid()


# 数据的头部大小为16个字节
# 读取的数据类型：1 head; 2 error_body; 3 common_body;
# 头部信息不存在invoke_id，所以为None
DEFAULT_READ_PARAMS = 16, 1, None


def get_ip():
    global ip
    if ip:
        return ip
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(('8.8.8.8', 80))
        ip = sock.getsockname()[0]
        logger.debug('Current IP Address: {}'.format(ip))
    finally:
        sock.close()
    return ip
