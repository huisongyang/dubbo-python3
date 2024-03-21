# -*- coding: utf-8 -*-
import json
import threading

from dubbo.client import DubboClient
from dubbo.client import ZkRegister
from dubbo.codec.encoder import Object
import logging

logger = logging.getLogger('python-dubbo')
logging.basicConfig(level=logging.INFO)


def pretty_print(value):
    print(json.dumps(value, ensure_ascii=False, indent=4, sort_keys=True))


zk_client = ZkRegister('')

client = DubboClient('', zk_register=zk_client)

java_obj = Object('')
java_obj[''] = ''
java_obj[''] = ''

for i in range(100):
    result = client.call('getMemberInfo', java_obj)
    pretty_print(result)

# threading.Event().wait()
