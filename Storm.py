# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import sys
import traceback
from collections import deque
from typing import Dict
from typing import List

from overrides import overrides

import utility

"""
https://github.com/apache/storm/blob/master/storm-multilang/python/src/main/resources/resources/storm.py

:author liangziqian
:date 2020-04-27

"""


class Tuple(object):
    def __init__(self, id, component, stream, task, values):
        self.id = id
        self.component = component
        self.stream = stream
        self.task = task
        self.values = values

    def __repr__(self):
        return '<%s%s>' % (
            self.__class__.__name__,
            ''.join(' %s=%r' % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys())))

    def is_heartbeat_tuple(self):
        return self.task == -1 and self.stream == "__heartbeat"


class Component(object):
    # queue up commands we read while trying to read taskids
    _pending_commands = deque()
    # queue up task ids we read while trying to read commands/tuples
    _pending_task_ids = deque()

    # reads lines and reconstructs newlines appropriately
    # (readMsg)
    @classmethod
    def _read_message(cls):
        msg = ""
        while True:
            line = sys.stdin.readline()
            if not line:
                raise Exception('Read EOF from stdin')
            if line[0:-1] == "end":
                break
            msg = msg + line
        return json.loads(msg[0:-1])

    # readTaskIds
    @classmethod
    def _read_task_ids(cls):
        if cls._pending_task_ids:
            return cls._pending_task_ids.popleft()
        else:
            msg = cls._read_message()
            while type(msg) is not list:
                cls._pending_commands.append(msg)
                msg = cls._read_message()
            return msg

    # readCommand
    @classmethod
    def _read_command(cls):
        if cls._pending_commands:
            return cls._pending_commands.popleft()
        else:
            msg = cls._read_message()
            while type(msg) is list:
                cls._pending_task_ids.append(msg)
                msg = cls._read_message()
            return msg

    # readTuple
    @classmethod
    def _read_tuple(cls):
        cmd = cls._read_command()
        return Tuple(cmd["id"], cmd["comp"], cmd["stream"], cmd["task"], cmd["tuple"])

    # sendMsgToParent
    @classmethod
    def _send_message_to_parent(cls, msg):
        sys.__stdout__.write('【start】' + json.dumps(msg) + '\n【end】\n')
        sys.__stdout__.flush()

    @classmethod
    def _sync(cls):
        cls._send_message_to_parent({'command': 'sync'})

    @classmethod
    def _send_pid(cls, heartbeat_dir):
        pid = os.getpid()
        cls._send_message_to_parent({'pid': pid})
        open(heartbeat_dir + "/" + str(pid), "w").close()

    @classmethod
    def _emit_component(cls, *args, **kwargs):
        return NotImplemented

    @classmethod
    def emit_directly(cls, task, *args, **kwargs):
        kwargs["directTask"] = task
        return cls._emit_component(*args, **kwargs)

    @classmethod
    def ack(cls, tup, strategy_record_map: Dict):
        if strategy_record_map is None:
            cls._send_message_to_parent({"command": "ack", "id": tup.id})
        else:
            cls._send_message_to_parent({"command": "ack", "id": tup.id, "info": strategy_record_map})

    @classmethod
    def fail(cls, tup):
        cls._send_message_to_parent({"command": "fail", "id": tup.id})

    @classmethod
    def report_error(cls, msg):
        cls._send_message_to_parent({"command": "error", "msg": msg})

    @classmethod
    def log(cls, msg, level=2):
        cls._send_message_to_parent({"command": "log", "msg": msg, "level": level})

    @classmethod
    def log_trace(cls, msg):
        cls.log(msg, 0)

    @classmethod
    def log_debug(cls, msg):
        cls.log(msg, 1)

    @classmethod
    def log_info(cls, msg):
        cls.log(msg, 2)

    @classmethod
    def log_warn(cls, msg):
        cls.log(msg, 3)

    @classmethod
    def log_error(cls, msg):
        cls.log(msg, 4)

    @classmethod
    def rpc_metrics(cls, name, params):
        cls._send_message_to_parent({"command": "metrics", "name": name, "params": params})

    @classmethod
    def _init_component(cls):
        setup_info = cls._read_message()
        cls._send_pid(setup_info['pidDir'])
        return [setup_info['conf'], setup_info['context']]


class Bolt(Component):

    @classmethod
    @overrides
    def _emit_component(cls, tup, stream=None, anchors=None, direct_task=None):
        if anchors is None:
            anchors = []
        m = {"command": "emit"}
        if stream is not None:
            m["stream"] = stream
        m["anchors"] = [a.id for a in anchors]
        if direct_task is not None:
            m["task"] = direct_task
        m["tuple"] = tup
        cls._send_message_to_parent(m)
        return cls._read_task_ids()

    def initialize(self, stormconf, context):
        pass

    def process(self, tuple):
        pass

    def run(self):
        conf, context = Component._init_component()
        try:
            self.initialize(conf, context)
            while True:
                tup = Component._read_tuple()
                if tup.is_heartbeat_tuple():
                    Component._sync()
                else:
                    self.process(tup)
        except Exception:
            Component.report_error(traceback.format_exc())


class Spout(Component):

    @classmethod
    @overrides
    def _emit_component(cls, tup, stream=None, message_id=None, direct_task=None):
        m = {"command": "emit"}
        if message_id is not None:
            m["id"] = message_id
        if stream is not None:
            m["stream"] = stream
        if direct_task is not None:
            m["task"] = direct_task
        m["tuple"] = tup
        cls._send_message_to_parent(m)
        return cls._read_task_ids()

    def initialize(self, conf, context):
        pass

    def activate(self):
        pass

    def deactivate(self):
        pass

    @overrides
    def ack(self, id):
        pass

    @overrides
    def fail(self, id):
        pass

    def next_tuple(self):
        pass

    def run(self):
        conf, context = Component._init_component()
        try:
            self.initialize(conf, context)
            while True:
                msg = Component._read_command()
                if msg["command"] == "activate":
                    self.activate()
                if msg["command"] == "deactivate":
                    self.deactivate()
                if msg["command"] == "next":
                    self.next_tuple()
                if msg["command"] == "ack":
                    self.ack(msg["id"])
                if msg["command"] == "fail":
                    self.fail(msg["id"])
                Component._sync()
        except Exception:
            Component.report_error(traceback.format_exc())


class BasicBolt(Bolt):

    def __init__(self):
        self.anchors = []

    def emit(self, value: List, stream=None):
        Bolt._emit_component(tup=value, stream=stream, anchors=self.anchors)

    def initialize(self, stormconf, context):
        pass

    def process(self, storm_tuple: Tuple):
        pass

    def run(self):
        conf, context = Component._init_component()
        try:
            self.initialize(conf, context)
            while True:
                tup = Component._read_tuple()
                if tup.is_heartbeat_tuple():
                    Component._sync()
                else:
                    try:
                        self.anchors = [tup]
                        strategy_record_data = self.process(tup)
                        sys.stdout.flush()
                        if strategy_record_data is not None and isinstance(strategy_record_data, utility.StrategyRecordData):
                            self.ack(tup, strategy_record_data.to_map())
                        else:
                            self.ack(tup, None)
                    except Exception:
                        self.report_error(traceback.format_exc())
                        self.fail(tup)
        except Exception:
            self.report_error(traceback.format_exc())
