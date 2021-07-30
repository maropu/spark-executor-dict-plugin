#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import grpc

from kvs_pb2 import LookupRequest
from kvs_pb2_grpc import AssociativeArrayStub


class DictClient():

    def __init__(self, port=6543) -> None:
        self.port = port
        self.stub = AssociativeArrayStub(grpc.insecure_channel(f"localhost:{port}"))

    def lookup(self, key: str) -> str:
        response = self.stub.Lookup(LookupRequest(key=key))
        return response.value

