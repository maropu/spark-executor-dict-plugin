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

from dict_pb2 import LookupRequest
from dict_pb2_grpc import DictStub
from typing import Union


class DictClient():

    def __init__(self, port=6543, num_retries=3, error_on_rpc_failrues=True) -> None:
        self.port = port
        self.stub = DictStub(grpc.insecure_channel(f'localhost:{port}'))
        self.num_retries = num_retries
        self.error_on_rpc_failrues = error_on_rpc_failrues

    def _lookup(self, req: LookupRequest) -> str:
        num_trials = 0
        while num_trials <= self.num_retries:
            num_trials += 1
            try:
                response = self.stub.Lookup(req)
                return response.value
            except Exception as e:
                if num_trials >= self.num_retries and self.error_on_rpc_failrues:
                    raise RuntimeError(f'RPC failed: {e}')

        raise RuntimeError(f'RPC (key={req} lookup) failed '
                           f'because the maximum number of retries (num_retries={self.num_retries}) reached')

    def lookup(self, key: Union[str, int]) -> str:
        if type(key) is str:
            return self._lookup(LookupRequest(strKey=key))
        elif type(key) is int:
            return self._lookup(LookupRequest(int64Key=key))
        else:
            raise ValueError(f'Unsupported type: {type(key).__name__}')

    def lookup_by_str(self, key: str) -> str:
        return self._lookup(LookupRequest(strKey=key))

    def lookup_by_i32(self, key: int) -> str:
        return self._lookup(LookupRequest(int32Key=key))

    def lookup_by_i64(self, key: int) -> str:
        return self._lookup(LookupRequest(int64Key=key))
