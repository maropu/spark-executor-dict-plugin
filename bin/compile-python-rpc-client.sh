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

ROOT_DIR="$( cd "`dirname $0`"/..; pwd )"

# Activate a conda virtual env
. ${ROOT_DIR}/bin/conda.sh && activate_conda_virtual_env "${ROOT_DIR}"

# Generates a RPC Python client to look up a dictonary
cd $ROOT_DIR/python
python3 -m grpc_tools.protoc \
  -I${ROOT_DIR}/src/main/proto \
  --python_out=. \
  --grpc_python_out=. \
  --experimental_allow_proto3_optional \
  ${ROOT_DIR}/src/main/proto/dict.proto

