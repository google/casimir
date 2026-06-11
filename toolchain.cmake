# Copyright 2026, The Android Open Source Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Force CMake to allow older policies (compatibility with CMake < 3.5 was
# removed in newer CMake versions, which breaks older bundled dependencies
# like c-ares in grpcio-sys).
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)
