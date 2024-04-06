# Copyright 2024 Fasih Khatib
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

import dask.dataframe as dd

from tsum.log import log
from tsum.model import Pattern
from tsum.util import filter_


def overhead(T: dd.DataFrame, pattern: Pattern) -> int:
    N = len(filter_(T=T, pattern=pattern))
    D = len(T.columns)
    return D * log(N)
