#!/usr/bin/python
# Copyright 2021 AstroLab Software
# Author: Julien Peloton
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
import pandas as pd
import numpy as np

# Read clipboard
pdf = pd.read_clipboard(sep='\t', skiprows=range(1, 10000, 2))

# Keep only night with more than 1GB of data
pdf = pdf[pdf['Size'].apply(lambda x: 'G' in x)]

# Number of TeraBytes
n_tera = pdf['Size'].apply(lambda x: float(x[:-1])).sum() / 1024
print('Data volume: {} TB'.format(n_tera))

# Format URL
pdf['Name'].apply(lambda x: 'https://ztf.uw.edu/alerts/public/' + x)\
    .to_csv('uri-ztf.csv', header=None, index=False)

# Number of nights per call
n_file_per_split = 5

# Split
pdf = pd.read_csv('uri-ztf.csv', header=None)
indices = np.arange(0, len(pdf), n_file_per_split).tolist() + [-1]
for left, right in zip(indices[0:-1], indices[1:]):
    subpdf = pd.DataFrame(pdf[0].values[left: right])
    subpdf.to_csv('uris/{:03d}.txt'.format(left), header=None, index=False)
