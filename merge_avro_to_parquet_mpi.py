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
"""Merge individual alerts (avro) into big parquet files. Compression factor is about 1.6"""
import os
import glob
import sys
import logging
import argparse
import time

import numpy as np

from mpi4py import MPI

from fastavro import reader, writer, parse_schema

from fink_client.avroUtils import AlertReader

parser = argparse.ArgumentParser(
    description='Merge avro avro files into single avro file.'
)
parser.add_argument(
    "-i", "--inputdir",
    action="store", dest="input_dir",
    help="Input file directory", required=True
)
parser.add_argument(
    "-o", "--output_dir",
    action="store", dest="output_dir",
    help="Output file name", required=True
)
parser.add_argument(
    "-q", "--quiet",
    action="store_false", dest="verbose",
    help="don't print log messages to stdout", default=True
)
args = parser.parse_args()

rank = MPI.COMM_WORLD.rank
size = MPI.COMM_WORLD.size

if rank == 0:
    t0 = time.time()

# Add Logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

if args.verbose is False:
    ch.setLevel(logging.ERROR)
else:
    ch.setLevel(logging.DEBUG)

# init variables
input_dir = args.input_dir
NFILE = 3000

filelist = glob.glob(os.path.join(input_dir, '*.avro'))
if rank == 0:
    if len(filelist) == 0:
        logging.error('No data for {}'.format(input_dir))
        sys.exit()

if rank == 0:
    logging.info('Start writing merged data into: %s', args.output_dir)

with open(filelist[0], mode='rb') as file_data:
    file_data.seek(0)
    data = reader(file_data)
    schema = data.writer_schema
schema = parse_schema(schema)

nsplit = int(len(filelist) / NFILE) + 1

filelist_split = np.array_split(filelist, nsplit)

for index, index_split in enumerate(range(rank, len(filelist_split), size)):
    fns = filelist_split[index_split].tolist()

    logging.info('{}/{}...'.format(index_split + 1, len(filelist_split)))
    name = os.path.join(args.output_dir, fns[0].split('/')[1])

    r = AlertReader(fns)

    pdf = r.to_pandas()
    pdf.to_parquet(name.replace('.avro', '.parquet'))

MPI.COMM_WORLD.barrier()
if rank == 0:
    logging.info('Took {:.2f} seconds'.format(time.time() - t0))
