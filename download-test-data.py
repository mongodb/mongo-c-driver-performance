#!/usr/bin/env python

# Copyright 2016 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Download test data for the MongoDB Driver Performance Benchmarking Spec."""

import argparse
import glob
import shutil
import tarfile
from os.path import join, realpath, exists, dirname, isdir

try:
    from urllib.parse import urljoin
    from urllib.request import urlretrieve
except ImportError:
    from urlparse import urljoin
    from urllib import urlretrieve


TEST_PATH = join(
    dirname(realpath(__file__)),
    join('performance-testdata'))

BASE = ("https://github.com/ajdavis/driver-performance-test-data/"
        "raw/add-closing-brace/")


def download_test_data(refresh):
    if not isdir(TEST_PATH):
        raise Exception("No directory '%s'" % (TEST_PATH, ))

    if refresh:
        for path in glob.glob(join(TEST_PATH, '*')):
            if isdir(path):
                shutil.rmtree(path)

    for name in "extended_bson", "single_and_multi_document", "parallel":
        target_dir = join(TEST_PATH, name)

        if not exists(target_dir):
            print('Downloading %s.tgz' % (name, ))
            file_path = join(TEST_PATH, name + ".tgz")
            urlretrieve(urljoin(BASE, name + ".tgz"), file_path)

            # Each tgz contains a single directory, e.g. "parallel.tgz" expands
            # to a directory called "parallel" full of data files.
            with tarfile.open(file_path, "r:gz") as tar:
                tar.extractall(path=TEST_PATH)
                tar.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-r", "--refresh", action='store_true',
                        help="Download again")
    args = parser.parse_args()
    download_test_data(args.refresh)
