# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import json
import mock
from requests.exceptions import HTTPError
from requests.models import Response

import databricks_cli.dbfs.cli as cli
from databricks_cli.dbfs.api import DbfsErrorCodes
from tests.utils import get_callback, get_error_and_quit_mock, eat_error_and_quit


def test_cp_cli_to_dbfs_non_recursive(tmpdir):
    testfile = os.path.join(tmpdir.strpath, 'testfile')
    with open(testfile, 'wb'):
        pass
    with mock.patch('databricks_cli.dbfs.cli.get_status') as get_status_mock:
        with mock.patch('databricks_cli.dbfs.cli.put_file') as put_file_mock:
            resp = Response()
            resp._content = json.dumps({'error_code': DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST})
            get_status_mock.side_effect = HTTPError(response=resp)
            get_callback(cli.cp_cli)(False, False, testfile, 'dbfs:/apple')
            assert put_file_mock.call_count == 1
            assert put_file_mock.call_args_list[0][0][0] == testfile


@get_error_and_quit_mock('databricks_cli.dbfs.cli')
def test_cp_cli_to_dbfs_non_recursive_src_dir(tmpdir, **kwargs):
    """
    Invoke ``databricks fs cp src dst`` with ``src`` as a dir.
    This should error and quit early.
    """
    error_and_quit_mock = kwargs['error_and_quit_mock']
    path = tmpdir.strpath
    eat_error_and_quit(get_callback(cli.cp_cli))(False, False, path, 'dbfs:/apple')
    assert error_and_quit_mock.call_count == 1
    error_string = 'The local file {} is a directory.'.format(path)
    assert error_string in error_and_quit_mock.call_args_list[0][0][0]


@get_error_and_quit_mock('databricks_cli.dbfs.cli')
def test_cp_cli_both_local(tmpdir, **kwargs):
    """
    Invoke ``databricks fs cp src dst`` with ``src`` and ``dst``
    in the local filesystem.
    This should error and quit early.
    """
    error_and_quit_mock = kwargs['error_and_quit_mock']
    path = tmpdir.strpath
    eat_error_and_quit(get_callback(cli.cp_cli))(False, False, path, path)
    assert error_and_quit_mock.call_count == 1
    error_string = 'Both paths provided are from your local filesystem.'
    assert error_string in error_and_quit_mock.call_args_list[0][0][0]


@get_error_and_quit_mock('databricks_cli.dbfs.cli')
def test_cp_cli_both_dbfs(**kwargs):
    """
    Invoke ``databricks fs cp src dst`` with ``src`` and ``dst``
    in the DBFS.
    This should error and quit early.
    """
    path = 'dbfs:/apple'
    error_and_quit_mock = kwargs['error_and_quit_mock']
    eat_error_and_quit(get_callback(cli.cp_cli))(False, False, path, path)
    assert error_and_quit_mock.call_count == 1
    assert error_and_quit_mock.call_count == 1
    error_string = 'Both paths provided are from the DBFS filesystem.'
    assert error_string in error_and_quit_mock.call_args_list[0][0][0]
