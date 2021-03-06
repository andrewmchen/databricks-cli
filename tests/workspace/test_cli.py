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
import mock

import databricks_cli.workspace.cli as cli
import databricks_cli.workspace.api as api
from databricks_cli.workspace.api import WorkspaceFileInfo, NOTEBOOK
from databricks_cli.workspace.types import WorkspaceLanguage, WorkspaceFormat
from tests.utils import get_callback

def test_export_workspace_cli(tmpdir):
    path = tmpdir.strpath
    with mock.patch('databricks_cli.workspace.cli.get_status') as get_status_mock:
        with mock.patch('databricks_cli.workspace.cli.export_workspace') as export_workspace_mock:
            get_status_mock.return_value = WorkspaceFileInfo('/notebook-name', NOTEBOOK, WorkspaceLanguage.SCALA)
            get_callback(cli.export_workspace_cli)('/notebook-name', path, WorkspaceFormat.SOURCE, False)
            assert export_workspace_mock.call_args[0][1] == os.path.join(path, 'notebook-name.scala')

def test_export_dir_helper(tmpdir):
    """
    Copy to directory ``tmpdir`` with structure as follows
    - a (directory)
      - b (scala)
      - c (python)
      - d (r)
      - e (sql)
    - f (directory)
      - g (directory)
    """
    def _list_objects_mock(path):
        if path == '/':
            return [
                WorkspaceFileInfo('/a', api.DIRECTORY),
                WorkspaceFileInfo('/f', api.DIRECTORY)
            ]
        elif path == '/a':
            return [
                WorkspaceFileInfo('/a/b', api.NOTEBOOK, WorkspaceLanguage.SCALA),
                WorkspaceFileInfo('/a/c', api.NOTEBOOK, WorkspaceLanguage.PYTHON),
                WorkspaceFileInfo('/a/d', api.NOTEBOOK, WorkspaceLanguage.R),
                WorkspaceFileInfo('/a/e', api.NOTEBOOK, WorkspaceLanguage.SQL),
            ]
        elif path == '/f':
            return [WorkspaceFileInfo('/f/g', api.DIRECTORY)]
        elif path == '/f/g':
            return []
        else:
            assert False, 'We shouldn\'t reach this case...'

    with mock.patch('databricks_cli.workspace.cli.list_objects', new=mock.Mock(wraps=_list_objects_mock)) as list_objects_mock:
        with mock.patch('databricks_cli.workspace.cli.export_workspace') as export_workspace_mock:
            cli._export_dir_helper('/', tmpdir.strpath, False)
            # Verify that the directories a, f, g exist.
            assert os.path.isdir(os.path.join(tmpdir.strpath, 'a'))
            assert os.path.isdir(os.path.join(tmpdir.strpath, 'f'))
            assert os.path.isdir(os.path.join(tmpdir.strpath, 'f', 'g'))
            # Verify we exported files b, c, d, e with the correct names
            assert export_workspace_mock.call_count == 4
            assert export_workspace_mock.call_args_list[0][0][0] == '/a/b'
            assert export_workspace_mock.call_args_list[0][0][1] == tmpdir.strpath + '/a/b.scala'
            assert export_workspace_mock.call_args_list[1][0][0] == '/a/c'
            assert export_workspace_mock.call_args_list[1][0][1] == tmpdir.strpath + '/a/c.py'
            assert export_workspace_mock.call_args_list[2][0][0] == '/a/d'
            assert export_workspace_mock.call_args_list[2][0][1] == tmpdir.strpath + '/a/d.r'
            assert export_workspace_mock.call_args_list[3][0][0] == '/a/e'
            assert export_workspace_mock.call_args_list[3][0][1] == tmpdir.strpath + '/a/e.sql'
            # Verify that we only called list 4 times.
            assert list_objects_mock.call_count == 4

def test_import_dir_helper(tmpdir):
    """
    Copy from directory ``tmpdir`` with structure as follows
    - a (directory)
      - b (scala)
      - c (python)
      - d (r)
      - e (sql)
    - f (directory)
      - g (directory)
    """
    os.makedirs(os.path.join(tmpdir.strpath, 'a'))
    os.makedirs(os.path.join(tmpdir.strpath, 'f'))
    os.makedirs(os.path.join(tmpdir.strpath, 'f', 'g'))
    with open(os.path.join(tmpdir.strpath, 'a', 'b.scala'), 'wb') as f:
        f.write('println(1 + 1)')
    with open(os.path.join(tmpdir.strpath, 'a', 'c.py'), 'wb') as f:
        f.write('print 1 + 1')
    with open(os.path.join(tmpdir.strpath, 'a', 'd.r'), 'wb') as f:
        f.write('I don\'t know how to write r')
    with open(os.path.join(tmpdir.strpath, 'a', 'e.sql'), 'wb') as f:
        f.write('select 1+1 from table;')
    with mock.patch('databricks_cli.workspace.cli.mkdirs') as mkdirs_mock:
        with mock.patch('databricks_cli.workspace.cli.import_workspace') as import_workspace:
            cli._import_dir_helper(tmpdir.strpath, '/', False)
            # Verify that the directories a, f, g exist.
            assert mkdirs_mock.call_count == 4
            # The order of list may be undeterminstic apparently. (It's different in Travis CI)
            assert any([ca[0][0] == '/' for ca in mkdirs_mock.call_args_list])
            assert any([ca[0][0] == '/a' for ca in mkdirs_mock.call_args_list])
            assert any([ca[0][0] == '/f' for ca in mkdirs_mock.call_args_list])
            assert any([ca[0][0] == '/f/g' for ca in mkdirs_mock.call_args_list])
            # Verify that we imported the correct files
            assert import_workspace.call_count == 4
            assert any([ca[0][0] == tmpdir.strpath + '/a/b.scala' \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][0] == tmpdir.strpath + '/a/c.py' \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][0] == tmpdir.strpath + '/a/d.r' \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][0] == tmpdir.strpath + '/a/e.sql' \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][1] == '/a/b' for ca in import_workspace.call_args_list])
            assert any([ca[0][1] == '/a/c' for ca in import_workspace.call_args_list])
            assert any([ca[0][1] == '/a/d' for ca in import_workspace.call_args_list])
            assert any([ca[0][1] == '/a/e' for ca in import_workspace.call_args_list])
            assert any([ca[0][2] == WorkspaceLanguage.SCALA \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][2] == WorkspaceLanguage.PYTHON \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][2] == WorkspaceLanguage.R \
                    for ca in import_workspace.call_args_list])
            assert any([ca[0][2] == WorkspaceLanguage.SQL \
                    for ca in import_workspace.call_args_list])

