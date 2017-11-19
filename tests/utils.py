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

import six
import mock


def eat_error_and_quit(function):
    def inner(*args, **kwargs):
        try:
            function(*args, **kwargs)
        except _ErrorAndQuitError:
            pass
    return inner


def get_callback(command):
    """
    Convenience function to reach into a Command object's callback and
    to unwrap the decorators
    """
    function = command.callback
    while hasattr(function, '__wrapped__'):
        function = function.__wrapped__
    return function


class get_error_and_quit_mock(object): # NOQA
    """
    A decorator which provides a mock for ``error_and_quit`` in kwargs.
    For example:

    @error_and_quit_mock('databricks_cli.dbfs.cli')
    def test_cp_cli_to_dbfs_non_recursive_src_dir(tmpdir, **kwargs):
        error_and_quit_mock = kwargs['error_and_quit_mock']
        error_and_quit_decorator(test_function)
    """
    def __init__(self, mock_path):
        self.mock_path = mock_path + '.error_and_quit'

    def __call__(self, function):
        @six.wraps(function)
        def decorator(*args, **kwargs):
            with mock.patch(self.mock_path) as e_mock:
                e_mock.side_effect = _ErrorAndQuitError()
                kwargs['error_and_quit_mock'] = e_mock
                return function(*args, **kwargs)
        return decorator


class _ErrorAndQuitError(RuntimeError):
    pass
