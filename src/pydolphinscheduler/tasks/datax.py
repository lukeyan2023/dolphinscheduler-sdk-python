# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Task datax."""

from __future__ import annotations

from pydolphinscheduler.constants import TaskType
from pydolphinscheduler.core.mixin import WorkerResourceMixin
from pydolphinscheduler.core.task import Task
from pydolphinscheduler.models.datasource import Datasource


class CustomDataX(WorkerResourceMixin, Task):
    """Task CustomDatax object, declare behavior for custom DataX task to dolphinscheduler.

    You provider json template for DataX, it can synchronize data according to the template you provided.
    """

    CUSTOM_CONFIG = 1

    _task_custom_attr = {"custom_config", "json", "xms", "xmx"}

    ext: set = {".json"}
    ext_attr: str = "_json"

    def __init__(
        self,
        name: str,
        json: str,
        xms: int | None = 1,
        xmx: int | None = 1,
        *args,
        **kwargs,
    ):
        self._json = json
        super().__init__(name, TaskType.DATAX, *args, **kwargs)
        self.custom_config = self.CUSTOM_CONFIG
        self.xms = xms
        self.xmx = xmx
        self.add_attr(**kwargs)


class DataX(WorkerResourceMixin, Task):
    """Task DataX object, declare behavior for DataX task to dolphinscheduler.

    It should run database datax job in multiply sql link engine, such as:

    - MySQL
    - Oracle
    - Postgresql
    - SQLServer

    You provider datasource_name and datatarget_name contain connection information, it decisions which
    database type and database instance would synchronous data.

    :param name: task name.
    :param datasource_name: source database name for task datax to extract data.
    :param datatarget_name: target database name for task datax to load data.
    :param sql: sql statement for task datax to extract data form source database.
    :param target_table: target table name for task datax to load data into target database.
    :param datasource_type: source database type, dolphinscheduler use
    """

    CUSTOM_CONFIG = 0

    _task_custom_attr = {
        "custom_config",
        "sql",
        "target_table",
        "job_speed_byte",
        "job_speed_record",
        "pre_statements",
        "post_statements",
        "xms",
        "xmx",
    }

    ext: set = {".sql"}
    ext_attr: str = "_sql"

    def __init__(
        self,
        name: str,
        datasource_name: str,
        datatarget_name: str,
        sql: str,
        target_table: str,
        datasource_type: str | None = None,
        datatarget_type: str | None = None,
        job_speed_byte: int | None = 0,
        job_speed_record: int | None = 1000,
        pre_statements: list[str] | None = None,
        post_statements: list[str] | None = None,
        xms: int | None = 1,
        xmx: int | None = 1,
        *args,
        **kwargs,
    ):
        self._sql = sql
        super().__init__(name, TaskType.DATAX, *args, **kwargs)
        self.custom_config = self.CUSTOM_CONFIG
        self.datasource_type = datasource_type
        self.datasource_name = datasource_name
        self.datatarget_type = datatarget_type
        self.datatarget_name = datatarget_name
        self.target_table = target_table
        self.job_speed_byte = job_speed_byte
        self.job_speed_record = job_speed_record
        self.pre_statements = pre_statements or []
        self.post_statements = post_statements or []
        self.xms = xms
        self.xmx = xmx
        self.add_attr(**kwargs)

    @property
    def source_params(self) -> dict:
        """Get source params for datax task."""
        datasource_task_u = Datasource.get_task_usage_4j(
            self.datasource_name, self.datasource_type
        )
        return {
            "dsType": datasource_task_u.type,
            "dataSource": datasource_task_u.id,
        }

    @property
    def target_params(self) -> dict:
        """Get target params for datax task."""
        datasource_task_u = Datasource.get_task_usage_4j(
            self.datatarget_name, self.datatarget_type
        )
        return {
            "dtType": datasource_task_u.type,
            "dataTarget": datasource_task_u.id,
        }

    @property
    def task_params(self, camel_attr: bool = True, custom_attr: set = None) -> dict:
        """Override Task.task_params for datax task.

        datax task have some specials attribute for task_params, and is odd if we
        directly set as python property, so we Override Task.task_params here.
        """
        params = super().task_params
        params.update(self.source_params)
        params.update(self.target_params)
        return params
