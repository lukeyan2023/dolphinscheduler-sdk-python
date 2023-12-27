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

"""Task SeaTunnel."""

import logging

from pydolphinscheduler.constants import TaskType
from pydolphinscheduler.core.task import Task
from pydolphinscheduler.exceptions import PyDSParamException

log = logging.getLogger(__file__)


class DeployMode:
    """SeaTunnel deploy mode, for now it just contain `LOCAL`, `CLIENT` and `CLUSTER`."""

    LOCAL = "local"
    CLUSTER = "cluster"
    CLIENT = "client"


class SparkMaster:
    """Spark Master mode, for now it just contain `YARN`, `LOCAL`, `SPARK` and `MESOS`."""
    YARN = "yarn"
    LOCAL = "local"
    SPARK = "spark://"
    MESOS = "mesos://"


class FlinkRunMode:
    """Flink run mode, for now it just contain `NONE`, `RUN`, `RUN_APPLICATION`"""

    NONE = "none"
    RUN = "run"
    RUN_APPLICATION = "run-application"


class StartupScript:
    SEATUNNEL = "seatunnel.sh"
    SEATUNNEL_FLINK_13_V2 = "start-seatunnel-flink-13-connector-v2.sh"
    SEATUNNEL_FLINK_15_V2 = "start-seatunnel-flink-15-connector-v2.sh"
    SEATUNNEL_FLINK_V2 = "start-seatunnel-flink-connector-v2.sh"
    SEATUNNEL_FLINK = "start-seatunnel-flink.sh"
    SEATUNNEL_SPARK_2_V2 = "start-seatunnel-spark-2-connector-v2.sh"
    SEATUNNEL_SPARK_3_V2 = "start-seatunnel-spark-3-connector-v2.sh"
    SEATUNNEL_SPARK_V2 = "start-seatunnel-spark-connector-v2.sh"
    SEATUNNEL_SPARK = "start-seatunnel-spark.sh"


class SeaTunnel(Task):
    """Task spark object, declare behavior for spark task to dolphinscheduler."""

    _task_custom_attr = {
        "raw_script",
        "startup_script",
        "deploy_mode",
        "use_custom",
        "spark_master",
        "flink_run_mode",
        "others"
    }

    ext: set = {".config", "conf", ".json"}
    ext_attr: str = "_raw_script"

    def __init__(
            self,
            name: str,
            raw_script: str,
            startup_script: StartupScript | None = StartupScript.SEATUNNEL,
            deploy_mode: DeployMode | None = None,
            use_custom: bool | None = True,
            spark_master: SparkMaster | None = None,
            flink_run_mode: FlinkRunMode | None = None,
            others: str | None = None,
            *args,
            **kwargs
    ):
        self._raw_script = raw_script
        super().__init__(name, TaskType.SEATUNNEL, *args, **kwargs)
        self.startup_script = startup_script
        self._deploy_mode = deploy_mode
        self.use_custom = use_custom
        self.spark_master = spark_master
        self.flink_run_mode = flink_run_mode
        self.others = others

    @property
    def deploy_mode(self) -> DeployMode | None:
        """
        """
        if self.startup_script == StartupScript.SEATUNNEL:
            log.info(
                "The StartupScript is set to SEATUNNEL, deploy_mode only support `LOCAL` and `CLUSTER`"
            )
            if (
                    self.deploy_mode == DeployMode.LOCAL
                    or self.deploy_mode == DeployMode.CLUSTER
            ):
                return self.deploy_mode
            else:
                raise PyDSParamException(
                    "Parameter deployMode %s not support, when StartupScript is set to SEATUNNEL", self.deploy_mode
                )
        elif (
                self.startup_script == StartupScript.SEATUNNEL_SPARK
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_2_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_3_V2
        ):
            log.info(
                "The StartupScript is set to SPARK, deploy_mode only support `CLIENT` and `CLUSTER`"
            )
            if (
                    self.deploy_mode == DeployMode.CLUSTER
                    or self.deploy_mode == DeployMode.CLIENT
            ):
                return self.deploy_mode
            else:
                raise PyDSParamException(
                    "Parameter deployMode %s not support, when StartupScript is set to SPARK", self.deploy_mode
                )
        else:
            if self.deploy_mode is not None:
                log.warning(
                    "Parameter deploy_mode does not need to be specified, when StartupScript is set to FLINK"
                )
            return None

    @property
    def spark_master(self) -> SparkMaster | None:
        """"""
        if (
                self.startup_script == StartupScript.SEATUNNEL_SPARK
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_2_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_3_V2
        ):
            if (
                    self.spark_master == SparkMaster.YARN
                    or self.spark_master == SparkMaster.LOCAL
                    or self.spark_master == SparkMaster.SPARK
                    or self.spark_master == SparkMaster.MESOS
            ):
                return self.spark_master
            else:
                raise PyDSParamException(
                    "Parameter spark_master %s not support.", self.spark_master
                )
        else:
            if self.spark_master is not None:
                log.warning(
                    "Parameter spark_master does not need to be specified, when StartupScript is set to SEATUNNEL or "
                    "FLINK"
                )
            return None

    @property
    def flink_run_mode(self) -> FlinkRunMode | None:
        """"""
        if (
                self.startup_script == StartupScript.SEATUNNEL_FLINK
                or self.startup_script == StartupScript.SEATUNNEL_FLINK_V2
                or self.startup_script == StartupScript.SEATUNNEL_FLINK_13_V2
                or self.startup_script == StartupScript.SEATUNNEL_FLINK_15_V2
        ):
            if (
                    self.flink_run_mode == FlinkRunMode.RUN
                    or self.flink_run_mode == FlinkRunMode.NONE
                    or self.flink_run_mode == FlinkRunMode.RUN_APPLICATION
            ):
                return self.flink_run_mode
            else:
                raise PyDSParamException(
                    "Parameter flink_run_mode %s not support.", self.flink_run_mode
                )
        else:
            if self.flink_run_mode is not None:
                log.warning(
                    "Parameter flink_run_mode does not need to be specified, when StartupScript is set to SEATUNNEL or "
                    "SPARK"
                )
            return None

    @property
    def others(self) -> str | None:
        """"""
        if (
                self.startup_script == StartupScript.SEATUNNEL_SPARK
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_2_V2
                or self.startup_script == StartupScript.SEATUNNEL_SPARK_3_V2
        ):
            if self.others is not None:
                log.warning(
                    "Parameter others does not need to be specified, when StartupScript is set SPARK"
                )
            return None
        else:
            return self.others
