# Copyright 2024 TikTok Pte. Ltd.
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

import os
import json
import atexit
from typing import List
from jpype import JClass, getDefaultJVMPath, startJVM
from jpype import shutdownJVM, isJVMStarted

from petsql.common import Config

from .exception import SQLCompilerException


class JVMManager:
    """
    Singleton pattern JVM manager, responsible for managing the entire lifecycle of the JVM,
    ensuring that the JVM is started and shut down correctly.

    Attributes
    ----------
    is_started : bool
        Whether the JVM is started.
    java_path : str
        The path of the Java class file.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JVMManager, cls).__new__(cls)
            cls._instance.is_started = False
            cls._instance.java_path = ""
        return cls._instance

    def start_jvm(self, java_path: str) -> None:
        """
        Start the JVM if it is not started.

        Parameters
        ----------
        java_path : str
            The path of the Java class file.
        """
        if not self.is_started:
            if not isJVMStarted():
                self.java_path = java_path
                startJVM(getDefaultJVMPath(), "-ea", "-Djava.class.path=" + self.java_path)
                self.is_started = True

    def shutdown_jvm(self) -> None:
        """
        Shutdown the JVM if it is started.
        """
        if self.is_started and isJVMStarted():
            shutdownJVM()
            self.is_started = False


class SQLCompiler:
    """
    SQLCompiler is a class that compiles SQL statements into logical plans.
    The class invokes Calcite.

    Parameters
    ----------
    java_path : str, default=None
        The path of the Java class file. If not provided, the default path is used.

    Attributes
    ----------
    java_path : str
        The path of the Java class file.
    """

    def __init__(self, java_path=None) -> None:
        if java_path is None:
            java_path = os.path.dirname(os.path.abspath(__file__)) + "/__binding/petsql-1.0.jar"
        JVMManager().start_jvm(java_path)

    def compile(self, sql: str, config: Config) -> List:
        """
        Compile the SQL statement into a logical plan.

        Parameters
        ----------
        sql : str
            The SQL statement to be compiled.
        config : Config
            The configuration of the SQL statement.

        Returns
        -------
        List
            The logical plan of the SQL statement.
        """
        try:
            # Get the SQLCompiler class from JVM
            sql_compiler = JClass("com.tiktok.petsql.SQLCompiler")

            compiler = sql_compiler()
            tmp = config.to_dict()
            ret = compiler.sqlToLogicPlan(sql, json.dumps({"schemas": tmp["schemas"]}))
            return json.loads(str(ret))["rels"]
        except Exception as e:
            raise SQLCompilerException(str(e)) from e


# Shutdown the JVM when the program exits
atexit.register(JVMManager().shutdown_jvm)
