"""
Database Client Module for CUBRID Database in Django.

This module provides the implementation of the DatabaseClient class, which extends
Django's BaseDatabaseClient. It is tailored specifically for the CUBRID database,
offering functionalities to interface with the CUBRID command-line tools directly
from Django. The main purpose of this class is to facilitate database operations
such as running management commands or performing database maintenance tasks from
within a Django application environment.

The DatabaseClient class is primarily used for debugging or managing the database
from the Django command-line interface. It is not typically used in application
code but can be valuable for administrative tasks and during development.

The module's implementation ensures compatibility with CUBRID's command-line
interface, adapting Django's standard database client functionalities to suit the
specific requirements and behavior of the CUBRID database system.
"""

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    """
    A Django database client for the CUBRID database.

    This class extends Django's BaseDatabaseClient, providing custom functionality
    for interfacing with the CUBRID database. It handles the construction of command-line
    arguments required to invoke CUBRID's command-line tools, facilitating various
    database operations from within Django.

    Attributes:
    executable_name (str): The name of the CUBRID command-line executable. Defaults
                           to 'csql', which is the standard command-line tool for
                           interacting with CUBRID databases.

    Methods:
    settings_to_cmd_args_env(settings_dict, parameters): Generates the command-line
                                                          arguments and environment
                                                          settings required to connect
                                                          to the CUBRID database using
                                                          the provided Django settings.

    The class is typically used internally by Django's 'dbshell' command and other
    database management commands.

    Example Usage:
    # This is an internal Django class and is not typically used directly in application code.
    """
    executable_name = 'csql'

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]

        database = settings_dict["OPTIONS"].get(
            "database",
            settings_dict["OPTIONS"].get("db", settings_dict["NAME"]),
        )
        user = settings_dict["OPTIONS"].get("user", settings_dict["USER"])
        password = settings_dict["OPTIONS"].get(
            "password",
            settings_dict["OPTIONS"].get("passwd", settings_dict["PASSWORD"]),
        )
        host = settings_dict["OPTIONS"].get("host", settings_dict["HOST"])

        if user:
            args += ["-u", user]
        if password:
            args += ["-p", password]
        if database:
            if host:
                args += [f'{database}@{host}']
            else:
                args += [database]

        args.extend(parameters)
        return args, None
