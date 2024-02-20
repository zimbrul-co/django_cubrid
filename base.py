"""
Module: base.py

This module forms the core of the Django backend for the CUBRID database. It includes
the implementation of the BaseDatabaseWrapper class, essential for integrating the
CUBRID database with Django's ORM layer. This backend enables Django applications to
interact seamlessly with CUBRID databases, leveraging Django's built-in features such
as model definition, query generation, and transaction management.

The module handles the import and configuration of the cubrid_db database adapter,
ensuring that it is properly set up to interact with Django. It also manages the
creation and maintenance of database connections, providing a bridge between Django's
abstractions and the specific requirements of the CUBRID database system.

Key Components:
- BaseDatabaseWrapper: A subclass of Django's base wrapper class, tailored to meet the
  specific connection and query execution requirements of the CUBRID database.
- Exception Handling: Integration of CUBRID-specific exceptions into Django's error
  management system, allowing for smooth error handling within Django applications.
- Signal Dispatching: Implementation of connection signals for notifying different
  components of the Django framework when database connections are created.

Requirements:
- Django: This module is designed to be used with Django and relies on Django's internal
  mechanisms and structures.
- cubrid_db: The cubrid_db Python adapter is required for database interactions and must
  be installed and configured in the environment where Django is running.

This module is a crucial part of the Django-CUBRID backend, enabling Django applications
to utilize CUBRID as their database backend in a way that is consistent with Django's
architecture and design philosophy.

For detailed documentation on configuring and using this backend, refer to the official
Django documentation and the documentation specific to the Django-CUBRID backend.
"""

import re
import django
import django.db.utils

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.signals import connection_created
from django.utils.asyncio import async_unsafe
from django.utils.regex_helper import _lazy_re_compile

try:
    import cubrid_db as Database
except ImportError as import_error:
    raise ImproperlyConfigured(f"Error loading cubrid_db module: {import_error}") from import_error

from django_cubrid.client import DatabaseClient
from django_cubrid.creation import DatabaseCreation
from django_cubrid.features import DatabaseFeatures
from django_cubrid.introspection import DatabaseIntrospection
from django_cubrid.operations import DatabaseOperations
from django_cubrid.schema import DatabaseSchemaEditor
from django_cubrid.validation import DatabaseValidation


# This should match the numerical portion of the version numbers (we can treat
# versions like 5.0.24 and 5.0.24a as the same).
db_version_re = _lazy_re_compile(r"(\d{1,2})\.(\d{1,2})\.(\d{1,2}).(\d{1,8})")


def get_django_error(e):
    """
    Takes a CUBRID exception and returns the Django equivalent.
    """
    cubrid_exc_type = type(e)
    django_exc_type = getattr(django.db.utils,
        cubrid_exc_type.__name__, django.db.utils.Error)
    return django_exc_type(*tuple(e.args))


class CursorWrapper:
    """
    A thin wrapper around CUBRID's normal cursor class.
    """

    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, args=None):
        """
        Execute an SQL query with optional arguments.

        This method adapts and executes an SQL query on the underlying database cursor.
        It modifies the query by substituting Python-style placeholders ('%s') with
        database-specific placeholders (e.g., '?'), allowing for parameterized queries
        that can prevent SQL injection vulnerabilities. It then executes the adapted
        query using the provided arguments.

        Parameters:
        query (str): The SQL query to be executed. The query should use '%s' placeholders
                    for any parameters to be passed in.
        args (tuple, list, or dict, optional): The arguments to be used with the query.
                                            These arguments are substituted into the
                                            query in place of the placeholders.

        Returns:
        The return value of this method depends on the behavior of the `execute` method
        of the underlying database cursor. Typically, it could be the number of rows
        affected by the query, or a result set in case of a SELECT query.

        Raises:
        get_django_error(e): A Django-specific database error, translated from the
                            native database exceptions. This ensures that the database
                            errors are consistent with Django's exception handling.

        Example:
        cursor.execute("SELECT * FROM my_table WHERE column = %s", (value,))
        """
        try:
            if args:
                query = re.sub('([^%])%s', '\\1?', query)
                query = query.replace('%%', '%')
            else:
                # If there are no args, assume '%s' is intended to be literal and
                # should not be replaced.
                query = query.replace('%%', '%')

            return self.cursor.execute(query, args)

        except Database.Error as e:
            raise get_django_error(e) from e

    def executemany(self, query, args):
        """
        Execute a database query against all parameter sequences or mappings provided in 'args'.

        This method is similar to 'execute()', but instead of executing a single query with
        a single set of parameters, it executes the query multiple times, once for each set
        of parameters in 'args'. This is typically used for batch operations, such as
        inserting multiple rows into a table. The method adapts the query by replacing
        Python-style placeholders ('%s') with database-specific placeholders (e.g., '?').

        Parameters:
        query (str): The SQL query to be executed. The query should use '%s' placeholders
                    for any parameters to be passed in.
        args (list of tuples/lists/dicts): A list of parameter sequences or mappings. Each
                                        item in the list corresponds to one set of
                                        parameters to be used with the query.

        Returns:
        The return value of this method depends on the behavior of the `executemany` method
        of the underlying database cursor. It is typically the number of rows affected
        by each execution of the query.

        Raises:
        get_django_error(e): A Django-specific database error, translated from the native
                            database exceptions. This ensures that database errors are
                            consistent with Django's exception handling framework.

        Example:
        cursor.executemany("INSERT INTO my_table (column1, column2) VALUES (%s, %s)",
                        [(value1_1, value1_2), (value2_1, value2_2), ...])
        """
        try:
            query = re.sub('([^%])%s', '\\1?', query)
            query = re.sub('%%', '%', query)

            return self.cursor.executemany(query, args)
        except Database.Error as e:
            raise get_django_error(e) from e

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]

        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class DatabaseWrapper(BaseDatabaseWrapper):
    """
    A Django database backend wrapper for CUBRID.

    This class is a subclass of Django's BaseDatabaseWrapper and provides
    CUBRID-specific implementations for database interaction. It serves as the
    primary interface between Django's ORM layer and the CUBRID database.
    The wrapper handles the creation and management of database connections and
    translates Django's query syntax into queries that are compatible with CUBRID.

    Attributes:
    vendor (str): A string identifier for the CUBRID database backend.
    operators (dict): A mapping of Django query operators to CUBRID SQL syntax.
                      This mapping ensures that Django's ORM can construct
                      queries compatible with CUBRID's SQL dialect.

    The class includes methods for opening and closing database connections,
    managing transactions, and executing queries. It also contains logic for
    schema management, such as creating and altering database tables.

    Note:
    - This wrapper adapts Django's SQL syntax to be compatible with CUBRID,
      and there may be some differences in behavior compared to other database
      backends.
    - It is important to keep this backend up to date with changes in both
      Django's database API and CUBRID's features.

    Usage of this class is typically handled internally by Django's ORM;
    however, it can be used directly for advanced database operations that
    require specific customizations or optimizations.
    """

    vendor = 'cubrid'
    display_name = 'CUBRID'

    # Operators taken from PosgreSQL implementation.
    # Check for differences between this syntax and CUBRID's.
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
        'regex': 'REGEXP BINARY %s',
        'iregex': 'REGEXP %s',
    }
    # Patterns taken from other backend implementations.
    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\\', '\\\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    class VarbitStr(str):
        """Helper class for the Django BinaryField field type, to accomodate optional max_length"""
        def __mod__(self, field_dict):
            assert isinstance(field_dict, dict), "field_dict trebuie să fie un dicționar"
            assert 'max_length' in field_dict, "field_dict trebuie să conțină cheia 'max_length'"

            max_length = field_dict['max_length']
            if max_length is not None:
                return f"bit varying({8 * max_length})"
            return "bit varying"

    data_types = {
        'AutoField': 'integer AUTO_INCREMENT',
        'BigAutoField': 'bigint AUTO_INCREMENT',
        'BinaryField': VarbitStr(),
        'BooleanField': 'short',
        'CharField': 'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'datetime',
        'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'bigint',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'char(15)',
        'GenericIPAddressField': 'char(39)',
        'JSONField': 'json',
        'NullBooleanField': 'short',
        'OneToOneField': 'integer',
        'PositiveBigIntegerField': 'bigint',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallAutoField': 'smallint AUTO_INCREMENT',
        'SmallIntegerField': 'smallint',
        'TextField': 'string',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }

    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation

    Database = Database


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._db_version = None

    def get_connection_params(self):
        settings_dict = self.settings_dict

        # Construct datasource name
        dsn = "CUBRID"
        if settings_dict['HOST'].startswith('/'):
            dsn += ':' + settings_dict['HOST']
        elif settings_dict['HOST']:
            dsn += ':' + settings_dict['HOST']
        else:
            dsn += ':localhost'
        if 'PORT' in settings_dict:
            dsn += ':' + settings_dict['PORT']
        if 'NAME' in settings_dict:
            dsn += ':' + settings_dict['NAME']
        dsn += ':::'
        kwargs = {'dsn': dsn}

        # Set username and password, if provided
        if 'USER' in settings_dict:
            kwargs['user'] = settings_dict['USER']
        if 'PASSWORD' in settings_dict:
            kwargs['password'] = settings_dict['PASSWORD']

        return kwargs

    @async_unsafe
    def get_new_connection(self, conn_params):
        return Database.connect(**conn_params)

    def _valid_connection(self):
        return self.connection is not None

    def init_connection_state(self):
        pass

    @async_unsafe
    def create_cursor(self, name=None):
        if not self._valid_connection():
            self.connection = self.get_new_connection(None)
            connection_created.send(sender=self.__class__, connection=self)

        cursor = CursorWrapper(self.connection.cursor())
        return cursor

    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit

    def is_usable(self):
        try:
            return bool(self.connection.ping())
        except Database.Error:
            return False

    def get_database_version(self):
        if self._db_version:
            return self._db_version

        if not self._valid_connection():
            self.connection = self.get_new_connection(None)
        version_str = self.connection.server_version()
        if not version_str:
            raise Database.InterfaceError('Unable to determine CUBRID version string')

        match = db_version_re.match(version_str)
        if not match:
            raise ValueError(
                f"Unable to determine CUBRID version from version string '{version_str}'"
            )

        self._db_version = tuple(int(x) for x in match.groups())
        return self._db_version

    def _savepoint_commit(self, sid):
        # CUBRID does not support "RELEASE SAVEPOINT xxx"
        pass
