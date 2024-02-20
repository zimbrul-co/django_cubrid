"""
This module, operations.py, is part of a Django backend customized for CUBRID database
integration. It defines the `DatabaseOperations` class, which extends or modifies
Django's base database operations to ensure compatibility and optimized performance
with the CUBRID database system.

Key Features and Functionalities:
- `DatabaseOperations`: A class that inherits from Django's `BaseDatabaseOperations`.
  This class overrides and adds methods specific to the CUBRID database, handling various
  database operations such as data formatting, query construction, and schema management.
- CUBRID-specific Implementations: Includes custom methods and properties tailored for
  the CUBRID database, ensuring that Django's ORM can efficiently interact with CUBRID.
- Compatibility Layer: Acts as a compatibility layer between Django's ORM system and the
  CUBRID database, translating Django's standard operations into CUBRID-compatible queries
  and commands.
- Utility Functions: Provides utility functions for date and time formatting, string
  encoding, and regular expression operations, which are crucial for database operations
  in Django.

This module is a critical component of the Django-CUBRID backend, enabling seamless integration
and operation of Django applications with the CUBRID database.
"""
import uuid

from collections import deque

from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.encoding import force_str
from django.utils.regex_helper import _lazy_re_compile


class DatabaseOperations(BaseDatabaseOperations):
    """
    The DatabaseOperations class extends Django's BaseDatabaseOperations, providing
    support for the CUBRID database within the Django-CUBRID backend. This class
    facilitates interaction between Django applications and CUBRID databases.

    Attributes:
        compiler_module (str): Module for SQL compilation in CUBRID context.

    Key Functionalities:
    - Custom Database Operations: Adapts BaseDatabaseOperations methods for CUBRID.
    - SQL Query Formatting: Adjusts SQL syntax for CUBRID compatibility.
    - Data Type Handling: Manages data types as per CUBRID standards.
    - Schema Management: Tailors schema operations for CUBRID databases.
    - Performance Optimizations: Enhances efficiency for CUBRID interactions.

    Note:
    This class is part of the Django-CUBRID backend. It's optimized for CUBRID
    databases and works best with other components of this backend.
    """
    # pylint: disable=too-many-public-methods

    compiler_module = "django_cubrid.compiler"

    cast_data_types = {
        'AutoField': 'integer',
        'BigAutoField': 'bigint',
        'SmallAutoField': 'smallint',
    }
    cast_char_field_without_max_length = "varchar"

    _extract_format_re = _lazy_re_compile(r"[A-Z_]+")

    def format_for_duration_arithmetic(self, sql):
        """Do nothing since formatting is handled in the custom function."""
        return sql

    def date_extract_sql(self, lookup_type, sql, params):
        # https://www.cubrid.org/manual/en/10.1/sql/function/datetime_fn.html
        sql_dict = {
            "second": f"SECOND({sql})",
            "minute": f"MINUTE({sql})",
            "hour": f"HOUR({sql})",
            "day": f"DAY({sql})",
            "week_day": f"DAYOFWEEK({sql})",
            "iso_week_day": f"WEEKDAY({sql}) + 1",
            "week": f"WEEK({sql}, 3)",
            "month": f"MONTH({sql})",
            "quarter": f"QUARTER({sql})",
            "year": f"YEAR({sql})",
            "iso_year": f"YEAR({sql})",
        }

        if not lookup_type in sql_dict:
            raise NotImplementedError(f"Lookup type ({lookup_type}) is not implemented")

        return sql_dict[lookup_type], params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql_dict = {
            "day": f"TRUNC({sql}, 'dd')",
            "week": f"TRUNC({sql}, 'day')",
            "month": f"TRUNC({sql}, 'mm')",
            "quarter": f"TRUNC({sql}, 'q')",
            "year": f"TRUNC({sql}, 'yy')",
        }

        if not lookup_type in sql_dict:
            raise NotImplementedError(f"Lookup type ({lookup_type}) is not implemented")

        return sql_dict[lookup_type], params

    def datetime_cast_date_sql(self, sql, params, tzname):
        return f"DATE({sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        return f"TIME({sql})", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql_dict = {
            "second": f"EXTRACT(SECOND FROM {sql})",
            "minute": f"EXTRACT(MINUTE FROM {sql})",
            "hour": f"EXTRACT(HOUR FROM {sql})",
            "day": f"EXTRACT(DAY FROM {sql})",
            "week_day": f"EXTRACT(DAYOFWEEK FROM {sql})",
            "iso_week_day": f"EXTRACT(WEEKDAY FROM {sql}) + 1",
            "week": f"EXTRACT(WEEK FROM {sql}, 3)",
            "month": f"EXTRACT(MONTH FROM {sql})",
            "quarter": f"EXTRACT(QUARTER FROM {sql})",
            "year": f"EXTRACT(YEAR FROM {sql})",
            "iso_year": f"EXTRACT(YEAR FROM {sql})",
        }

        if not lookup_type in sql_dict:
            raise ValueError(f"Lookup type ({lookup_type}) is not implemented")

        return sql_dict[lookup_type], params

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        if lookup_type in ['year', 'quarter', 'month', 'week', 'day']:
            return self.date_trunc_sql(lookup_type, sql, params, tzname)

        fields = ['year', 'month', 'day', 'hour', 'minute', 'second', 'milisecond']
        dt_format = ('%Y-', '%m', '-%d', ' %H:', '%i', ':%s', '.%f')
        dt_format_defaults = ('0000-', '01', '-01', ' 00:', '00', ':00', '.000000')
        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            pass
        else:
            format_str = "".join(dt_format[:i] + dt_format_defaults[i:])
            return f"CAST(DATE_FORMAT({sql}, '{format_str}') AS DATETIME)", params
        return sql, params

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        fields = {
            "hour": "%H:00:00",
            "minute": "%H:%i:00",
            "second": "%H:%i:%s",
        }
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return f"CAST(TIME_FORMAT({sql}, '{format_str}') AS TIME)", params

        return f"TIME({sql})", params

    def force_no_ordering(self):
        return [(None, ("NULL", [], False))]

    def quote_name(self, name):
        # Check for characters that are not allowed even when quoted.
        if '[' in name or ']' in name or '.' in name:
            name = name.replace("[", "!SQBL!")
            name = name.replace("]", "!SQBR!")
            name = name.replace(".", "!DOT!")

        # Check if the name is already quoted.
        if name.startswith("`") and name.endswith("`"):
            return name
        if name.startswith('"') and name.endswith('"'):
            return name

        # Define a set of special characters as per CUBRID documentation.
        special_chars = set("() +-*/%||!<>=>|^&~")

        # Add conditions for special names that need double quotes.
        # This includes names with spaces, special characters, or non-ASCII characters.
        if any(char in name for char in special_chars) or \
                name.startswith("__") or name.endswith("__"):
            # Wrap the name in double quotes for CUBRID compatibility.
            return f'"{name}"'

        # For regular names, wrap them in backticks.
        return f"`{name}`"

    def regex_lookup(self, lookup_type):
        if lookup_type == "regex":
            return "%s REGEXP BINARY %s"
        return "%s REGEXP %s"

    def no_limit_value(self):
        # 2**63 - 1
        return 9223372036854775807

    def limit_offset_sql(self, low_mark, high_mark):
        if high_mark is None:
            if low_mark:
                row_count = self.no_limit_value()
                return f'LIMIT {low_mark},{row_count}'
            return ''

        row_count = high_mark - low_mark
        if low_mark:
            return f'LIMIT {low_mark},{row_count}'
        return f'LIMIT {row_count}'

    def last_executed_query(self, cursor, sql, params):
        """
        Return a string of the query last executed by the given cursor, with
        placeholders replaced with actual values.
        """
        if not params:
            return sql

        assert not isinstance(params, dict)

        u_params = tuple(force_str(val, strings_only=True, errors="replace") for val in params)
        return sql % u_params

    def last_insert_id(self, cursor, table_name, pk_name):
        return self.connection.connection.get_last_insert_id()

    @staticmethod
    def _remove_relations_in_cycles(tables, relations):
        def find_cycles(relations):
            graph = {table: [] for table in tables}
            for table_from, table_to in relations:
                graph[table_from].append(table_to)

            color = {u: "WHITE" for u in tables}
            cycles = []

            def dfs_visit(u, path):
                color[u] = "GRAY"
                path.append(u)
                for v in graph[u]:
                    if color[v] == "GRAY":  # Cycle detected
                        cycle_start_index = path.index(v)
                        cycles.append(path[cycle_start_index:].copy())
                    elif color[v] == "WHITE":
                        dfs_visit(v, path)
                path.pop()
                color[u] = "BLACK"

            for u in tables:
                if color[u] == "WHITE":
                    dfs_visit(u, [])

            return cycles

        cycles = find_cycles(relations)
        relations_to_remove = {tuple(cycle[i:i+2]) for cycle in cycles for i in range(len(cycle)-1)}
        relations_to_remove.update({(cycle[-1], cycle[0]) for cycle in cycles})  # Close the cycles

        # Remove all relations that are part of any cycle
        filtered_relations = [r for r in relations if r not in relations_to_remove]

        return filtered_relations

    @staticmethod
    def _topological_sort(tables, relations):
        # Create a dictionary to keep the adjacency list
        adj_list = {table: [] for table in tables}
        # Create a dictionary to count the in-degrees (number of incoming edges)
        in_degree = {table: 0 for table in tables}

        # Build the adjacency list and calculate in-degree for each node
        for table_from, table_to in relations:
            adj_list[table_from].append(table_to)
            in_degree[table_to] += 1

        # Find all nodes with no initial dependencies (in-degree 0)
        queue = deque([table for table in in_degree if in_degree[table] == 0])

        sorted_list = []  # List to keep the topologically sorted order

        while queue:
            node = queue.popleft()
            sorted_list.append(node)

            # For each adjacent node, decrease in-degree and if it becomes 0, add it to the queue
            for adjacent in adj_list[node]:
                in_degree[adjacent] -= 1
                if in_degree[adjacent] == 0:
                    queue.append(adjacent)

        # Check if topological sorting is possible (detect cycles)
        if len(sorted_list) != len(tables):
            # Could not perform a complete topological sort (cycles detected)
            raise ValueError("Cycles detected in table dependencies, "
                "cannot perform topological sort.")

        return sorted_list

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []

        # Construct a list of all relations between the tables
        relations = []
        with self.connection.cursor() as cursor:
            for table_name in tables:
                table_rels = self.connection.introspection.get_relations(cursor, table_name)
                table_from = table_name
                relations.extend(((table_from, table_to)
                    for field_name, (other_field_name, table_to) in table_rels.items()
                    if table_to != table_from and table_to in tables))

        relations = list(set(relations)) # eliminate duplicates
        relations = self._remove_relations_in_cycles(tables, relations) # no cycles
        sorted_tables = self._topological_sort(tables, relations)

        # List of simple delete queries
        sql_list = []
        for table_name in sorted_tables:
            sql_delete = style.SQL_KEYWORD("DELETE")
            sql_from = style.SQL_KEYWORD("FROM")
            table_name = style.SQL_FIELD(self.quote_name(table_name))
            sql_list.append(f"{sql_delete} {sql_from} {table_name};")

        # Perform sequence reset if requested
        if reset_sequences:
            sql_list += self._sequence_reset_by_table_names_sql(style, sorted_tables, 1)

        return sql_list

    def _sequence_reset_sql(self, style, sequence_info, initial_value = None):
        if initial_value is None:
            initial_value = sequence_info['value'] + 1

        table_name_sql = style.SQL_TABLE(self.quote_name(sequence_info['table']))
        return (f"{style.SQL_KEYWORD('ALTER')} {style.SQL_KEYWORD('TABLE')} "
                f"{table_name_sql} {style.SQL_KEYWORD('AUTO_INCREMENT')} = {initial_value};"
        )

    def _sequence_reset_by_table_names_sql(self, style, table_names, initial_value = None):
        sql_list = []
        with self.connection.cursor() as cursor:
            for table_name in table_names:
                sequence_info = self.connection.introspection.get_sequences(
                    cursor, table_name)[0]
                sql = self._sequence_reset_sql(style, sequence_info, initial_value)
                sql_list.append(sql)
        return sql_list

    def sequence_reset_by_name_sql(self, style, sequences):
        return [
            self._sequence_reset_sql(style, sequence_info)
            for sequence_info in sequences
        ]

    def sequence_reset_sql(self, style, model_list):
        # pylint: disable=protected-access
        table_names = [model._meta.db_table for model in model_list]
        return self._sequence_reset_by_table_names_sql(style, table_names)

    def year_lookup_bounds(self, value):
        """
        Returns the start and end timestamps for a given year.

        This method is used to generate the bounds for filtering database records
        based on a year. It creates timestamps for the start and end of the year,
        excluding microseconds.

        Args:
            value (str): The year as a string.

        Returns:
            list: A list containing two strings, the start and end timestamps of the year.
        """
        first = '%s-01-01 00:00:00.00'
        second = '%s-12-31 23:59:59.99'
        return [first % value, second % value]

    def lookup_cast(self, lookup_type, internal_type=None):
        """
        Returns the SQL lookup cast for a given lookup type.

        This method adjusts the SQL query based on the lookup type. For case-insensitive
        lookups, it wraps the lookup in an UPPER function.

        Args:
            lookup_type (str): The type of lookup to perform.
            internal_type (str, optional): The internal type of the field. Defaults to None.

        Returns:
            str: The SQL lookup cast.
        """
        lookup = '%s'

        # Use UPPER(x) for case-insensitive lookups.
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            lookup = f'UPPER({lookup})'

        return lookup

    def max_name_length(self):
        """
        Returns the maximum length of database object names.

        This method specifies the maximum allowed length for names of database objects
        (like tables, columns, etc.) in the CUBRID database.

        Returns:
            int: The maximum name length.
        """
        return 64

    def bulk_insert_sql(self, fields, placeholder_rows):
        """
        Generates SQL for bulk insert operations.

        This method creates an SQL string for performing bulk insert operations. It formats
        the provided data into a suitable SQL 'VALUES' clause.

        Args:
            fields (list): The list of fields for the insert.
            placeholder_rows (list): A list of placeholder rows for the values to be inserted.

        Returns:
            str: The SQL string for the bulk insert operation.
        """
        # pylint: disable=unused-argument
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join(f"({sql})" for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def adapt_booleanfield_value(self, value):
        """
        Adapt Python True/False to 1 or 0 for CUBRID
        This method is called by BooleanField, patched for CUBRID in compiler.py
        See boolean_field_get_db_prep_value() from compiler.py
        """
        return 1 if value else 0

    def adapt_datefield_value(self, value):
        return value

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None
        # Expression values are adapted by the database.
        if hasattr(value, "resolve_expression"):
            return value

        return value

    def adapt_timefield_value(self, value):
        if value is None:
            return None
        # Expression values are adapted by the database.
        if hasattr(value, "resolve_expression"):
            return value

        if timezone.is_aware(value):
            raise ValueError("backend does not support timezone-aware times.")
        return value

    def get_db_converters(self, expression):
        """
        Returns a list of database value converters for a given field type.

        Based on the type of the field in a database expression, this method appends
        appropriate converter functions to the list of converters. These converters
        are used to adapt Python values from database-specific formats.

        Args:
            expression (Expression): The database field expression.

        Returns:
            list: A list of converter functions.
        """
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'TextField':
            converters.append(self.convert_textfield_value)
        elif internal_type in ['BooleanField', 'NullBooleanField']:
            converters.append(self.convert_booleanfield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif internal_type in ['IPAddressField', 'GenericIPAddressField']:
            converters.append(self.convert_ipaddress_value)
        return converters

    # The following methods are converters used in get_db_converters. They
    # take a value, an expression, and a connection as arguments, and return a converted value.

    # pylint: disable=unused-argument

    def convert_textfield_value(self, value, expression, connection):
        """
        Converts a text field value to a Python string.

        Args:
            value (str): The text value.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            str: The converted text value.
        """
        if value is not None:
            value = force_str(value)
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        """
        Converts a boolean field value to a Python boolean.

        Args:
            value (int): The boolean value (0 or 1).
            expression, connection: Unused, but required for interface consistency.

        Returns:
            bool: The converted boolean value.
        """
        if value in (0, 1):
            value = bool(value)
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        """
        Converts a UUID field value to a Python UUID object.

        Args:
            value (str): The UUID value as a string.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            UUID: The converted UUID value.
        """
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_ipaddress_value(self, value, expression, connection):
        """
        Converts an IP address field value to a Python string.

        Args:
            value (str): The IP address value.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            str: The converted IP address value.
        """
        if value is not None:
            value = value.strip()
        return value

    # pylint: enable=unused-argument
