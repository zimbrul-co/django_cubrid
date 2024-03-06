"""
This module defines the `DatabaseIntrospection` class for the Django-CUBRID backend,
extending Django's `BaseDatabaseIntrospection`. It provides functionality for
introspecting the schema of a CUBRID database from a Django application.

The `DatabaseIntrospection` class includes a mapping of CUBRID database field types
to Django field types, enabling accurate interpretation and representation of database
schemas within Django models. The module facilitates the extraction of database
metadata such as tables, columns, indexes, and other relevant information, which is
crucial for Django's ORM to interact effectively with a CUBRID database.

This module is an integral part of the Django-CUBRID backend, ensuring compatibility
and seamless integration of CUBRID databases within Django projects.

Classes:
    DatabaseIntrospection: Extends Django's BaseDatabaseIntrospection to provide
    introspection capabilities specific to CUBRID databases.

Dependencies:
    - Django's base introspection classes and methods
    - cubrid_db for CUBRID database field type definitions
"""
from collections import namedtuple

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection,
    FieldInfo as BaseFieldInfo,
    TableInfo as BaseTableInfo,
)
from django.db.models.indexes import Index
from django.utils.encoding import force_str

from cubrid_db import field_type


VARCHAR_MAXLEN = 1073741823


FieldInfo = namedtuple("FieldInfo",
    BaseFieldInfo._fields + ("comment",),
)
InfoLine = namedtuple('InfoLine', [
    'col_name', 'attr_type', 'data_type', 'prec', 'scale', 'is_nullable',
    'default_value', 'def_order', 'collation', 'comment', 'is_system_class',
    'class_type', 'partitioned', 'owner_name', 'is_reuse_old_class',
])
TableInfo = namedtuple("TableInfo",
    BaseTableInfo._fields + ("comment",),
)


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """
    The DatabaseIntrospection class in the Django-CUBRID backend extends Django's
    BaseDatabaseIntrospection, providing specialized introspection functionalities
    for CUBRID databases.

    This class implements the necessary methods and properties to retrieve metadata
    about the structure of a CUBRID database. It facilitates the mapping of CUBRID
    database field types to Django field types, enabling the Django ORM to accurately
    represent the database schema in models.

    Attributes:
        data_types_reverse (dict): A dictionary mapping CUBRID field types to Django
        field types. This mapping is essential for translating CUBRID schema information
        into a format understandable by Django's ORM.

    Methods:
        get_field_type(data_type, description): Get Django field type from db column type
        get_table_list(cursor): Retrieves a list of table names in the database.
        get_table_description(cursor, table_name): Provides a description of the
        specified table.
        get_relations(cursor, table_name): Retrieves information about the relationships
        between tables.
        get_key_columns(cursor, table_name): Returns a list of foreign key columns for
        the specified table.
        get_indexes(cursor, table_name): Gathers information about indexes on the
        specified table.
        get_constraints(cursor, table_name): Provides details on constraints for the
        given table.

    This class plays a crucial role in enabling Django applications to interact
    with CUBRID databases, ensuring that database operations and model definitions
    are correctly aligned with the underlying database schema.
    """

    data_types_reverse = {
        field_type.BIT: 'BinaryField',
        field_type.VARBIT: 'BinaryField',
        field_type.CHAR: 'CharField',
        field_type.NCHAR: 'CharField',
        field_type.NUMERIC: 'DecimalField',
        field_type.INT: 'IntegerField',
        field_type.SMALLINT: 'SmallIntegerField',
        field_type.BIGINT: 'BigIntegerField',
        field_type.FLOAT: 'FloatField',
        field_type.DOUBLE: 'FloatField',
        field_type.DATE: 'DateField',
        field_type.TIME: 'TimeField',
        field_type.TIMESTAMP: 'DateTimeField',
        field_type.DATETIME: 'DateTimeField',
        field_type.SET: 'TextField',
        field_type.MULTISET: 'TextField',
        field_type.SEQUENCE: 'TextField',
        field_type.JSON: 'JSONField',
    }

    def get_field_type(self, data_type, description):
        """
        Use the cursor description to match a Django field type to a database column,
        in case the data_type is insufficient.
        """
        if data_type in [field_type.VARCHAR, field_type.VARNCHAR]:
            return 'CharField' if description.internal_size < VARCHAR_MAXLEN else 'TextField'

        return super().get_field_type(data_type, description)

    def get_table_list(self, cursor):
        """Returns a list of tables in the current database."""
        cursor.execute("SELECT class_name, class_type, comment FROM db_class "
            "WHERE is_system_class != 1")
        return [TableInfo(r[0], {0: 't', 1: 'v'}.get(r[1]), r[2])
                for r in cursor.fetchall()]

    def identifier_converter(self, name):
        """Identifiers are case insensitive under CUBRID"""
        return name.lower()

    def get_table_description(self, cursor, table_name):
        """Returns a description of the table, with the DB-API cursor.description interface."""

        # Get accurate information with this query (taken from cubridmanager)
        cursor.execute("""
            SELECT a.attr_name, a.attr_type, a.data_type, a.prec, a.scale, a.is_nullable,
            a.default_value, a.def_order, a.collation, a.comment,
            c.is_system_class, c.class_type, c.partitioned,
            c.owner_name, c.is_reuse_oid_class
            FROM db_attribute a, db_class c
            WHERE c.class_name=a.class_name AND c.class_name = ?
            ORDER BY a.class_name, a.def_order;""", [table_name])
        field_info = {line[0]: InfoLine(*line) for line in cursor.fetchall()}

        cursor.execute(f"SELECT * FROM {self.connection.ops.quote_name(table_name)} LIMIT 1")

        fields = []
        for line in cursor.description:
            info = field_info[line[0]]
            fields.append(FieldInfo(
                force_str(line[0]),         # name
                line[1],                    # type
                info.prec,                  # display_size - use precision value
                info.prec,                  # internal size - use precision value
                info.prec,                  # precision
                info.scale,                 # scale
                info.is_nullable == "YES",  # null_ok
                info.default_value,         # default
                info.collation,             # collation
                info.comment,               # comment
            ))
        return fields

    def get_relations(self, cursor, table_name):
        """
        Return a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all foreign keys in the given table.
        """
        cursor.execute("SELECT index_name FROM db_index "
            "WHERE class_of.class_name = ? AND is_foreign_key = 1",
            [table_name]
        )
        fk_names = [r[0] for r in cursor.fetchall()]

        constraints = self.get_constraints(cursor, table_name)
        relations = {}
        for fk_name in fk_names:
            attrs = constraints[fk_name]
            field_name = attrs['columns'][0]
            other_table = attrs['foreign_key'][0]
            field_name_other_table = attrs['foreign_key'][1]
            relations[field_name] = (field_name_other_table, other_table)

        return relations

    def get_sequences(self, cursor, table_name, table_fields=()):
        """
        Retrieves a list of sequences associated with a given table in the CUBRID database.
        """
        cursor.execute("SELECT name, att_name, current_val FROM db_serial "
            "WHERE class_name = ?", [table_name.lower()])
        row = cursor.fetchone()
        if row is None:
            return []

        sequence_name, column_name, value = row
        value = int(value) # convert from Decimal to int

        # Ensure the value for the sequence will be greater or equal to max id
        qn = self.connection.ops.quote_name
        cursor.execute(f"SELECT MAX({qn(column_name)}) FROM {qn(table_name)}")
        max_id = cursor.fetchone()[0]
        value = max(value, max_id or 0)

        return [{
            'table': table_name,
            'column': column_name,
            'name': sequence_name,
            'value': value, # used for sequence_reset
        }]

    def get_indexes(self, cursor, table_name):
        """
        Retrieves a dictionary of indexes defined in a specified table in a CUBRID database.

        This method executes a SQL query to fetch index information from the database. It
        joins the 'db_index_key' and 'db_index' tables to obtain details about each index,
        focusing on those where 'key_order' is 0 and 'key_count' is 1, which indicates
        single-column indexes.

        The method filters indexes based on the provided table name and returns a dictionary
        where each key is the name of a column that is indexed, and the value is a dictionary
        specifying whether this index is a primary key and whether it is unique.

        Parameters:
            cursor (Cursor): The database cursor used to execute the query.
            table_name (str): The name of the table for which to retrieve index information.

        Returns:
            dict: A dictionary where each key is a column name and the value is another
            dictionary with two keys: 'primary_key' (boolean indicating if the index is
            a primary key) and 'unique' (boolean indicating if the index is unique).

        Example:
            If a table has an index on column 'id' as a primary key and unique, and another
            index on column 'name' which is not a primary key but is unique, the method
            will return:
            {
                'id': {'primary_key': True, 'unique': True},
                'name': {'primary_key': False, 'unique': True}
            }

        Note:
            This method only considers single-column indexes and assumes that 'key_order' of 0
            and 'key_count' of 1 are indicators of such indexes in the CUBRID database schema.
        """
        cursor.execute("""
            SELECT db_index_key.key_attr_name, db_index.is_primary_key, db_index.is_unique
            FROM db_index_key, db_index
            WHERE db_index_key.class_name = ?
              AND db_index.class_name = ?
              AND db_index_key.key_order = 0
              AND db_index_key.index_name = db_index.index_name
              AND db_index.key_count = 1;""", [table_name, table_name])
        rows = list(cursor.fetchall())
        indexes = {}
        for row in rows:
            indexes[row[0]] = {'primary_key': (row[1] == 'YES'), 'unique': (row[2] == 'YES')}

        return indexes

    def get_constraints(self, cursor, table_name):
        """
        Retrieves a dictionary of constraints for a specified table in a CUBRID database.

        This method fetches the 'CREATE TABLE' statement for the given table and parses it
        to extract information about various types of constraints, including primary keys,
        unique constraints, foreign keys, and indexes. It employs several helper functions
        to parse different parts of the SQL statement and extract relevant details.

        The method returns a dictionary where each key is the name of a constraint, and the
        value is another dictionary describing the type of constraint (primary key, unique,
        foreign key, check, index) along with other attributes like column names and, in the
        case of foreign keys, reference table and column.

        Parameters:
            cursor (Cursor): The database cursor used to execute the query.
            table_name (str): The name of the table for which to retrieve constraint information.

        Returns:
            dict: A dictionary where each key is a constraint name and the value is a
            dictionary describing the constraint. The description includes the columns
            involved in the constraint, and boolean flags indicating the type of the
            constraint (e.g., 'primary_key', 'unique', 'foreign_key', 'check', 'index').
            For foreign keys, additional details like the reference table and column are
            also included.

        Example:
            If a table has a primary key constraint on the 'id' column, a unique constraint
            on the 'email' column, and a foreign key constraint referencing the 'department_id'
            column of the 'department' table, the method will return something like:
            {
                'constraint_name1': {'columns': ['id'], 'primary_key': True, 'unique': False, ...},
                'constraint_name2': {
                    'columns': ['email'], 'primary_key': False, 'unique': True, ...},
                'constraint_name3': {
                    'columns': ['department_id'], 'foreign_key': ('department', 'id'), ...}
            }

        Note:
            This method assumes a specific format for the 'CREATE TABLE' statement in CUBRID
            databases and may not correctly interpret constraints if the format differs from
            the expected standard.
        """
        # pylint: disable=too-many-statements

        def parse_create_table_stmt(stmt):
            i = 0
            while i < len(stmt):
                i_constraint = stmt.find('CONSTRAINT', i + 1)
                i_index = stmt.find('INDEX', i + 1)

                if i_constraint == -1 and i_index == -1:
                    yield stmt[i:]
                    return

                if i_constraint == -1:
                    i1 = i_index
                elif i_index == -1:
                    i1 = i_constraint
                else:
                    i1 = min(i_constraint, i_index)

                yield stmt[i:i1]
                i = i1

        def parse_columns_sql(columns_sql):
            return [column[1:-1] for column in columns_sql.split(", ")]

        def parse_constraint_sql(sql):
            name_i0 = sql.index('[')
            name_i1 = sql.index(']', name_i0)
            name = sql[name_i0 + 1 : name_i1]

            sql = sql[name_i1 + 1:]
            kind_i1 = sql.index('(')
            kind = sql[:kind_i1].strip()

            sql = sql[kind_i1 + 1:]
            columns_i1 = sql.index(')')

            attrs = {
                'columns': parse_columns_sql(sql[:columns_i1].strip()),
                'primary_key': kind == "PRIMARY KEY",
                'unique': kind == "UNIQUE KEY",
                'foreign_key': None,
                'check': False,
                'index': False,
            }

            if kind == "FOREIGN KEY":
                sql = sql[columns_i1 + 1:]
                assert sql.strip().startswith("REFERENCES")
                ref_name_i0 = sql.index('[')
                ref_name_i1 = sql.index(']')
                ref_name = sql[ref_name_i0 + 1 : ref_name_i1].split('.')[-1]

                sql = sql[ref_name_i1 + 1:]
                ref_columns_i0 = sql.index('(')
                ref_columns_i1 = sql.index(')')
                ref_columns_sql = sql[ref_columns_i0 + 1 : ref_columns_i1]
                ref_columns = parse_columns_sql(ref_columns_sql)
                assert len(ref_columns) == 1

                attrs['foreign_key'] = (ref_name, ref_columns[0])

            return name, attrs

        def parse_index_columns_sql(columns_sql):
            columns, orders = [], []
            for element in map(lambda s: s.strip(), columns_sql.split(',')):
                if ' ' in element:
                    name, order = element.split(' ')
                    columns.append(name.strip(' []'))
                    orders.append(order.strip())
                else:
                    columns.append(element.strip(' []'))
                    orders.append('ASC')
            return columns, orders

        def parse_index_sql(sql):
            name = sql.split('[')[1].split(']')[0]
            columns_sql = sql.split('(')[1].split(')')[0]
            columns, orders = parse_index_columns_sql(columns_sql)

            attrs = {
                'columns': columns,
                'primary_key': False,
                'unique': False,
                'foreign_key': None,
                'check': False,
                'index': True,
                'orders': orders,
                'type': Index.suffix,
            }

            return name, attrs

        query = f"SHOW CREATE TABLE {self.connection.ops.quote_name(table_name)}"
        cursor.execute(query)
        _, stmt = cursor.fetchone()

        constraints = {}
        l = list(parse_create_table_stmt(stmt))
        for sql in l:
            if sql.startswith("CONSTRAINT"):
                name, attrs = parse_constraint_sql(sql)
            elif sql.startswith("INDEX"):
                name, attrs = parse_index_sql(sql)
            else:
                name = None

            if name is not None:
                constraints[name] = attrs

        return constraints
