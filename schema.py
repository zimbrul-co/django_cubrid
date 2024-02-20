"""
This module provides Django backend support for CUBRID database operations,
particularly focusing on schema editing functionalities. It defines the
`DatabaseSchemaEditor` class, an extension of Django's `BaseDatabaseSchemaEditor`,
tailored to generate CUBRID-specific SQL statements for schema modifications.
These modifications include operations like table deletion, column modification,
and alteration of column constraints. The custom implementation ensures compatibility
and efficient interaction between Django models and the CUBRID database schema.
"""
import datetime

from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    """
    A Django database schema editor for CUBRID databases.

    This class extends Django's `BaseDatabaseSchemaEditor` to provide
    specialized schema editing capabilities for CUBRID databases. It overrides
    and specifies SQL statement templates for various schema operations such as
    deleting tables, dropping columns, and modifying column types and constraints.
    The customization is necessary to accommodate the SQL syntax and features
    specific to CUBRID, ensuring seamless schema manipulations within Django's ORM.
    """

    sql_delete_table = "DROP TABLE %(table)s CASCADE CONSTRAINTS"
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_alter_column_type = "MODIFY %(column)s %(type)s%(collation)s"
    sql_alter_column_null = "MODIFY %(column)s %(type)s NULL"
    sql_alter_column_not_null = "MODIFY %(column)s %(type)s NOT NULL"
    sql_alter_column_no_default = "MODIFY %(column)s %(type)s DEFAULT NULL"
    sql_alter_column_no_default_null = "ALTER COLUMN %(column)s SET DEFAULT NULL"

    sql_rename_column = "ALTER TABLE %(table)s CHANGE %(old_column)s %(new_column)s %(type)s"
    sql_delete_unique = "ALTER TABLE %(table)s DROP INDEX %(name)s"
    sql_create_fk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY "
        "(%(column)s) REFERENCES %(to_table)s (%(to_column)s)"
    )
    sql_create_column_inline_fk = (
        "CONSTRAINT %(name)s FOREIGN KEY REFERENCES %(to_table)s(%(to_column)s)"
    )
    sql_delete_fk = "ALTER TABLE %(table)s DROP FOREIGN KEY %(name)s"
    sql_delete_index = "DROP INDEX %(name)s ON %(table)s"
    sql_create_pk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s)"
    sql_delete_pk = "ALTER TABLE %(table)s DROP PRIMARY KEY"

    sql_alter_table_comment = "ALTER TABLE %(table)s COMMENT = %(comment)s"
    sql_alter_column_comment = "ALTER TABLE %(table)s COMMENT ON COLUMN %(column)s = %(comment)s"

    def quote_value(self, value):
        """
        Quotes a value for use in a SQL statement, adapting it to the CUBRID database format.

        This method takes a Python data type and converts it into a string representation
        suitable for inclusion in a SQL query, ensuring proper formatting and escaping
        as needed for the CUBRID database. It handles various data types like dates, times,
        strings, bytes, booleans, and other basic types.

        Parameters:
            value: The value to be quoted. Can be an instance of `datetime.date`,
                `datetime.time`, `datetime.datetime`, `str`, `bytes`, `bytearray`,
                `memoryview`, `bool`, or other basic data types.

        Returns:
            str: A string representation of the input value formatted as a literal
                suitable for SQL queries. Dates and times are returned in single quotes,
                strings are escaped and quoted, bytes are converted to hexadecimal format,
                booleans are represented as '1' or '0', and other types are converted
                to their string representation.

        Note:
            For string values, this method uses the `escape_string` method of the
            CUBRID database connection to handle escaping, ensuring that the value
            is safe to include in SQL queries.
        """
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return f"'{value}'"
        if isinstance(value, str):
            return f"'{self.connection.connection.escape_string(value)}'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return f"B'{value.hex().upper()}'"
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def prepare_default(self, value):
        return self.quote_value(value)

    def _comment_sql(self, comment):
        return f'COMMENT {self.quote_value(comment or "")}'

    def _alter_column_comment_sql(self, model, new_field, new_type, new_db_comment):
        """
        The base implementation uses _comment_sql and inserts an extra COMMENT keyword.
        Need to remove this extra keyword.
        Cannot keep the base _comment_sql() implementation because it fails to add COMMENT.
        This behavior can change in future Django versions, so this situation may not apply
        for Django > 4.2.
        """
        sql = super()._alter_column_comment_sql(model, new_field, new_type, new_db_comment)
        return (sql[0].replace('= COMMENT', '='), sql[1])

    def _collate_sql(self, collation, old_collation=None, table_name=None):
        return "COLLATE " + collation if collation else ""
