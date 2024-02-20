"""
SQL Compiler Module for CUBRID Database Backend in Django.

This module contains classes and functions responsible for compiling Django ORM
queries into SQL queries that are compatible with the CUBRID database. It extends
and customizes Django's default SQL compiler to accommodate the SQL syntax,
functions, and conventions specific to CUBRID.

The module includes classes like SQLCompiler, SQLInsertCompiler, SQLDeleteCompiler,
and SQLUpdateCompiler, each tailored to handle different aspects of SQL query
compilation. These compilers translate Django's QuerySet methods and expressions
into corresponding SQL statements, ensuring that complex queries, including
aggregations, joins, and subqueries, are executed correctly on a CUBRID database.

Key Features:
- Custom SQL query compilation: Adapts Django's query construction to CUBRID's SQL syntax.
- Support for complex queries: Handles advanced ORM features including joins, subqueries,
  and aggregation.
- Performance optimizations: Includes CUBRID-specific optimizations to enhance query
  performance.

This module plays a crucial role in the integration between Django's ORM and the CUBRID
database, acting as a bridge that translates high-level ORM queries into raw SQL queries
understood by CUBRID.

Note:
- This module is part of the Django-CUBRID database backend and is not intended to be
  used directly by application developers.
- Understanding of Django's ORM internals and CUBRID's SQL syntax is essential for
  modifying or extending this module.
"""
from django.core.exceptions import EmptyResultSet

#;pylint: disable=unused-import
from django.db.models.sql.compiler import (
    SQLCompiler,
    SQLInsertCompiler as BaseSQLInsertCompiler,
    SQLDeleteCompiler,
    SQLUpdateCompiler,
    SQLAggregateCompiler,
)
#;pylint: enable=unused-import

from django.db.models.expressions import (
    Case,
    Col,
    Combinable,
    NegatedExpression,
    Subquery,
    Value,
)
from django.db.models.fields import BooleanField
from django.db.models.lookups import Exact
from django.db.models.fields.json import (
    compile_json_path,
    ContainedBy,
    DataContains,
    HasKeyLookup,
    KeyTransform,
    KeyTransformIn,
)


def combinable__pow__(self, other):
    """Not supported by CUBRID"""
    raise NotImplementedError("CUBRID does not have a power operator")
def combinable__rpow__(self, other):
    """Not supported by CUBRID"""
    raise NotImplementedError("CUBRID does not have a power operator")

Combinable.__pow__ = combinable__pow__
Combinable.__rpow__ = combinable__rpow__
Combinable.BITXOR = "^"
Combinable.POW = ""


def boolean_field_get_db_prep_value(self, value, connection, prepared = False):
    """Use an adapter for the BooleanField value"""
    if not prepared:
        value = self.get_prep_value(value)
    return connection.ops.adapt_booleanfield_value(value)

BooleanField.get_db_prep_value = boolean_field_get_db_prep_value


def negated_expression_as_sql(self, compiler, connection):
    """
    The negated expression needs adaptations for CUBRID SQL, which does not
    use boolean values. If the negated expression is a column with bool value,
    or if the expression is a value of bool type, we need to compare with 0 or 1.
    """
    try:
        sql, params = super(NegatedExpression, self).as_sql(compiler, connection)
    except EmptyResultSet:
        return "1=1", ()

    if isinstance(self.expression, Col) and \
            isinstance(self.expression.output_field, BooleanField):
        return f"{sql}=0", params

    if isinstance(self.expression, Value) and isinstance(self.expression.value, bool):
        return (f"{sql}=1" if self.expression.value else f"{sql}=0"), params

    return f"NOT {sql}", params

NegatedExpression.as_sql = negated_expression_as_sql


def exact_as_sql(self, compiler, connection):
    """
    For the Exact lookup with boolean values or conditional expression results,
    use the super() implementation, which makes a suitable logical expression.
    * Column
    * Case expression
    * Subquery with SELECT with Column
    """
    if isinstance(self.lhs, (Case, Col)) or (isinstance(self.lhs, Subquery) and \
            self.lhs.query.select and isinstance(self.lhs.query.select[0], Col)):
        return super(Exact, self).as_sql(compiler, connection)
    return exact_as_sql_default_impl(self, compiler, connection)

exact_as_sql_default_impl = Exact.as_sql
Exact.as_sql = exact_as_sql


def json_data_contains_as_cubrid(self, compiler, connection):
    """For json data_contains, need to compare the result of JSON_CONTAINS with 1"""
    sql, params = self.as_sql(compiler, connection)
    sql = f"{sql}=1"
    return sql, params

setattr(DataContains, 'as_cubrid', json_data_contains_as_cubrid)


def json_contained_by_as_cubrid(self, compiler, connection):
    """For json contained_by, need to compare the result of JSON_CONTAINS with 1"""
    sql, params = self.as_sql(compiler, connection)
    sql = f"{sql}=1"
    return sql, params

setattr(ContainedBy, 'as_cubrid', json_contained_by_as_cubrid)


def json_key_transform_as_cubrid(self, compiler, connection):
    """For json key transform, set usage of JSON_EXTRACT function"""
    lhs, params, key_transforms = self.preprocess_lhs(compiler, connection)
    json_path = compile_json_path(key_transforms)
    return f"JSON_EXTRACT({lhs}, '{json_path}')", params

setattr(KeyTransform, 'as_cubrid', json_key_transform_as_cubrid)


def json_has_key_lookup_as_cubrid(self, compiler, connection):
    """For json has_key, set usage of JSON_CONTAINS_PATH function"""
    sql, params = self.as_sql(compiler, connection,
        template="JSON_CONTAINS_PATH(%s, 'one', %%s)=1",
    )
    return sql, params

setattr(HasKeyLookup, 'as_cubrid', json_has_key_lookup_as_cubrid)


def json_key_transform_in_resolve_expression_parameter(self, compiler, connection, sql, param):
    """Use the JSON_EXTRACT function, like the MySQL backend"""
    sql, params = super(KeyTransformIn, self).resolve_expression_parameter(
        compiler,
        connection,
        sql,
        param,
    )
    if not hasattr(param, "as_sql"):
        sql = "JSON_EXTRACT(%s, '$')"
    return sql, params

setattr(KeyTransformIn, 'resolve_expression_parameter',
        json_key_transform_in_resolve_expression_parameter)


class SQLInsertCompiler(BaseSQLInsertCompiler, SQLCompiler):
    """
    SQL Insert Compiler for the CUBRID Database Backend in Django.

    This class is a specialized version of Django's SQL insert compiler, tailored
    for use with the CUBRID database. It inherits from Django's standard
    SQLInsertCompiler as well as the custom SQLCompiler specifically designed
    for CUBRID. This inheritance structure allows the class to leverage the
    general insert compilation logic of Django while also applying any CUBRID-specific
    adaptations defined in SQLCompiler.

    The SQLInsertCompiler class is used by Django's ORM to compile insert statements
    into SQL queries that are compatible with the CUBRID database. This includes
    handling of batch inserts, handling of primary key values, and any other
    CUBRID-specific considerations for insert operations.
    """
    def execute_sql(self, returning_fields=None):
        """
        Custom implementation for the insert execute_sql.
        last_insert_id() does not work if the pk value is provided, and returns None.
        In that case, we need to use the pre-save value of the pk and put in in the
        returned tuple, so the model instance does not have the value set to None
        after saving.
        """
        rows = super().execute_sql(returning_fields)
        if not self.returning_fields:
            return rows

        assert len(rows) == 1

        pre_save_values = [self.pre_save_val(field, self.query.objs[0])
            for field in self.returning_fields]
        returning_values = list(rows[0])
        returning_values = [psv if rv is None else rv for psv, rv in
            zip(pre_save_values, returning_values)]
        return [tuple(returning_values)]
