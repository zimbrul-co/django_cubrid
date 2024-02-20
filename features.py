"""
Database Features Module for the CUBRID Database Backend in Django

This module defines the DatabaseFeatures class, which specifies the capabilities
and limitations of the CUBRID database in the context of Django's ORM.

The DatabaseFeatures class is a critical component of the Django database backend
architecture. It informs Django's ORM about the specific behaviors and
characteristics of the CUBRID database, allowing the ORM to adapt its operations
accordingly. This adaptation covers various aspects of database interactions,
including transaction handling, schema management, query formation, and more.

This module is intended for internal use by Django's ORM and database backend
system. It helps maintain the database-agnostic nature of Django by providing a
clear definition of the CUBRID database's capabilities.
"""
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.utils import InterfaceError
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    """
    Database feature flags for CUBRID Database Backend in Django.

    This class extends Django's BaseDatabaseFeatures to specify the database
    capabilities and features supported by the CUBRID database backend. Each attribute
    in this class represents a specific feature or capability of the CUBRID database,
    allowing Django to appropriately adapt its behavior and queries.

    This class is used internally by Django to determine how to implement certain
    ORM features based on the capabilities of the CUBRID database.
    """

    minimum_database_version = (10, 1)

    allow_sliced_subqueries_with_in = False

    allows_group_by_selected_pks = True

    atomic_transactions = False

    can_introspect_check_constraints = False

    can_rollback_ddl = True

    closed_cursor_error_class = InterfaceError

    has_bulk_insert = True

    has_select_for_update = True

    has_select_for_update_nowait = False

    has_zoneinfo_database = False

    related_fields_match_type = True

    requires_literal_defaults = True

    supports_boolean_expr_in_select_clause = False

    supports_column_check_constraints = False

    supports_comments = True

    supports_comments_inline = True

    supports_date_lookup_using_string = False

    supports_expression_indexes = False

    supports_forward_references = False

    supports_ignore_conflicts = False

    supports_microsecond_precision = False  # Removed from mainline Django, kept for tests

    supports_paramstyle_pyformat = False

    supports_partial_indexes = False

    supports_regex_backreferencing = False

    supports_select_for_update_with_limit = False

    supports_table_check_constraints = False

    supports_timezones = False

    supports_unspecified_pk = True

    time_cast_precision = 0

    @cached_property
    def introspected_field_types(self):
        """Specify how the field types are introspected with CUBRID"""
        return {
            **super().introspected_field_types,
            "AutoField": "IntegerField",
            "BigAutoField": "BigIntegerField",
            "BooleanField": "SmallIntegerField",
            "DurationField": "BigIntegerField",
            "SmallAutoField": "SmallIntegerField",
        }

    django_test_skips = {
        "CUBRID does not support disabling constraint checks": {
            "backends.base.test_creation.TestDeserializeDbFromString.test_circular_reference",
            "backends.base.test_creation.TestDeserializeDbFromString.test_self_reference",
            "backends.base.test_creation.TestDeserializeDbFromString."
            "test_circular_reference_with_natural_key",
            "backends.tests.FkConstraintsTests.test_disable_constraint_checks_manually",
            "backends.tests.FkConstraintsTests.test_disable_constraint_checks_context_manager",
            "backends.tests.FkConstraintsTests.test_check_constraints",
            "backends.tests.FkConstraintsTests.test_check_constraints_sql_keywords",
        },
        "CUBRID does not allow duplicate indexes": {
            "schema.tests.SchemaTests.test_add_inline_fk_index_update_data",
            "schema.tests.SchemaTests.test_remove_index_together_does_not_remove_meta_indexes",
        },
        "CUBRID does not allow auto increment on char field": {
            "schema.tests.SchemaTests.test_alter_auto_field_to_char_field",
        },
        "CUBRID does not support removing the primary key": {
            "schema.tests.SchemaTests.test_alter_not_unique_field_to_primary_key",
            "schema.tests.SchemaTests.test_primary_key",
        },
        "CUBRID does not allow foreign key to reference non-primary key": {
            "schema.tests.SchemaTests.test_rename_referenced_field",
            "model_fields.test_foreignkey.ForeignKeyTests.test_non_local_to_field",
        },
        "CUBRID cannot change attributes used in foreign keys": {
            "schema.tests.SchemaTests.test_alter_pk_with_self_referential_field"
        },
        "CUBRID does not implement ISO year extraction": {
            "db_functions.datetime.test_extract_trunc.DateFunctionTests.test_extract_iso_year_func",
            "db_functions.datetime.test_extract_trunc.DateFunctionTests."
            "test_extract_iso_year_func_boundaries",
        },
        "CUBRID does not implement SHA224, SHA256, SHA384, SHA512": {
            "db_functions.text.test_sha224.SHA224Tests.test_basic",
            "db_functions.text.test_sha224.SHA224Tests.test_transform",
            "db_functions.text.test_sha256.SHA256Tests.test_basic",
            "db_functions.text.test_sha256.SHA256Tests.test_transform",
            "db_functions.text.test_sha384.SHA384Tests.test_basic",
            "db_functions.text.test_sha384.SHA384Tests.test_transform",
            "db_functions.text.test_sha512.SHA512Tests.test_basic",
            "db_functions.text.test_sha512.SHA512Tests.test_transform",
        },
        "CUBRID does not have implement positive number constraints": {
            "model_fields.test_integerfield.PositiveIntegerFieldTests.test_negative_values",
        },
        "CUBRID does not support order by exists": {
            "expressions.tests.BasicExpressionsTests.test_order_by_exists",
        },
        "CUBRID does not have a power operator": {
            "expressions.tests.ExpressionOperatorTests.test_lefthand_power",
            "expressions.tests.ExpressionOperatorTests.test_righthand_power",
        },
    }
