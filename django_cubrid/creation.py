"""
Database Creation Module for CUBRID Backend in Django

This module is part of the Django backend for CUBRID databases and is responsible for
handling the creation, modification, and deletion of database schemas. It includes
classes and methods that facilitate the management of database tables, indexes, and
other schema elements in alignment with the Django ORM models.

Key Components:
- DatabaseCreation: A class that is tailored to manage database creation and schema
  operations for the CUBRID database. It includes methods for creating tables,
  setting up indexes, and handling other database-specific schema tasks.
- Supporting functions: These functions provide additional utilities for managing
  database schemas, such as applying migrations or custom SQL scripts.

This module is typically used internally by Django during the process of migrating
database schemas, reflecting model changes in the database structure. It ensures
that the Django models are accurately and efficiently represented in the CUBRID
database schema.

Usage:
This module is not usually directly used by Django developers. Instead, it is
utilized by Django's migration and management commands.

Note:
- Understanding of Django's ORM and migration system, as well as familiarity with
  CUBRID's database schema capabilities, is essential for customizing or extending
  the functionalities of this module.
- This module is specific to the Django backend for CUBRID and may contain
  CUBRID-specific implementations and considerations.
"""
import os
import sys
import time
import subprocess

from django.db.backends.base.creation import BaseDatabaseCreation


def _database_exists(db_name):
    """Check if a CUBRID database exists"""
    # Read the CUBRID databases.txt file
    db_path = os.environ['CUBRID_DATABASES']
    db_path = os.path.join(db_path, "databases.txt")
    with open(db_path, "r", encoding='ascii') as f:
        db_lines = f.readlines()

    # Skip the header line
    db_lines = db_lines[1:]

    # Keep only the first column, to get the list of database names
    db_lines = list(map(lambda l: l.split()[0], db_lines))

    # Return if db_name is in the list of database names
    return db_name in db_lines

def _is_server_running(db_name):
    """Return whether the CUBRID server is running"""
    cp = subprocess.run(["cubrid", "server", "status"],
        capture_output = True, check = True, text = True)
    return f'Server {db_name}' in cp.stdout

def _check_consistency(db_name):
    """Checks the consistency of a database"""
    try:
        subprocess.run(["cubrid", "checkdb", db_name], check = True)
        return True
    except subprocess.CalledProcessError:
        return False

def _get_create_command(db_name):
    """Get the command to create a new CUBRID database"""
    return ["cubrid", "createdb", "--db-volume-size=20M",
            "--log-volume-size=20M", db_name, "en_US.utf8"]

def _get_copy_command(source_db_name, dest_db_name):
    """Get the command to create a new CUBRID database"""
    return ["cubrid", "copydb", "--replace", source_db_name, dest_db_name]

def _get_start_command(db_name):
    """Get the command to start the CUBRID server for a given database"""
    return ["cubrid", "server", "start", db_name]

def _get_stop_command(db_name):
    """Get the command to stop the CUBRID server for a given database"""
    return ["cubrid", "server", "stop", db_name]

def _get_delete_command(db_name):
    """Get the command to delete a CUBRID database"""
    return ["cubrid", "deletedb", db_name]


class DatabaseCreation(BaseDatabaseCreation):
    """
    Database creation class for CUBRID database in Django.

    This class extends Django's BaseDatabaseCreation and is responsible for managing
    the creation and destruction of the test database in a CUBRID environment. It
    provides CUBRID-specific implementations for setting up and tearing down the test
    database, ensuring compatibility with CUBRID's database management commands.

    Methods:
    _create_test_db: Creates the test database using CUBRID commands.
    _destroy_test_db: Destroys the test database, cleaning up any resources.
    """

    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        """
        Creates the test database for CUBRID.

        This internal method sets up the test database by executing CUBRID commands
        to create, start, and check the database. It handles the case where a test
        database may already exist and provides an option to recreate it if necessary.

        Parameters:
        verbosity (int): The verbosity level.
        autoclobber (bool): Whether to automatically overwrite the existing test database
                            without confirmation.
        keepdb (bool): If True, keeps the existing database if it already exists. Defaults
                    to False.

        Returns:
        str: The name of the test database that was created or found.

        If 'keepdb' is True and the test database already exists, this method checks the
        database and returns its name. If the database does not exist or 'keepdb' is False,
        it proceeds to create and start a new test database. If an error occurs during
        database creation and 'autoclobber' is False, it prompts the user to confirm
        deletion and recreation of the test database.
        """
        test_database_name = self._get_test_db_name()
        display_name = self._get_database_display_str(verbosity, test_database_name)

        # Check if the test database already exists
        database_exists = _database_exists(test_database_name)
        self.log(f"Database exists for alias {display_name}: {database_exists}")

        # Check whether the CUBRID server is started for this database
        is_server_running = _is_server_running(test_database_name) if database_exists else False
        self.log(f"Database server running for alias {display_name}: {is_server_running}")

        def create_db():
            self.log(f"Creating new database: {test_database_name}")
            cp = subprocess.run(_get_create_command(test_database_name),
                capture_output = True, check = False, text = True)
            self.log(cp.stdout)
            self.log(cp.stderr)
            cp.check_returncode()

        def destroy_db():
            if not autoclobber:
                confirm = input(f"Type 'yes' if you would like to try deleting the test "
                                f"database '{test_database_name}', or 'no' to cancel: ")
            if autoclobber or confirm == 'yes':
                self.log(f"Destroying old database for alias {display_name}: {test_database_name}")
                if is_server_running:
                    subprocess.run(_get_stop_command(test_database_name), check = True)
                subprocess.run(_get_delete_command(test_database_name), check = True)
            else:
                self.log("Tests cancelled.")
                sys.exit(1)

        def start_server():
            self.log("Starting the CUBRID server")
            subprocess.run(_get_start_command(test_database_name), check = True)

        if database_exists:
            if keepdb:
                if not is_server_running:
                    start_server()
                    is_server_running = True

                # Check the consistency of the database
                if _check_consistency(test_database_name):
                    return test_database_name

                self.log("Database consistency check failed")
                destroy_db()
            else:
                self.log(f"Cannot use old database for alias {display_name}")
                destroy_db()

        # The database does not exist, or the old one has been destroyed
        # Create a new test database and start the CUBRID server for it
        create_db()
        start_server()

        return test_database_name

    def _destroy_test_db(self, test_database_name, verbosity):
        """
        Removes the test database for CUBRID.

        This internal method is responsible for destroying the test database created
        during testing. It stops the CUBRID server and deletes the test database using
        CUBRID commands.

        Parameters:
        test_database_name (str): The name of the test database to be destroyed.
        verbosity (int): The verbosity level.

        The method attempts to stop the server and delete the test database. If an error
        occurs during this process, it logs the error. Finally, it closes the database
        connection.
        """
        time.sleep(1) # To avoid "database is being accessed by other users" errors.
        try:
            subprocess.run(_get_stop_command(test_database_name), check = True)
            subprocess.run(_get_delete_command(test_database_name), check = True)
        except subprocess.CalledProcessError as e:
            self.log(f"Error destroying the test database: {e}")
        finally:
            self.connection.close()

    def _clone_test_db(self, suffix, verbosity, keepdb=False):
        source_database_name = self.connection.settings_dict["NAME"]
        target_database_name = self.get_test_db_clone_settings(suffix)["NAME"]
        display_name = self._get_database_display_str(verbosity, source_database_name)

        # Check if the target database already exists
        database_exists = _database_exists(target_database_name)

        # Check whether the CUBRID server is started for this database
        is_server_running = _is_server_running(target_database_name) if database_exists else False

        def copy_db():
            self.log(f"Cloning database for alias {display_name} to {target_database_name}")
            cp = subprocess.run(_get_copy_command(source_database_name, target_database_name),
                check = False, capture_output = True, text = True)
            self.log(cp.stdout)
            self.log(cp.stderr)
            cp.check_returncode()

        if database_exists:
            if keepdb:
                if not is_server_running:
                    subprocess.run(_get_start_command(target_database_name), check = True)
                    is_server_running = True

                # Check the consistency of the database
                if _check_consistency(target_database_name):
                    return

                self.log(f"Database consistency check failed for alias {display_name}")

        # The CUBRID servers need to be stopped for copy_db to be possible
        subprocess.run(_get_stop_command(source_database_name), check = False)
        if is_server_running:
            subprocess.run(_get_stop_command(target_database_name), check = True)

        # The target database does not exist, or the old one has been destroyed
        # Clone the existing database and restart the CUBRID server for both
        copy_db()
        subprocess.run(_get_start_command(source_database_name), check = True)
        subprocess.run(_get_start_command(target_database_name), check = True)
