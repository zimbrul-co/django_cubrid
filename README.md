django_cubrid: Django backend for CUBRID Database
======================

Overview
--------
Django_cubrid is a comprehensive Django database backend for CUBRID, a powerful open-source
relational database management system optimized for web applications. This backend integrates
seamlessly with Django, offering full support for Django's ORM capabilities and transaction
management, while also providing efficient connection handling and compatibility with CUBRID
features and data types.

For more information about CUBRID, visit the web site:
http://www.cubrid.org

Features
--------
- Full ORM Support: Leverage Django's ORM system to work with CUBRID databases.
- Transaction Management: Support for Django's transaction handling to ensure data integrity.
- Efficient Connection Handling: Manage database connections efficiently for different
  deployment scenarios.
- CUBRID-specific Features: Utilize CUBRID's unique features and data types within Django.

Prerequisites
-------------
* Python
  Tested with Python >= 3.9

* Django
  Tested with Django >= 2.1

Build
-----
* When building the CUBRID Python driver, if the Python version meets the prerequisites,
the django_cubrid will be installed into Python library.


Configure
---------
Configure the DATABASES part in your setting.py like below:
```
    DATABASES = {
        'default': {
            'ENGINE': 'django_cubrid',       # The backend name: django_cubrid
            'NAME': 'demodb',                # CUBRID Database name: ie, demodb
            'USER': 'public',                # User to access CUBRID: ie, public
            'PASSWORD': '',                  # Password to access CUBRID.
            'HOST': '',                      # Set to empty string for localhost.
            'PORT': '33000',                 # Set to empty string for default.
        }
    }
```

For more information on using and configuring this backend, refer to the Django
documentation and the CUBRID database documentation.

Note:
- This backend is community-driven and is not officially part of the Django project.
- Always ensure compatibility between your Django version and this backend.

Known issues
------------

* The Django sqlflush maybe failed because of the foreign constraints between database
tables.

* After using the Django loaddata command, the insert SQL manipulation in the application
maybe failed, becuse of the auto field.

License
-------

CUBRID is distributed under two licenses, Database engine is under GPL v2 or
later and the APIs are under BSD license.

The django_cubrid is under BSD license.
