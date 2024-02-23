django_cubrid
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
* Python >= 3.9
* Django >= 4.2
* cubrid_db >= 0.7.1

Build instructions
------------------

Build:

```
$ python setup.py build
```

Install:

```
$ sudo python setup.py install
```


Django configuration
--------------------

Configure the DATABASES part in your Django settings.py module:

```
    DATABASES = {
        'default': {
            'ENGINE': 'django_cubrid',       # The backend name: django_cubrid
            'NAME': 'demodb',                # CUBRID Database name
            'USER': 'public',                # User to access CUBRID
            'PASSWORD': '',                  # Password to access CUBRID.
            'HOST': '',                      # Set to empty string for localhost.
            'PORT': '33000',                 # Set to empty string for default.
        }
    }
```

For more information on using and configuring this backend, refer to the Django
documentation.

Note:
- This backend is community-driven and is not officially part of the Django project.
- Always ensure compatibility between your Django version and this backend.

Known issues
------------

* ForeignKey to_field feature does not work because CUBRID limitations:
CUBRID does not allow foreign key to reference non-primary key;
* Does not support microsecond precision, CUBRID has millisecond precision;
* Positive number constraints are not implemented by CUBRID;
* SQL order by ... exists is not supported by CUBRID;
* No math power operators are present in CUBRID SQL;
* Django loaddata command can fail in some cases with recursive relations, because
CUBRID does not support disabling constraint checks;
* CUBRID does not allow duplicate indexes;
* CUBRID does not allow auto increment on char field;
* CUBRID does not support removing the primary key;
* CUBRID does not implement SHA224, SHA256, SHA384, SHA512;
* CUBRID does not implement ISO year extraction;
* FTimeDelta does not work - many tests are failing, not yet fixed;
* Some other various Django tests are failing.

Testing
-------

Start CUBRID service:

```
$ cubrid service start
```

Clone Django patched and adapted for django_cubrid testing:

```
$ git clone git@github.com:zimbrul-co/django.git
$ git checkout cubrid-4.2
```

Create a new virtual environment for the tests:

```
$ python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -r tests/requirements/cubrid.txt
```

Test:

```
(venv) $ cd tests
(venv) $ python ./runtests.py --settings test_cubrid
```

License
-------

CUBRID is distributed under two licenses, Database engine is under GPL v2 or
later and the APIs are under BSD license.

The django_cubrid is under BSD license. See LICENSE file.
