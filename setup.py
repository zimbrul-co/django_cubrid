"""Setup script for the django_cubrid Python package."""

from setuptools import setup, find_packages

with open('django_cubrid/__init__.py', "r", encoding='utf-8') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.split('=')[1].strip().strip("'").strip('"')
            break

with open('README.md', "r", encoding='utf-8') as readme_file:
    readme = readme_file.read()

setup(
    name='django_cubrid',
    version=version,
    description='A Django database backend for CUBRID',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Casian Andrei',
    author_email='casian@zco.ro',
    url='https://github.com/zimbrul-co/django_cubrid',
    packages=find_packages(),
    classifiers=[
        'Framework :: Django',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
    ],
    install_requires=[
        'Django>=4.2',
        'cubrid_db @ git+https://github.com/zimbrul-co/cubrid_db.git@master#egg=cubrid_db',
    ],
    python_requires='>=3.9',
    license='BSD-3-Clause',
)
