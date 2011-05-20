#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='cybercom',
      version='0.0',
      packages= find_packages(),
      install_requires = ['pymongo', 'psycopg2', 'iso8601', 'simplejson', 'sqlalchemy']      
)
