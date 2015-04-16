try:
    from setuptools import setup
except:
    from distutils.core import setup

# Workaround for nose bug from: http://bugs.python.org/issue15881
try:
    import multiprocessing
except ImportError:
    pass

from distutils.core import setup

setup(name='mortar-luigi',
      version='0.1.0',
      description='Mortar Extensions for Luigi',
      long_description='Mortar Extensions for Luigi',
      author='Mortar Data',
      author_email='info@mortardata.com',
      url='http://github.com/mortardata/mortar-luigi',
      namespace_packages = [
          'mortar'
      ],
      packages=[
          'mortar.luigi'
      ],
      license='LICENSE.txt',
      install_requires=[
          'luigi',
          'requests',
          'boto==2.24.0',
          'pymongo>=2.5',
          'mortar-api-python>=0.2.4'
      ],
      test_suite="nose.collector",
      tests_require=[
          'mock',
          'six',
          'moto==0.3.9',
          'nose>=1.3.0'
      ]
)
