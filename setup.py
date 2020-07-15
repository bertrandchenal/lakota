#!/usr/bin/env python
from setuptools import setup

#import baltic

long_description = '''
Baltic organise reads and writes through a changelog inspired by
Git (and by DVCSs in general). This changelog provides: historisation,
concurrency control and ease of synchronisation across different
storages.
'''

description = ('Versioned storage for data series')

setup(name='baltic',
      version='0.0',
      description=description,
      long_description=long_description,
      author='Bertrand Chenal',
      url='https://github.com/bertrandchenal/baltic',
      license='MIT',
      packages=['baltic'],
      install_requires=['numpy', 'numcodecs', 's3fs', 'numexpr'],
      # py_modules=['baltic'],
      entry_points={
          'console_scripts': [
              'baltic = baltic.cli:run',
          ],
      },
  )
