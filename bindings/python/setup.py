#!/usr/bin/python

import subprocess

from setuptools import setup, find_packages, Extension

deps = ('libconsus',)
includes = subprocess.check_output(('pkg-config', '--cflags-only-I') + deps).decode('utf8')
includes = [x.strip() for x in includes.split('-I') if x.strip()]
libraries = subprocess.check_output(('pkg-config', '--libs-only-l') + deps).decode('utf8')
libraries = [x.strip() for x in libraries.split('-l') if x.strip()]

consus = Extension('consus',
                 sources=['consus.c'],
                 include_dirs=includes,
                 libraries=libraries
                 )

setup(name = 'consus',
      version = '0.0.dev',
      description = 'Bindings for Consus, a transactional key-value store',
      url = 'http://consus.io/',
      author = 'Robert Escriva',
      author_email = 'robert@consus.io',
      license = 'BSD',
      ext_modules = [consus],
      )
