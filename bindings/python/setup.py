#!/usr/bin/python

import subprocess

from setuptools import setup, find_packages, Extension

def silent_fail(args):
    try:
        return subprocess.check_output(args).decode('utf8')
    except subprocess.CalledProcessError:
        return ''

deps = ('libconsus',)
includes = silent_fail(('pkg-config', '--cflags-only-I') + deps)
includes = [x.strip() for x in includes.split('-I') if x.strip()]
library_dirs = silent_fail(('pkg-config', '--libs-only-L') + deps)
library_dirs = [x.strip() for x in library_dirs.split('-L') if x.strip()]
libraries = silent_fail(('pkg-config', '--libs-only-l') + deps)
libraries = [x.strip() for x in libraries.split('-l') if x.strip()]

consus = Extension('consus',
                 sources=['consus.c'],
                 include_dirs=includes,
                 library_dirs=library_dirs,
                 runtime_library_dirs=library_dirs,
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
