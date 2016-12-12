#!/usr/bin/python

# Copyright (c) 2016, Robert Escriva, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of Consus nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

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
