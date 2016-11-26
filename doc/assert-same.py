import hashlib
import sys

SAME = [
        ('install/ubuntu14.04-src-prereqs',
            'install/ubuntu16.04-src-prereqs',
            'install/ubuntu16.10-src-prereqs'),
    ]

def sha256(f):
    return hashlib.sha256(open(f).read()).hexdigest()

for files in SAME:
    if len(files) == 0: continue
    ref = sha256(files[0])
    for f in files:
        if sha256(f) != ref:
            print f, 'does not match other files'
            sys.exit(1)
