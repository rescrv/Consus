import time

import consus

c = consus.Client()

t1 = c.begin_transaction()
time.sleep(1)
t2 = c.begin_transaction()

assert t2.put('table', 'key', 'v1')
assert t1.put('table', 'key', 'v2')
aborted = None
try:
    t2.commit()
    aborted = False
except AssertionError:
    aborted = True
t1.commit()
