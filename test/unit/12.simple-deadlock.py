import time

import consus

c = consus.Client()

t1 = c.begin_transaction()
time.sleep(1)
t2 = c.begin_transaction()
time.sleep(1)
assert t2.put('table', 'key', 'v1')
time.sleep(1)
assert t1.put('table', 'key', 'v2')
time.sleep(1)

aborted = None
try:
    t2.commit()
    aborted = False
except consus.ConsusAbortedException:
    aborted = True
t1.commit()
assert aborted
