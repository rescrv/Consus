import consus

c = consus.Client()
t = c.begin_transaction()
assert t.put('the table', 'the key', 'the value')
t.commit()
