import consus

c = consus.Client()

t = c.begin_transaction()
assert t.get('the table', 'the key') is None
t.commit()

t = c.begin_transaction()
assert t.put('the table', 'the key', 'the value')
t.commit()

t = c.begin_transaction()
assert t.get('the table', 'the key') == 'the value'
t.commit()
