import consus

c = consus.Client()
t = c.begin_transaction()
assert t.get('the table', 'the key') is None
t.commit()
