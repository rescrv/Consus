import consus

c1 = consus.Client()
t1 = c1.begin_transaction()
t1.commit()

c2 = consus.Client(b'127.0.0.1')
t2 = c1.begin_transaction()
t2.commit()

c3 = consus.Client('127.0.0.1')
t3 = c1.begin_transaction()
t3.commit()

c4 = consus.Client(b'127.0.0.1', 1982)
t4 = c1.begin_transaction()
t4.commit()

c5 = consus.Client('127.0.0.1', 1982)
t5 = c1.begin_transaction()
t5.commit()

c6 = consus.Client(b'127.0.0.1:1982')
t6 = c1.begin_transaction()
t6.commit()

c7 = consus.Client('127.0.0.1:1982')
t7 = c1.begin_transaction()
t7.commit()

c8 = consus.Client(b'[::]:1982,127.0.0.1:1982')
t8 = c1.begin_transaction()
t8.commit()

c9 = consus.Client('[::]:1982,127.0.0.1:1982')
t9 = c1.begin_transaction()
t9.commit()
