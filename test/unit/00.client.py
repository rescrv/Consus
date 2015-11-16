import consus

c1 = consus.Client()
c2 = consus.Client(b'127.0.0.1')
c3 = consus.Client('127.0.0.1')
c4 = consus.Client(b'127.0.0.1', 1982)
c5 = consus.Client('127.0.0.1', 1982)
c6 = consus.Client(b'127.0.0.1:1982')
c7 = consus.Client('127.0.0.1:1982')
c8 = consus.Client(b'[::]:1982,127.0.0.1:1982')
c9 = consus.Client('[::]:1982,127.0.0.1:1982')
