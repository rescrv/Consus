# Copyright (c) 2015, Robert Escriva
# All rights reserved.


import datetime
import calendar
import json
import time


cdef extern from "stdint.h":

    ctypedef short int int16_t
    ctypedef unsigned short int uint16_t
    ctypedef int int32_t
    ctypedef unsigned int uint32_t
    ctypedef long int int64_t
    ctypedef unsigned long int uint64_t
    ctypedef long unsigned int size_t


cdef extern from "stdlib.h":

    void *malloc(size_t size);
    void free(void *ptr);


cdef extern from "consus.h":

    cdef enum consus_returncode:
        CONSUS_SUCCESS       = 6656
        CONSUS_NOT_FOUND     = 6657
        CONSUS_UNKNOWN_TABLE = 6720
        CONSUS_NONE_PENDING  = 6721
        CONSUS_TIMEOUT       = 6784
        CONSUS_INTERRUPTED   = 6785
        CONSUS_SEE_ERRNO     = 6786
        CONSUS_INTERNAL      = 6910
        CONSUS_GARBAGE       = 6911

    cdef struct consus_client
    cdef struct consus_transaction
    consus_client* consus_create(const char* coordinator, uint16_t port);
    consus_client* consus_create_conn_str(const char* conn_str);
    void consus_destroy(consus_client* client);
    int64_t consus_loop(consus_client* client, int timeout,
                        consus_returncode* status);
    int64_t consus_wait(consus_client* client, int64_t x, int timeout,
                        consus_returncode* status);
    int consus_poll_fd(consus_client* client);
    int consus_block(consus_client* client, int timeout);
    const char* consus_error_message(consus_client* client);
    const char* consus_error_location(consus_client* client);
    const char* consus_returncode_to_string(consus_returncode);
    int64_t consus_begin_transaction(consus_client* client, consus_returncode* status, consus_transaction** xact);
    int64_t consus_commit_transaction(consus_transaction* xact, consus_returncode* status);
    int64_t consus_abort_transaction(consus_transaction* xact, consus_returncode* status);
    int64_t consus_restart_transaction(consus_transaction* xact, consus_returncode* status);
    void consus_destroy_transaction(consus_transaction* xact);

    int64_t consus_get(consus_transaction* xact,
                       const char* table,
                       const char* key, size_t key_sz,
                       consus_returncode* status,
                       const char** value, size_t* value_sz);
    int64_t consus_put(consus_transaction* xact,
                       const char* table,
                       const char* key, size_t key_sz,
                       const char* value, size_t value_sz,
                       consus_returncode* status);


cdef class Client:
    cdef consus_client* client

    def __cinit__(self, *args):
        conn_str = '127.0.0.1:1982'
        if len(args) == 2 and not isinstance(args[1], int):
            raise ValueError('Expected an integer port number')
        elif len(args) == 2:
            conn_str = '%s:%d' % tuple(args)
        elif len(args) == 0:
            pass
        elif len(args) == 1:
            conn_str = args[0]
        elif len(args) != 1:
            raise TypeError('Client() at most 2 arguments (%d given)' % len(args))
        if not isinstance(conn_str, bytes):
            conn_str = conn_str.encode('ascii')
        self.client = consus_create_conn_str(conn_str)

    def __dealloc__(self):
        if self.client:
            consus_destroy(self.client)

    def begin_transaction(self):
        return Transaction(self)


cdef class Transaction:
    cdef Client client
    cdef consus_transaction* xact

    def __cinit__(self, Client client):
        cdef consus_returncode status
        self.client = client
        req = consus_begin_transaction(self.client.client, &status, &self.xact)
        self.finish(req, &status)
        assert self.xact

    def __dealloc__(self):
        if self.xact:
            consus_destroy_transaction(self.xact);

    def get(self, str table, key):
        cdef bytes tmp = table.encode('ascii')
        cdef bytes jkey = json.dumps(key).encode('utf8')
        cdef consus_returncode status
        cdef const char* t = tmp
        cdef const char* k = jkey
        cdef size_t k_sz = len(jkey)
        cdef char* value
        cdef size_t value_sz
        req = consus_get(self.xact, t, k, k_sz, &status, &value, &value_sz)
        self.finish(req, &status)
        if status == CONSUS_SUCCESS:
            x = json.loads(value[:value_sz].decode('utf8'))
            free(value)
            return x
        else:
            return None

    def put(self, str table, key, value):
        cdef bytes tmp = table.encode('ascii')
        cdef bytes jkey = json.dumps(key).encode('utf8')
        cdef bytes jvalue = json.dumps(value).encode('utf8')
        cdef consus_returncode status
        cdef const char* t = tmp
        cdef const char* k = jkey
        cdef size_t k_sz = len(jkey)
        cdef const char* v = jvalue
        cdef size_t v_sz = len(jvalue)
        req = consus_put(self.xact, t, k, k_sz, v, v_sz, &status)
        self.finish(req, &status)
        return True

    def commit(self):
        cdef consus_returncode status
        req = consus_commit_transaction(self.xact, &status)
        self.finish(req, &status)

    def abort(self):
        cdef consus_returncode status
        req = consus_abort_transaction(self.xact, &status)
        self.finish(req, &status)

    cdef finish(self, int64_t req, consus_returncode* rstatus):
        cdef consus_returncode lstatus
        if req < 0:
            print consus_error_message(self.client.client).decode('ascii', 'ignore'), '@', \
                  consus_error_location(self.client.client).decode('ascii', 'ignore'), ' ', \
                  consus_returncode_to_string(rstatus[0]).decode('ascii', 'ignore')
            assert False # XXX
        lid = consus_wait(self.client.client, req, -1, &lstatus);
        if lid < 0:
            print consus_error_message(self.client.client).decode('ascii', 'ignore'), '@', \
                  consus_error_location(self.client.client).decode('ascii', 'ignore'), ' ', \
                  consus_returncode_to_string(lstatus).decode('ascii', 'ignore')
            assert False # XXX
        assert req == lid
        if rstatus[0] != CONSUS_SUCCESS and rstatus[0] != CONSUS_NOT_FOUND:
            print consus_error_message(self.client.client).decode('ascii', 'ignore'), '@', \
                  consus_error_location(self.client.client).decode('ascii', 'ignore'), ' ', \
                  consus_returncode_to_string(rstatus[0]).decode('ascii', 'ignore')
            assert False # XXX
