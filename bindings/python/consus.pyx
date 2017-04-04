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

    void *malloc(size_t size)
    void free(void *ptr)


cdef extern from "consus.h":

    cdef enum consus_returncode:
        CONSUS_SUCCESS       = 6656
        CONSUS_LESS_DURABLE  = 6657
        CONSUS_NOT_FOUND     = 6658
        CONSUS_ABORTED       = 6659
        CONSUS_COMMITTED     = 6660
        CONSUS_UNKNOWN_TABLE = 6720
        CONSUS_NONE_PENDING  = 6721
        CONSUS_INVALID       = 6722
        CONSUS_TIMEOUT       = 6784
        CONSUS_INTERRUPTED   = 6785
        CONSUS_SEE_ERRNO     = 6786
        CONSUS_COORD_FAIL    = 6787
        CONSUS_UNAVAILABLE   = 6788
        CONSUS_SERVER_ERROR  = 6789
        CONSUS_INTERNAL      = 6910
        CONSUS_GARBAGE       = 6911

    cdef struct consus_client
    cdef struct consus_transaction
    consus_client* consus_create(const char* coordinator, uint16_t port)
    consus_client* consus_create_conn_str(const char* conn_str)
    void consus_destroy(consus_client* client)
    int64_t consus_loop(consus_client* client, int timeout,
                        consus_returncode* status)
    int64_t consus_wait(consus_client* client, int64_t x, int timeout,
                        consus_returncode* status)
    int consus_poll_fd(consus_client* client)
    int consus_block(consus_client* client, int timeout)
    const char* consus_error_message(consus_client* client)
    const char* consus_error_location(consus_client* client)
    const char* consus_returncode_to_string(consus_returncode)
    int64_t consus_begin_transaction(consus_client* client, consus_returncode* status, consus_transaction** xact)
    int64_t consus_commit_transaction(consus_transaction* xact, consus_returncode* status)
    int64_t consus_abort_transaction(consus_transaction* xact, consus_returncode* status)
    int64_t consus_restart_transaction(consus_transaction* xact, consus_returncode* status)
    void consus_destroy_transaction(consus_transaction* xact)

    int64_t consus_get(consus_transaction* xact,
                       const char* table,
                       const char* key, size_t key_sz,
                       consus_returncode* status,
                       const char** value, size_t* value_sz)
    int64_t consus_put(consus_transaction* xact,
                       const char* table,
                       const char* key, size_t key_sz,
                       const char* value, size_t value_sz,
                       consus_returncode* status)


class ConsusException(Exception):

    def __init__(self, status, message):
        super(ConsusException, self).__init__(self)
        self._status = status
        self._symbol = consus_returncode_to_string(self._status)
        self._message = message

    def status(self):
        return self._status

    def symbol(self):
        return self._symbol

    def message(self):
        return self._message

    def __str__(self):
        return 'ConsusException: {0} [{1}]'.format(self.message(), self.symbol())

    def __repr__(self):
        return str(self)


class ConsusLessDurableException(ConsusException): pass
class ConsusNotFoundException(ConsusException): pass
class ConsusAbortedException(ConsusException): pass
class ConsusCommittedException(ConsusException): pass
class ConsusUnknownTableException(ConsusException): pass
class ConsusNonePendingException(ConsusException): pass
class ConsusInvalidException(ConsusException): pass
class ConsusTimeoutException(ConsusException): pass
class ConsusInterruptedException(ConsusException): pass
class ConsusSeeErrnoException(ConsusException): pass
class ConsusCoordFailException(ConsusException): pass
class ConsusUnavailableException(ConsusException): pass
class ConsusServerErrorException(ConsusException): pass
class ConsusInternalException(ConsusException): pass
class ConsusGarbageException(ConsusException): pass


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

    cdef finish(self, int64_t req, consus_returncode* rstatus):
        cdef consus_returncode lstatus
        if req < 0:
            self.throw_exception(rstatus[0])
        lid = consus_wait(self.client, req, -1, &lstatus)
        if lid < 0:
            self.throw_exception(lstatus)
        assert req == lid
        if rstatus[0] != CONSUS_SUCCESS and rstatus[0] != CONSUS_NOT_FOUND and rstatus[0] != CONSUS_LESS_DURABLE:
            self.throw_exception(rstatus[0])

    cdef throw_exception(self, consus_returncode status):
        exception = {CONSUS_LESS_DURABLE: ConsusLessDurableException,
                     CONSUS_NOT_FOUND: ConsusNotFoundException,
                     CONSUS_ABORTED: ConsusAbortedException,
                     CONSUS_COMMITTED: ConsusCommittedException,
                     CONSUS_UNKNOWN_TABLE: ConsusUnknownTableException,
                     CONSUS_NONE_PENDING: ConsusNonePendingException,
                     CONSUS_INVALID: ConsusInvalidException,
                     CONSUS_TIMEOUT: ConsusTimeoutException,
                     CONSUS_INTERRUPTED: ConsusInterruptedException,
                     CONSUS_SEE_ERRNO: ConsusSeeErrnoException,
                     CONSUS_COORD_FAIL: ConsusCoordFailException,
                     CONSUS_UNAVAILABLE: ConsusUnavailableException,
                     CONSUS_SERVER_ERROR: ConsusServerErrorException,
                     CONSUS_INTERNAL: ConsusInternalException,
                     CONSUS_GARBAGE: ConsusGarbageException}.get(status, ConsusInternalException)
        raise exception(status, consus_error_message(self.client).decode('ascii', 'ignore'))


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
            consus_destroy_transaction(self.xact)

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
        return self.client.finish(req, rstatus)
