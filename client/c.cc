// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#define __STDC_LIMIT_MACROS

// POSIX
#include <errno.h>
#include <signal.h>

// C++
#include <new>

// e
#include <e/guard.h>

// consus
#include <consus.h>
#include "visibility.h"
#include "common/macros.h"
#include "client/client.h"
#include "client/transaction.h"

#define FAKE_STATUS consus_returncode _status; consus_returncode* status = &_status

#define SIGNAL_PROTECT_ERR(X) \
    sigset_t old_sigs; \
    sigset_t all_sigs; \
    sigfillset(&all_sigs); \
    if (pthread_sigmask(SIG_BLOCK, &all_sigs, &old_sigs) < 0) \
    { \
        *status = CONSUS_INTERNAL; \
        return (X); \
    } \
    e::guard g = e::makeguard(pthread_sigmask, SIG_SETMASK, (sigset_t*)&old_sigs, (sigset_t*)NULL)

#define SIGNAL_PROTECT SIGNAL_PROTECT_ERR(-1);
inline void return_void() {}
#define SIGNAL_PROTECT_VOID SIGNAL_PROTECT_ERR(return_void());

#define C_WRAP_EXCEPT(X) \
    consus::client* cl = reinterpret_cast<consus::client*>(client); \
    SIGNAL_PROTECT; \
    try \
    { \
        X \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = CONSUS_SEE_ERRNO; \
        cl->set_error_message("out of memory"); \
        return -1; \
    } \
    catch (...) \
    { \
        *status = CONSUS_INTERNAL; \
        cl->set_error_message("internal state corrupted"); \
        return -1; \
    }

#define C_WRAP_EXCEPT_XACT(X) \
    consus::transaction* tx = reinterpret_cast<consus::transaction*>(xact); \
    consus::client* cl = tx->parent(); \
    SIGNAL_PROTECT; \
    try \
    { \
        X \
    } \
    catch (std::bad_alloc& ba) \
    { \
        errno = ENOMEM; \
        *status = CONSUS_SEE_ERRNO; \
        cl->set_error_message("out of memory"); \
        return -1; \
    } \
    catch (...) \
    { \
        *status = CONSUS_INTERNAL; \
        cl->set_error_message("internal state corrupted"); \
        return -1; \
    }

extern "C"
{

CONSUS_API consus_client*
consus_create(const char* host, uint16_t port)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);

    try
    {
        return reinterpret_cast<struct consus_client*>(new consus::client(host, port));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        errno = EINVAL;
        return NULL;
    }
}

CONSUS_API consus_client*
consus_create_conn_str(const char* conn_str)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);

    try
    {
        return reinterpret_cast<struct consus_client*>(new consus::client(conn_str));
    }
    catch (std::bad_alloc& ba)
    {
        errno = ENOMEM;
        return NULL;
    }
    catch (...)
    {
        errno = EINVAL;
        return NULL;
    }
}

CONSUS_API void
consus_destroy(consus_client* client)
{
    delete reinterpret_cast<consus::client*>(client);
}

CONSUS_API int64_t
consus_loop(consus_client* client, int timeout, consus_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->loop(timeout, status);
    );
}

CONSUS_API int64_t
consus_wait(consus_client* client, int64_t id, int timeout, consus_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->wait(id, timeout, status);
    );
}

CONSUS_API const char*
consus_error_message(consus_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    consus::client* cl = reinterpret_cast<consus::client*>(_cl);
    return cl->error_message();
}

CONSUS_API const char*
consus_error_location(consus_client* _cl)
{
    FAKE_STATUS;
    SIGNAL_PROTECT_ERR(NULL);
    consus::client* cl = reinterpret_cast<consus::client*>(_cl);
    return cl->error_location();
}

CONSUS_API const char*
consus_returncode_to_string(consus_returncode stat)
{
    switch (stat)
    {
        CSTRINGIFY(CONSUS_SUCCESS);
        CSTRINGIFY(CONSUS_LESS_DURABLE);
        CSTRINGIFY(CONSUS_NOT_FOUND);
        CSTRINGIFY(CONSUS_ABORTED);
        CSTRINGIFY(CONSUS_COMMITTED);
        CSTRINGIFY(CONSUS_UNKNOWN_TABLE);
        CSTRINGIFY(CONSUS_NONE_PENDING);
        CSTRINGIFY(CONSUS_INVALID);
        CSTRINGIFY(CONSUS_TIMEOUT);
        CSTRINGIFY(CONSUS_INTERRUPTED);
        CSTRINGIFY(CONSUS_SEE_ERRNO);
        CSTRINGIFY(CONSUS_COORD_FAIL);
        CSTRINGIFY(CONSUS_UNAVAILABLE);
        CSTRINGIFY(CONSUS_SERVER_ERROR);
        CSTRINGIFY(CONSUS_INTERNAL);
        CSTRINGIFY(CONSUS_GARBAGE);
        default:
            return "unknown consus_returncode";
    }
}

CONSUS_API int64_t
consus_begin_transaction(consus_client* client,
                         consus_returncode* status,
                         consus_transaction** xact)
{
    C_WRAP_EXCEPT(
    return cl->begin_transaction(status, xact);
    );
}

CONSUS_API void
consus_destroy_transaction(consus_transaction* xact)
{
    delete reinterpret_cast<consus::transaction*>(xact);
}

CONSUS_API int64_t
consus_get(consus_transaction* xact,
           const char* table,
           const char* key, size_t key_sz,
           consus_returncode* status,
           char** value, size_t* value_sz)
{
    C_WRAP_EXCEPT_XACT(
    return tx->get(table, key, key_sz, status, value, value_sz);
    );
}

CONSUS_API int64_t
consus_put(consus_transaction* xact,
           const char* table,
           const char* key, size_t key_sz,
           const char* value, size_t value_sz,
           consus_returncode* status)
{
    C_WRAP_EXCEPT_XACT(
    return tx->put(table, key, key_sz, value, value_sz, status);
    );
}

CONSUS_API int64_t
consus_commit_transaction(consus_transaction* xact,
                          consus_returncode* status)
{
    C_WRAP_EXCEPT_XACT(
    return tx->commit(status);
    );
}

CONSUS_API int64_t
consus_abort_transaction(consus_transaction* xact,
                         consus_returncode* status)
{
    C_WRAP_EXCEPT_XACT(
    return tx->abort(status);
    );
}

CONSUS_API int
consus_debug_client_configuration(consus_client* client,
                                  consus_returncode* status,
                                  const char** str)
{
    C_WRAP_EXCEPT(
    return cl->debug_client_configuration(status, str);
    );
}

CONSUS_API int
consus_debug_txman_configuration(consus_client* client,
                                 consus_returncode* status,
                                 const char** str)
{
    C_WRAP_EXCEPT(
    return cl->debug_txman_configuration(status, str);
    );
}

CONSUS_API int
consus_debug_kvs_configuration(consus_client* client,
                               consus_returncode* status,
                               const char** str)
{
    C_WRAP_EXCEPT(
    return cl->debug_kvs_configuration(status, str);
    );
}

CONSUS_API int
consus_admin_create_data_center(consus_client* client, const char* name,
                                consus_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->create_data_center(name, status);
    );
}

CONSUS_API int
consus_admin_set_default_data_center(consus_client* client, const char* name,
                                     consus_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->set_default_data_center(name, status);
    );
}

CONSUS_API int
consus_admin_availability_check(consus_client* client,
                                consus_availability_requirements* reqs,
                                int timeout, consus_returncode* status)
{
    C_WRAP_EXCEPT(
    return cl->availability_check(reqs, timeout, status);
    );
}

} // extern "C"
