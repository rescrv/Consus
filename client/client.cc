// Copyright (c) 2015-2017, Robert Escriva, Cornell University
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

// po6
#include <po6/time.h>

// treadstone
#include <treadstone.h>

// consus
#include "common/client_configuration.h"
#include "common/consus.h"
#include "common/coordinator_returncode.h"
#include "common/kvs_configuration.h"
#include "common/macros.h"
#include "common/paxos_group.h"
#include "common/txman_configuration.h"
#include "client/client.h"
#include "client/pending.h"
#include "client/pending_begin_transaction.h"
#include "client/pending_string.h"
#include "client/pending_unsafe_lock_op.h"
#include "client/pending_unsafe_read.h"
#include "client/pending_unsafe_write.h"

using consus::client;

#define ERROR(CODE) \
    *status = CONSUS_ ## CODE; \
    m_last_error.set_loc(__FILE__, __LINE__); \
    m_last_error.set_msg()

#define _BUSYBEE_ERROR(BBRC) \
    case BUSYBEE_ ## BBRC: \
        ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned " XSTR(BBRC) << ": please file a bug"

#define BUSYBEE_ERROR_CASE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return -1;

#define BUSYBEE_ERROR_CASE_FALSE(BBRC) \
    _BUSYBEE_ERROR(BBRC); \
    return false;

client :: client(const char* host, uint16_t port)
    : m_coord(replicant_client_create(host, port))
    , m_config()
    , m_config_id(-1)
    , m_config_status(REPLICANT_SUCCESS)
    , m_config_state(0)
    , m_config_data(NULL)
    , m_config_data_sz(0)
    , m_busybee_controller(&m_config)
    , m_busybee(busybee_client::create(&m_busybee_controller))
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending()
    , m_returnable()
    , m_returned()
    , m_flagfd()
    , m_last_error()
{
    if (!m_coord)
    {
        throw std::bad_alloc();
    }

    busybee_returncode rc = m_busybee->set_external_fd(replicant_client_poll_fd(m_coord));
    assert(rc == BUSYBEE_SUCCESS);
}

client :: client(const char* conn_str)
    : m_coord(replicant_client_create_conn_str(conn_str))
    , m_config()
    , m_config_id(-1)
    , m_config_status(REPLICANT_SUCCESS)
    , m_config_state(0)
    , m_config_data(NULL)
    , m_config_data_sz(0)
    , m_busybee_controller(&m_config)
    , m_busybee(busybee_client::create(&m_busybee_controller))
    , m_next_client_id(1)
    , m_next_server_nonce(1)
    , m_pending()
    , m_returnable()
    , m_returned()
    , m_flagfd()
    , m_last_error()
{
    if (!m_coord)
    {
        throw std::bad_alloc();
    }

    busybee_returncode rc = m_busybee->set_external_fd(replicant_client_poll_fd(m_coord));
    assert(rc == BUSYBEE_SUCCESS);
}

client :: ~client() throw ()
{
    replicant_client_destroy(m_coord);
}

int64_t
client :: loop(int timeout, consus_returncode* status)
{
    *status = CONSUS_SUCCESS;
    m_last_error = e::error();

    while (!m_returnable.empty() || !m_pending.empty())
    {
        if (!m_returnable.empty())
        {
            m_returned = m_returnable.front();
            m_returnable.pop_front();
            m_last_error = m_returned->error();
            return m_returned->client_id();
        }

        if (inner_loop(timeout, status) < 0)
        {
            return -1;
        }
    }

    return post_loop(status);
}

int64_t
client :: wait(int64_t id, int timeout, consus_returncode* status)
{
    *status = CONSUS_SUCCESS;
    m_last_error = e::error();

    while (true)
    {
        for (std::list<e::intrusive_ptr<pending> >::iterator it = m_returnable.begin();
                it != m_returnable.end(); ++it)
        {
            pending* p = it->get();

            if (p->client_id() == id)
            {
                m_returned = *it;
                m_returnable.erase(it);
                m_last_error = m_returned->error();
                return m_returned->client_id();
            }
        }

        if (inner_loop(timeout, status) < 0)
        {
            return -1;
        }
    }

    return post_loop(status);
}

int64_t
client :: begin_transaction(consus_returncode* status,
                            consus_transaction** xact)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    int64_t client_id = generate_new_client_id();
    pending* p = new pending_begin_transaction(client_id, status, xact);
    p->kickstart_state_machine(this);
    return client_id;
}

int64_t
client :: unsafe_get(const char* table,
                     const char* key, size_t key_sz,
                     consus_returncode* status,
                     char** value, size_t* value_sz)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    int64_t client_id = generate_new_client_id();
    pending* p = new pending_unsafe_read(client_id, status,
            table, binkey, binkey_sz, value, value_sz);
    free(binkey);
    p->kickstart_state_machine(this);
    return client_id;
}

int64_t
client :: unsafe_put(const char* table,
                     const char* key, size_t key_sz,
                     const char* value, size_t value_sz,
                     consus_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;
    unsigned char* binval = NULL;
    size_t binval_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    if (treadstone_json_sz_to_binary(value, value_sz, &binval, &binval_sz) < 0)
    {
        ERROR(INVALID) << "value contains invalid JSON";
        free(binkey);
        return -1;
    }

    int64_t client_id = generate_new_client_id();
    pending* p = new pending_unsafe_write(client_id, status,
            table, binkey, binkey_sz, binval, binval_sz);
    free(binkey);
    free(binval);
    p->kickstart_state_machine(this);
    return client_id;
}

int64_t
client :: unsafe_lock(const char* table,
                      const char* key, size_t key_sz,
                      consus_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    int64_t client_id = generate_new_client_id();
    pending* p = new pending_unsafe_lock_op(client_id, status,
            table, binkey, binkey_sz, LOCK_LOCK);
    free(binkey);
    p->kickstart_state_machine(this);
    return client_id;
}

int64_t
client :: unsafe_unlock(const char* table,
                        const char* key, size_t key_sz,
                        consus_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    int64_t client_id = generate_new_client_id();
    pending* p = new pending_unsafe_lock_op(client_id, status,
            table, binkey, binkey_sz, LOCK_UNLOCK);
    free(binkey);
    p->kickstart_state_machine(this);
    return client_id;
}

int
client :: create_data_center(const char* name, consus_returncode* status)
{
    std::string tmp;
    e::packer(&tmp) << e::slice(name);
    replicant_returncode rc;
    char* data = NULL;
    size_t data_sz = 0;
    int64_t id = replicant_client_call(m_coord, "consus", "data_center_create",
                                       tmp.data(), tmp.size(), REPLICANT_CALL_ROBUST,
                                       &rc, &data, &data_sz);

    if (!replicant_finish(id, &rc, status))
    {
        return -1;
    }

    // XXX
    if (data) free(data);
#if 0
    coordinator_returncode crc;
    e::unpacker up(data, data_sz);
    up = up >> e::unpack_uint16(crc);

    if (up.error())
    {
        ERROR(COORD_FAIL) << "coordinator failure: invalid return value";
        return -1;
    }

    switch (crc)
    {
        case COORDINATOR_SUCCESS:
            *status = REPLICANT_SUCCESS;
            return 0;
    }

    abort();
#endif
    return 0;
}

int
client :: set_default_data_center(const char* name, consus_returncode* status)
{
    std::string tmp;
    e::packer(&tmp) << e::slice(name);
    replicant_returncode rc;
    char* data = NULL;
    size_t data_sz = 0;
    int64_t id = replicant_client_call(m_coord, "consus", "data_center_default",
                                       tmp.data(), tmp.size(), REPLICANT_CALL_ROBUST,
                                       &rc, &data, &data_sz);

    if (!replicant_finish(id, &rc, status))
    {
        return -1;
    }

    // XXX
    if (data) free(data);
#if 0
    coordinator_returncode crc;
    e::unpacker up(data, data_sz);
    up = up >> e::unpack_uint16(crc);

    if (up.error())
    {
        ERROR(COORD_FAIL) << "coordinator failure: invalid return value";
        return -1;
    }

    switch (crc)
    {
        case COORDINATOR_SUCCESS:
            *status = REPLICANT_SUCCESS;
            return 0;
    }

    abort();
#endif
    return 0;
}

int
client :: availability_check(consus_availability_requirements* reqs,
                             int timeout,
                             consus_returncode* status)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    const uint64_t start = po6::monotonic_time();
    uint64_t now = 0;
    uint64_t version = 0;

    while (timeout < 0 || start + PO6_SECONDS * timeout >= (now = po6::monotonic_time()))
    {
        replicant_returncode rc = REPLICANT_GARBAGE;
        char* data = NULL;
        size_t data_sz = 0;
        int64_t id = replicant_client_cond_wait(m_coord, "consus", "txmanconf", version, &rc, &data, &data_sz);
        int to = -1;

        if (timeout >= 0)
        {
            to = (start + PO6_SECONDS * timeout - now) / PO6_MILLIS + 1;
            to = std::min(to, int(100));
        }

        if (!replicant_finish(id, to, &rc, status))
        {
            replicant_client_kill(m_coord, id);

            if (rc == REPLICANT_TIMEOUT)
            {
                continue;
            }
            else
            {
                return -1;
            }
        }

        assert(data || data_sz == 0);
        e::unpacker up(data, data_sz);
        cluster_id cid;
        version_id vid;
        uint64_t flags;
        std::vector<data_center> dcs;
        std::vector<txman_state> txmans;
        std::vector<paxos_group> txman_groups;
        std::vector<kvs> kvss;
        up = txman_configuration(up, &cid, &vid, &flags, &dcs, &txmans, &txman_groups, &kvss);

        if (data)
        {
            free(data);
        }

        if (up.error())
        {
            ERROR(COORD_FAIL) << "coordinator failure: bad client configuration";
            return -1;
        }

        version = vid.get() + 1;
        unsigned txmans_avail = 0;

        for (size_t i = 0; i < txmans.size(); ++i)
        {
            if (txmans[i].state == txman_state::ONLINE)
            {
                ++txmans_avail;
            }
        }

        if (txmans_avail < reqs->txmans ||
            txman_groups.size() < reqs->txman_groups ||
            kvss.size() < reqs->kvss)
        {
            continue;
        }

        if (!reqs->stable)
        {
            *status = CONSUS_SUCCESS;
            return 0;
        }

        rc = REPLICANT_GARBAGE;
        data = NULL;
        data_sz = 0;
        id = replicant_client_call(m_coord, "consus", "is_stable",
                                   NULL, 0, REPLICANT_CALL_IDEMPOTENT,
                                   &rc, &data, &data_sz);

        if (!replicant_finish(id, -1, &rc, status))
        {
            replicant_client_kill(m_coord, id);
            return -1;
        }

        assert(data || data_sz == 0);
        up = e::unpacker(data, data_sz);
        coordinator_returncode ccr;
        up = up >> ccr;

        if (data)
        {
            free(data);
        }

        if (!up.error() && ccr == COORD_SUCCESS)
        {
            *status = CONSUS_SUCCESS;
            return 0;
        }
    }

    ERROR(TIMEOUT) << "operation timed out";
    return -1;
}

bool
client :: replicant_finish(int64_t id, replicant_returncode* rc, consus_returncode* status)
{
    return replicant_finish(id, -1, rc, status);
}

bool
client :: replicant_finish(int64_t id, int timeout, replicant_returncode* rc, consus_returncode* status)
{
    if (id < 0)
    {
        ERROR(COORD_FAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
        return false;
    }

    replicant_returncode lrc;
    int64_t lid = replicant_client_wait(m_coord, id, timeout, &lrc);

    if (lid < 0)
    {
        *rc = lrc;
        ERROR(COORD_FAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
        return false;
    }

    assert(id == lid);

    if (*rc != REPLICANT_SUCCESS)
    {
        ERROR(COORD_FAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
        return false;
    }

    return true;
}

int
client :: debug_client_configuration(consus_returncode* status, const char** str)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    replicant_returncode rc = REPLICANT_GARBAGE;
    char* data = NULL;
    size_t data_sz = 0;
    int64_t id = replicant_client_cond_wait(m_coord, "consus", "clientconf", 0, &rc, &data, &data_sz);

    if (!replicant_finish(id, &rc, status) || (!data && data_sz != 0))
    {
        return -1;
    }

    e::unpacker up(data, data_sz);
    cluster_id cid;
    version_id vid;
    uint64_t flags;
    std::vector<txman> txmans;
    up = client_configuration(up, &cid, &vid, &flags, &txmans);
    free(data);

    if (up.error())
    {
        ERROR(COORD_FAIL) << "coordinator failure: bad client configuration";
        return -1;
    }

    std::ostringstream ostr;
    ostr << cid << "\n"
         << vid << "\n";

    if (txmans.empty())
    {
        ostr << "no transaction managers";
    }
    else if (txmans.size() == 1)
    {
        ostr << "1 transaction manager:\n";
    }
    else
    {
        ostr << txmans.size() << " transaction managers:\n";
    }

    for (unsigned i = 0; i < txmans.size(); ++i)
    {
        ostr << txmans[i] << "\n";
    }

    e::intrusive_ptr<pending_string> p = new pending_string(ostr.str());
    *str = p->string();
    m_returned = p.get();
    *status = CONSUS_SUCCESS;
    m_last_error = e::error();
    return 0;
}

int
client :: debug_txman_configuration(consus_returncode* status, const char** str)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    replicant_returncode rc = REPLICANT_GARBAGE;
    char* data = NULL;
    size_t data_sz = 0;
    int64_t id = replicant_client_cond_wait(m_coord, "consus", "txmanconf", 0, &rc, &data, &data_sz);

    if (!replicant_finish(id, &rc, status) || (!data && data_sz != 0))
    {
        return -1;
    }

    e::unpacker up(data, data_sz);
    cluster_id cid;
    version_id vid;
    uint64_t flags;
    std::vector<data_center> dcs;
    std::vector<txman_state> txmans;
    std::vector<paxos_group> txman_groups;
    std::vector<kvs> kvss;
    up = txman_configuration(up, &cid, &vid, &flags, &dcs, &txmans, &txman_groups, &kvss);
    free(data);

    if (up.error())
    {
        ERROR(COORD_FAIL) << "coordinator failure: bad client configuration";
        return -1;
    }

    std::string s = txman_configuration(cid, vid, flags, dcs, txmans, txman_groups, kvss);
    e::intrusive_ptr<pending_string> p = new pending_string(s);
    *str = p->string();
    m_returned = p.get();
    *status = CONSUS_SUCCESS;
    m_last_error = e::error();
    return 0;
}

int
client :: debug_kvs_configuration(consus_returncode* status, const char** str)
{
    if (!maintain_coord_connection(status))
    {
        return -1;
    }

    replicant_returncode rc = REPLICANT_GARBAGE;
    char* data = NULL;
    size_t data_sz = 0;
    int64_t id = replicant_client_cond_wait(m_coord, "consus", "kvsconf", 0, &rc, &data, &data_sz);

    if (!replicant_finish(id, &rc, status) || (!data && data_sz != 0))
    {
        return -1;
    }

    e::unpacker up(data, data_sz);
    cluster_id cid;
    version_id vid;
    uint64_t flags;
    std::vector<kvs_state> kvss;
    std::vector<ring> rings;
    up = kvs_configuration(up, &cid, &vid, &flags, &kvss, &rings);
    free(data);

    if (up.error())
    {
        ERROR(COORD_FAIL) << "coordinator failure: bad client configuration";
        return -1;
    }

    std::string s = kvs_configuration(cid, vid, flags, kvss, rings);
    e::intrusive_ptr<pending_string> p = new pending_string(s);
    *str = p->string();
    m_returned = p.get();
    *status = CONSUS_SUCCESS;
    m_last_error = e::error();
    return 0;
}

const char*
client :: error_message()
{
    return m_last_error.msg();
}

const char*
client :: error_location()
{
    return m_last_error.loc();
}

void
client :: set_error_message(const char* msg)
{
    m_last_error = e::error();
    m_last_error.set_loc(__FILE__, __LINE__);
    m_last_error.set_msg() << msg;
}

uint64_t
client :: generate_new_nonce()
{
    return m_next_server_nonce++;
}

int64_t
client :: generate_new_client_id()
{
    return m_next_client_id++;
}

void
client :: initialize(server_selector* ss)
{
    m_config.initialize(ss);
}

void
client :: add_to_returnable(pending* p)
{
    m_returnable.push_back(p);
}

bool
client :: send(uint64_t nonce, comm_id id, std::auto_ptr<e::buffer> msg, pending* p)
{
    busybee_returncode rc = m_busybee->send(id.get(), msg);

    if (rc == BUSYBEE_DISRUPTED)
    {
        handle_disruption(id);
    }

    if (rc == BUSYBEE_SUCCESS)
    {
        m_pending[std::make_pair(id, nonce)] = p;
    }

    return rc == BUSYBEE_SUCCESS;
}

void
client :: handle_disruption(const comm_id& id)
{
    for (std::map<std::pair<comm_id, uint64_t>, e::intrusive_ptr<pending> >::iterator it = m_pending.begin();
            it != m_pending.end(); )
    {
        if (it->first.first == id)
        {
            e::intrusive_ptr<pending> p = it->second;
            m_pending.erase(it);
            p->handle_server_disruption(this, id);
            it = m_pending.begin();
        }
        else
        {
            ++it;
        }
    }
}

int64_t
client :: inner_loop(int timeout, consus_returncode* status)
{
    uint64_t cid_num;
    std::auto_ptr<e::buffer> msg;
    busybee_returncode rc = m_busybee->recv(timeout, &cid_num, &msg);
    comm_id id(cid_num);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            break;
        case BUSYBEE_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            return -1;
        case BUSYBEE_TIMEOUT:
            return 0;
        case BUSYBEE_DISRUPTED:
            handle_disruption(id);
            return 0;
        case BUSYBEE_EXTERNAL:
            if (!maintain_coord_connection(status))
            {
                return -1;
            }
            return 0;
        case BUSYBEE_SEE_ERRNO:
            ERROR(SEE_ERRNO) << po6::strerror(errno);
            return -1;
        BUSYBEE_ERROR_CASE(SHUTDOWN);
        default:
            ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                            << (unsigned) rc << ": please file a bug";
            return -1;
    }

    network_msgtype msg_type;
    uint64_t nonce;
    e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
    up = up >> msg_type >> nonce;

    if (up.error() || msg_type != CLIENT_RESPONSE)
    {
        ERROR(SERVER_ERROR) << "communication error: server "
                            << cid_num << " sent message="
                            << msg->as_slice().hex()
                            << " with invalid header";
        return -1;
    }

    std::map<std::pair<comm_id, uint64_t>, e::intrusive_ptr<pending> >::iterator it;
    it = m_pending.find(std::make_pair(id, nonce));

    if (it != m_pending.end())
    {
        e::intrusive_ptr<pending> p(it->second);
        m_pending.erase(it);
        p->handle_busybee_op(this, nonce, msg, up);
        return p->client_id();
    }

    return 0;
}

int64_t
client :: post_loop(consus_returncode* status)
{
    uint64_t cid_num;
    busybee_returncode rc = m_busybee->recv_no_msg(0, &cid_num);

    switch (rc)
    {
        case BUSYBEE_TIMEOUT:
            break;
        case BUSYBEE_INTERRUPTED:
            ERROR(INTERRUPTED) << "signal received";
            return -1;
        case BUSYBEE_DISRUPTED:
            handle_disruption(comm_id(cid_num));
            break;
        case BUSYBEE_EXTERNAL:
            if (!maintain_coord_connection(status))
            {
                return -1;
            }
            break;
        case BUSYBEE_SEE_ERRNO:
            ERROR(SEE_ERRNO) << po6::strerror(errno);
            return -1;
        BUSYBEE_ERROR_CASE(SHUTDOWN);
        case BUSYBEE_SUCCESS:
        default:
            ERROR(INTERNAL) << "internal error: BusyBee unexpectedly returned "
                            << (unsigned) rc << ": please file a bug";
            return -1;
    }

    ERROR(NONE_PENDING) << "no outstanding operations to process";
    return -1;
}

bool
client :: maintain_coord_connection(consus_returncode* status)
{
    if (m_config_status != REPLICANT_SUCCESS)
    {
        replicant_client_kill(m_coord, m_config_id);
        m_config_id = -1;
    }

    if (m_config_id < 0)
    {
        m_config_id = replicant_client_cond_follow(m_coord, "consus", "clientconf",
                                                   &m_config_status, &m_config_state,
                                                   &m_config_data, &m_config_data_sz);
        replicant_returncode rc;

        if (replicant_client_wait(m_coord, m_config_id, -1, &rc) < 0)
        {
            ERROR(COORD_FAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
            return false;
        }
    }

    replicant_returncode rc;

    if (replicant_client_loop(m_coord, 0, &rc) < 0)
    {
        if (rc == REPLICANT_TIMEOUT ||
            rc == REPLICANT_INTERRUPTED ||
            rc == REPLICANT_NONE_PENDING)
        {
        }
        else
        {
            ERROR(COORD_FAIL) << "coordinator failure: " << replicant_client_error_message(m_coord);
            return false;
        }
    }

    if (m_config.version().get() < m_config_state)
    {
        configuration new_config;
        e::unpacker up(m_config_data, m_config_data_sz);
        up = up >> new_config;

        if (!up.error())
        {
            m_config = new_config;
        }
    }

    return true;
}
