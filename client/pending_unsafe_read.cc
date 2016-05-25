// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/strescape.h>

// treadstone
#include <treadstone.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "client/client.h"
#include "client/pending_unsafe_read.h"

using consus::pending_unsafe_read;

pending_unsafe_read :: pending_unsafe_read(int64_t client_id,
                                           consus_returncode* status,
                                           const char* table,
                                           const unsigned char* key, size_t key_sz,
                                           char** value, size_t* value_sz)
    : pending(client_id, status)
    , m_ss()
    , m_table(table)
    , m_key(key, key + key_sz)
    , m_value(value)
    , m_value_sz(value_sz)
{
}

pending_unsafe_read :: ~pending_unsafe_read() throw ()
{
}

std::string
pending_unsafe_read :: describe()
{
    std::ostringstream ostr;
    ostr << "pending_unsafe_read(table=\"" << e::strescape(m_table)
         << "\", key=\"" << e::strescape(m_key) << "\")";
    return ostr.str();
}

void
pending_unsafe_read :: kickstart_state_machine(client* cl)
{
    cl->initialize(&m_ss);
    send_request(cl);
}

void
pending_unsafe_read :: handle_server_failure(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_unsafe_read :: handle_server_disruption(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_unsafe_read :: handle_busybee_op(client* cl,
                                         uint64_t,
                                         std::auto_ptr<e::buffer>,
                                         e::unpacker up)
{
    consus_returncode rc;
    uint64_t timestamp;
    e::slice value;
    up = up >> rc >> timestamp >> value;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "server sent a corrupt response to \"read\"";
        cl->add_to_returnable(this);
        return;
    }

    if (rc != CONSUS_SUCCESS && rc != CONSUS_NOT_FOUND)
    {
        set_status(rc);
        error(__FILE__, __LINE__) << "server sent failure code";
        cl->add_to_returnable(this);
        return;
    }

    if (rc == CONSUS_SUCCESS)
    {
        char* tmp = NULL;

        if (treadstone_binary_to_json(value.data(), value.size(), &tmp))
        {
            PENDING_ERROR(SEE_ERRNO) << po6::strerror(errno);
            cl->add_to_returnable(this);
            return;
        }

        *m_value = tmp;
        *m_value_sz = strlen(tmp);
        this->success();
        cl->add_to_returnable(this);
    }
    else if (rc == CONSUS_NOT_FOUND)
    {
        *m_value = NULL;
        *m_value_sz = 0;
        set_status(CONSUS_NOT_FOUND);
        error(__FILE__, __LINE__) << "value not found";
        cl->add_to_returnable(this);
    }
    else
    {
        abort();
    }
}

void
pending_unsafe_read :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = cl->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(UNSAFE_READ)
                        + VARINT_64_MAX_SIZE
                        + pack_size(e::slice(m_table))
                        + pack_size(e::slice(m_key));
        comm_id id = m_ss.next();

        if (id == comm_id())
        {
            PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers available";
            cl->add_to_returnable(this);
            return;
        }

        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << UNSAFE_READ
            << e::pack_varint(nonce)
            << e::slice(m_table)
            << e::slice(m_key);

        if (cl->send(nonce, id, msg, this))
        {
            return;
        }
    }
}
