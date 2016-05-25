// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "client/client.h"
#include "client/pending_unsafe_write.h"

using consus::pending_unsafe_write;

pending_unsafe_write :: pending_unsafe_write(int64_t client_id,
                                             consus_returncode* status,
                                             const char* table,
                                             const unsigned char* key, size_t key_sz,
                                             const unsigned char* value, size_t value_sz)
    : pending(client_id, status)
    , m_ss()
    , m_table(table)
    , m_key(key, key + key_sz)
    , m_value(value, value + value_sz)
{
}

pending_unsafe_write :: ~pending_unsafe_write() throw ()
{
}

std::string
pending_unsafe_write :: describe()
{
    std::ostringstream ostr;
    ostr << "pending_unsafe_write(table=\"" << e::strescape(m_table)
         << "\", key=\"" << e::strescape(m_key)
         << "\", value=\"" << e::strescape(m_value) << "\")";
    return ostr.str();
}

void
pending_unsafe_write :: kickstart_state_machine(client* cl)
{
    cl->initialize(&m_ss);
    send_request(cl);
}

void
pending_unsafe_write :: handle_server_failure(client* cl, comm_id)
{
    PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers to ensure durability";
    cl->add_to_returnable(this);
}

void
pending_unsafe_write :: handle_server_disruption(client* cl, comm_id)
{
    PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers to ensure durability";
    cl->add_to_returnable(this);
}

void
pending_unsafe_write :: handle_busybee_op(client* cl,
                                          uint64_t,
                                          std::auto_ptr<e::buffer>,
                                          e::unpacker up)
{
    consus_returncode rc;
    up = up >> rc;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "server sent a corrupt response to \"transaction-write\"";
        cl->add_to_returnable(this);
        return;
    }

    if (rc != CONSUS_SUCCESS)
    {
        set_status(rc);
        error(__FILE__, __LINE__) << "server sent failure code";
        cl->add_to_returnable(this);
        return;
    }

    this->success();
    cl->add_to_returnable(this);
}

void
pending_unsafe_write :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = cl->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(UNSAFE_WRITE)
                        + VARINT_64_MAX_SIZE
                        + sizeof(uint8_t)
                        + pack_size(e::slice(m_table))
                        + pack_size(e::slice(m_key))
                        + pack_size(e::slice(m_value));
        comm_id id = m_ss.next();

        if (id == comm_id())
        {
            PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers to ensure durability";
            cl->add_to_returnable(this);
            return;
        }

        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << UNSAFE_WRITE
            << e::pack_varint(nonce)
            << uint8_t(0)
            << e::slice(m_table)
            << e::slice(m_key)
            << e::slice(m_value);

        if (cl->send(nonce, id, msg, this))
        {
            m_ss.clear();
            return;
        }
    }
}
