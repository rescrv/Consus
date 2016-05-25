// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "client/client.h"
#include "client/pending_unsafe_lock_op.h"

using consus::pending_unsafe_lock_op;

pending_unsafe_lock_op :: pending_unsafe_lock_op(int64_t client_id,
                                                 consus_returncode* status,
                                                 const char* table,
                                                 const unsigned char* key, size_t key_sz,
                                                 lock_op op)
    : pending(client_id, status)
    , m_ss()
    , m_table(table)
    , m_key(key, key + key_sz)
    , m_op(op)
{
}

pending_unsafe_lock_op :: ~pending_unsafe_lock_op() throw ()
{
}

std::string
pending_unsafe_lock_op :: describe()
{
    std::ostringstream ostr;
    ostr << "pending_unsafe_lock_op(table=\"" << e::strescape(m_table)
         << "\", key=\"" << e::strescape(m_key) << "\")";
    return ostr.str();
}

void
pending_unsafe_lock_op :: kickstart_state_machine(client* cl)
{
    cl->initialize(&m_ss);
    send_request(cl);
}

void
pending_unsafe_lock_op :: handle_server_failure(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_unsafe_lock_op :: handle_server_disruption(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_unsafe_lock_op :: handle_busybee_op(client* cl,
                                            uint64_t,
                                            std::auto_ptr<e::buffer>,
                                            e::unpacker up)
{
    consus_returncode rc;
    e::slice value;
    up = up >> rc;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "server sent a corrupt response to \"lock\"";
        cl->add_to_returnable(this);
        return;
    }

    if (rc == CONSUS_SUCCESS)
    {
        this->success();
    }

    set_status(rc);
    cl->add_to_returnable(this);
}

void
pending_unsafe_lock_op :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = cl->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(UNSAFE_LOCK_OP)
                        + VARINT_64_MAX_SIZE
                        + pack_size(e::slice(m_table))
                        + pack_size(e::slice(m_key))
                        + pack_size(m_op);
        comm_id id = m_ss.next();

        if (id == comm_id())
        {
            PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers available";
            cl->add_to_returnable(this);
            return;
        }

        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << UNSAFE_LOCK_OP
            << e::pack_varint(nonce)
            << e::slice(m_table)
            << e::slice(m_key)
            << m_op;

        if (cl->send(nonce, id, msg, this))
        {
            return;
        }
    }
}
