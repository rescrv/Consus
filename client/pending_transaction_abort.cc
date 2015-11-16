// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "client/client.h"
#include "client/pending_transaction_abort.h"
#include "client/transaction.h"

using consus::pending_transaction_abort;

pending_transaction_abort :: pending_transaction_abort(int64_t client_id,
                                                       consus_returncode* status,
                                                       transaction* xact,
                                                       uint64_t slot)
    : pending(client_id, status)
    , m_xact(xact)
    , m_ss()
    , m_slot(slot)
{
}

pending_transaction_abort :: ~pending_transaction_abort() throw ()
{
}

std::string
pending_transaction_abort :: describe()
{
    std::ostringstream ostr;
    ostr << "pending_transaction_abort(id=" << m_xact->txid() << "\")";
    return ostr.str();
}

void
pending_transaction_abort :: kickstart_state_machine(client* cl)
{
    m_xact->initialize(&m_ss);
    send_request(cl);
}

void
pending_transaction_abort :: handle_server_failure(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_transaction_abort :: handle_server_disruption(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_transaction_abort :: handle_busybee_op(client* cl,
                                               uint64_t,
                                               std::auto_ptr<e::buffer>,
                                               e::unpacker up)
{
    consus_returncode rc;
    up = up >> rc;

    if (up.error())
    {
        abort(); // XXX
    }

    this->success(); // XXX
    cl->add_to_returnable(this);
}

void
pending_transaction_abort :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = m_xact->parent()->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(TXMAN_ABORT)
                        + pack_size(m_xact->txid())
                        + 2 * VARINT_64_MAX_SIZE;
        comm_id id = m_ss.next();

        if (id == comm_id())
        {
            m_xact->mark_aborted();
            PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers to ensure durability";
            cl->add_to_returnable(this);
            return;
        }

        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << TXMAN_ABORT << m_xact->txid()
            << e::pack_varint(nonce)
            << e::pack_varint(m_slot);

        if (cl->send(nonce, id, msg, this))
        {
            return;
        }
    }
}
