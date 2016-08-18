// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "common/transaction_id.h"
#include "client/client.h"
#include "client/pending_begin_transaction.h"
#include "client/transaction.h"

using consus::pending_begin_transaction;

pending_begin_transaction :: pending_begin_transaction(int64_t client_id,
                                                       consus_returncode* status,
                                                       consus_transaction** xact)
    : pending(client_id, status)
    , m_xact(xact)
    , m_ss()
{
    *m_xact = NULL;
}

pending_begin_transaction :: ~pending_begin_transaction() throw ()
{
}

std::string
pending_begin_transaction :: describe()
{
    return "pending_begin_transaction()";
}

void
pending_begin_transaction :: kickstart_state_machine(client* cl)
{
    cl->initialize(&m_ss);
    send_request(cl);
}

void
pending_begin_transaction :: handle_server_failure(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_begin_transaction :: handle_server_disruption(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_begin_transaction :: handle_busybee_op(client* cl,
                                               uint64_t,
                                               std::auto_ptr<e::buffer>,
                                               e::unpacker up)
{
    consus_returncode rc;
    transaction_id txid;
    std::vector<comm_id> ids;
    up = up >> rc >> txid >> ids;

    if (up.error())
    {
        PENDING_ERROR(SERVER_ERROR) << "server sent a corrupt response to \"begin-transaction\"";
        cl->add_to_returnable(this);
        return;
    }

    transaction* t = new transaction(cl, txid, &ids[0], ids.size());
    *m_xact = reinterpret_cast<consus_transaction*>(t);
    this->success();
    cl->add_to_returnable(this);
}

void
pending_begin_transaction :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = cl->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(TXMAN_BEGIN)
                        + VARINT_64_MAX_SIZE;
        comm_id id = m_ss.next();

        if (id == comm_id())
        {
            PENDING_ERROR(UNAVAILABLE) << "insufficient number of servers to ensure durability";
            cl->add_to_returnable(this);
            return;
        }

        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << TXMAN_BEGIN << e::pack_varint(nonce);

        if (cl->send(nonce, id, msg, this))
        {
            return;
        }
    }
}
