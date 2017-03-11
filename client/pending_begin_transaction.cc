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

// BusyBee
#include <busybee.h>

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
