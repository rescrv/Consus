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

// e
#include <e/strescape.h>

// treadstone
#include <treadstone.h>

// BusyBee
#include <busybee.h>

// consus
#include "common/consus.h"
#include "client/client.h"
#include "client/pending_transaction_read.h"
#include "client/transaction.h"

using consus::pending_transaction_read;

pending_transaction_read :: pending_transaction_read(int64_t client_id,
                                                     consus_returncode* status,
                                                     transaction* xact,
                                                     uint64_t slot,
                                                     const char* table,
                                                     const unsigned char* key, size_t key_sz,
                                                     char** value, size_t* value_sz)
    : pending(client_id, status)
    , m_xact(xact)
    , m_ss()
    , m_slot(slot)
    , m_table(table)
    , m_key(key, key + key_sz)
    , m_value(value)
    , m_value_sz(value_sz)
{
}

pending_transaction_read :: ~pending_transaction_read() throw ()
{
}

std::string
pending_transaction_read :: describe()
{
    std::ostringstream ostr;
    ostr << "pending_transaction_read(id=" << m_xact->txid()
         << ", table=\"" << e::strescape(m_table)
         << "\", key=\"" << e::strescape(m_key) << "\")";
    return ostr.str();
}

void
pending_transaction_read :: kickstart_state_machine(client* cl)
{
    m_xact->initialize(&m_ss);
    send_request(cl);
}

void
pending_transaction_read :: handle_server_failure(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_transaction_read :: handle_server_disruption(client* cl, comm_id)
{
    send_request(cl);
}

void
pending_transaction_read :: handle_busybee_op(client* cl,
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
        m_xact->mark_aborted();
        PENDING_ERROR(SERVER_ERROR) << "server sent a corrupt response to \"transaction-read\"";
        cl->add_to_returnable(this);
        return;
    }

    if (rc != CONSUS_SUCCESS && rc != CONSUS_NOT_FOUND)
    {
        m_xact->mark_aborted();
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
pending_transaction_read :: send_request(client* cl)
{
    while (true)
    {
        const uint64_t nonce = m_xact->parent()->generate_new_nonce();
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(TXMAN_READ)
                        + pack_size(m_xact->txid())
                        + 2 * VARINT_64_MAX_SIZE
                        + pack_size(e::slice(m_table))
                        + pack_size(e::slice(m_key));
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
            << TXMAN_READ << m_xact->txid()
            << e::pack_varint(nonce)
            << e::pack_varint(m_slot)
            << e::slice(m_table)
            << e::slice(m_key);

        if (cl->send(nonce, id, msg, this))
        {
            return;
        }
    }
}
