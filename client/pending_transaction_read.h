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

#ifndef consus_client_pending_transaction_read_h_
#define consus_client_pending_transaction_read_h_

// consus
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE
class transaction;

class pending_transaction_read : public pending
{
    public:
        pending_transaction_read(int64_t client_id,
                                 consus_returncode* status,
                                 transaction* xact,
                                 uint64_t slot,
                                 const char* table,
                                 const unsigned char* key, size_t key_sz,
                                 char** value, size_t* value_sz);
        virtual ~pending_transaction_read() throw ();

    public:
        virtual std::string describe();
        virtual void kickstart_state_machine(client* cl);
        virtual void handle_server_failure(client* cl, comm_id si);
        virtual void handle_server_disruption(client* cl, comm_id si);
        virtual void handle_busybee_op(client* cl,
                                       uint64_t nonce,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up);

    private:
        void send_request(client* cl);

    private:
        transaction* m_xact;
        server_selector m_ss;
        const uint64_t m_slot;
        std::string m_table;
        std::string m_key;
        char** m_value;
        size_t* m_value_sz;

    private:
        pending_transaction_read(const pending_transaction_read&);
        pending_transaction_read& operator = (const pending_transaction_read&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_transaction_read_h_
