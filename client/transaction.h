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

#ifndef consus_client_transaction_h_
#define consus_client_transaction_h_

// C
#include <stdint.h>

// e
#include <e/error.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/transaction_id.h"

BEGIN_CONSUS_NAMESPACE
class client;

class transaction
{
    public:
        transaction(client* cl, const transaction_id& txid,
                    const comm_id* ids, size_t ids_sz);
        ~transaction() throw ();

    public:
        transaction_id txid() { return m_txid; }
        client* parent() { return m_cl; }
        int64_t get(const char* table,
                    const char* key, size_t key_sz,
                    consus_returncode* status,
                    char** value, size_t* value_sz);
        int64_t put(const char* table,
                    const char* key, size_t key_sz,
                    const char* value, size_t value_sz,
                    consus_returncode* status);
        int64_t commit(consus_returncode* status);
        int64_t abort(consus_returncode* status);
        void initialize(server_selector* ss);
        void mark_aborted();

    private:
        client* const m_cl;
        const transaction_id m_txid;
        const std::vector<comm_id> m_ids;
        uint64_t m_next_slot;

    private:
        transaction(const transaction&);
        transaction& operator = (const transaction&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_transaction_h_
