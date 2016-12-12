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

#ifndef consus_txman_kvs_write_h_
#define consus_txman_kvs_write_h_

// STL
#include <memory>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/buffer.h>
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class kvs_write
{
    public:
        kvs_write(const uint64_t& sk);
        ~kvs_write() throw ();

    public:
        const uint64_t& state_key() const;
        bool finished();

    public:
        void write(unsigned flags,
                   const e::slice& table,
                   const e::slice& key,
                   uint64_t timestamp,
                   const e::slice& value,
                   daemon* d);
        void response(consus_returncode rc, daemon* d);
        void callback_client(comm_id client, uint64_t nonce);
        void callback_transaction(const transaction_group& tg, uint64_t seqno,
                                  void (transaction::*func)(consus_returncode, uint64_t, daemon*));

    private:
        const uint64_t m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        bool m_finished;
        // client callback
        comm_id m_client;
        uint64_t m_client_nonce;
        // transaction callback
        transaction_group m_tx_group;
        uint64_t m_tx_seqno;
        void (transaction::*m_tx_func)(consus_returncode, uint64_t, daemon*);

    private:
        kvs_write(const kvs_write&);
        kvs_write& operator = (const kvs_write&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_kvs_write_h_
