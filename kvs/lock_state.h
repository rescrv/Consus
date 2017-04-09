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

#ifndef consus_kvs_lock_state_h_
#define consus_kvs_lock_state_h_

// STL
#include <list>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/compat.h>

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_group.h"
#include "kvs/table_key_pair.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class lock_state
{
    public:
        lock_state(const table_key_pair& tk);
        ~lock_state() throw ();

    public:
        table_key_pair state_key();
        bool finished();

    public:
        void enqueue_lock(comm_id id, uint64_t nonce,
                          const transaction_group& tg,
                          daemon* d);
        void unlock(comm_id id, uint64_t nonce,
                    const transaction_group& tg,
                    daemon* d);
        std::string debug_dump();
        std::string logid();

    private:
        struct request;

    private:
        void invariant_check();
        bool ensure_initialized(daemon* d);
        void ordered_enqueue(const request& r);
        void send_wound(comm_id id, uint64_t nonce, uint8_t flags,
                        const transaction_group& tg,
                        daemon* d);
        void send_wound_drop(comm_id id, uint64_t nonce,
                             const transaction_group& tg,
                             daemon* d);
        void send_wound_abort(comm_id id, uint64_t nonce,
                              const transaction_group& tg,
                              daemon* d);
        void send_lock_held(const transaction_group& tg, daemon* d);
        void send_response(comm_id id, uint64_t nonce,
                           const transaction_group& tg,
                           daemon* d);

    private:
        const table_key_pair m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        transaction_group m_holder;
        std::list<request> m_reqs;

    private:
        lock_state(const lock_state&);
        lock_state& operator = (const lock_state&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_lock_state_h_
