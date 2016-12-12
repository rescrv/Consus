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

#ifndef consus_kvs_lock_manager_h_
#define consus_kvs_lock_manager_h_

// e
#include <e/compat.h>

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_group.h"
#include "kvs/lock_state.h"
#include "kvs/table_key_pair.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class lock_manager
{
    public:
        lock_manager(e::garbage_collector* gc);
        ~lock_manager() throw ();

    public:
        void lock(comm_id id, uint64_t nonce,
                  const e::slice& table, const e::slice& key,
                  const transaction_group& tg, daemon* d);
        void unlock(comm_id id, uint64_t nonce,
                    const e::slice& table, const e::slice& key,
                    const transaction_group& tg, daemon* d);
        std::string debug_dump();

    private:
        typedef e::state_hash_table<table_key_pair, lock_state> lock_map_t;

    private:
        lock_map_t m_locks;

    private:
        lock_manager(const lock_manager&);
        lock_manager& operator = (const lock_manager&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_lock_manager_h_
