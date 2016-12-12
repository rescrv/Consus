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

// STL
#include <sstream>

// BusyBee
#include "busybee_constants.h"

// consus
#include "common/network_msgtype.h"
#include "kvs/configuration.h"
#include "kvs/daemon.h"
#include "kvs/lock_manager.h"
#include "kvs/replica_set.h"

using consus::lock_manager;

extern std::vector<std::string> split_by_newlines(std::string s);

lock_manager :: lock_manager(e::garbage_collector* gc)
    : m_locks(gc)
{
}

lock_manager :: ~lock_manager() throw ()
{
}

void
lock_manager :: lock(comm_id id, uint64_t nonce,
                     const e::slice& table, const e::slice& key,
                     const transaction_group& tg, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->enqueue_lock(id, nonce, tg, d);
}

void
lock_manager :: unlock(comm_id id, uint64_t nonce,
                       const e::slice& table, const e::slice& key,
                       const transaction_group& tg, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->unlock(id, nonce, tg, d);
}

std::string
lock_manager :: debug_dump()
{
    std::ostringstream ostr;

    for (lock_map_t::iterator it(&m_locks); it.valid(); ++it)
    {
        lock_state* s = *it;
        std::string debug = s->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            ostr << s->logid() << ": " << lines[i] << "\n";
        }
    }

    return ostr.str();
}
