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

#ifndef consus_kvs_write_replicator_h_
#define consus_kvs_write_replicator_h_

// po6
#include <po6/threads/mutex.h>

// e
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class write_replicator
{
    public:
        write_replicator(uint64_t key);
        virtual ~write_replicator() throw ();

    public:
        uint64_t state_key();
        bool finished();

    public:
        void init(comm_id id, uint64_t nonce, unsigned flags,
                  const e::slice& table, const e::slice& key,
                  uint64_t timestamp, const e::slice& value,
                  std::auto_ptr<e::buffer> msg);
        void response(comm_id id, consus_returncode rc,
                      const replica_set& rs, daemon* d);
        void externally_work_state_machine(daemon* d);
        std::string debug_dump();

    private:
        struct write_stub;

    private:
        std::string logid();
        // creating a stub may invalidate any pointers previously returned by
        // get_*stub functions
        //
        // it is safe to call these with comm_id(), but they will always return
        // NULL for the default comm_id()
        write_stub* get_stub(comm_id id);
        write_stub* get_or_create_stub(comm_id id);
        void ensure_stub_exists(comm_id id) { get_or_create_stub(id); }
        void work_state_machine(daemon* d);
        bool returncode_is_final(consus_returncode rc);
        void send_write_request(write_stub* stub, uint64_t now, daemon* d);

    private:
        const uint64_t m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        bool m_finished;
        comm_id m_id;
        uint64_t m_nonce;
        unsigned m_flags;
        e::slice m_table;
        e::slice m_key;
        uint64_t m_timestamp;
        e::slice m_value;
        std::auto_ptr<e::buffer> m_backing;
        std::vector<write_stub> m_requests;
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_write_replicator_h_
