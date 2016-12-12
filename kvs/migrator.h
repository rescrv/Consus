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

#ifndef consus_kvs_migrator_h_
#define consus_kvs_migrator_h_

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

class migrator
{
    public:
        migrator(partition_id key);
        virtual ~migrator() throw ();

    public:
        partition_id state_key();
        bool finished();

    public:
        void ack(version_id version, daemon* d);
        void externally_work_state_machine(daemon* d);
        void terminate();
        std::string debug_dump();

    private:
        enum state_t
        {
            UNINITIALIZED,
            CHECK_CONFIG,
            TRANSFER_DATA,
            TERMINATED
        };

    private:
        void ensure_initialized(daemon* d);
        void work_state_machine(daemon* d);
        void work_state_machine_check_config(daemon* d);
        void work_state_machine_transfer_data(daemon* d);

    private:
        const partition_id m_state_key;
        po6::threads::mutex m_mtx;
        version_id m_version;
        state_t m_state;
        uint64_t m_last_handshake;
        uint64_t m_last_coord_call;
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_migrator_h_
