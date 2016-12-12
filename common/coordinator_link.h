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

#ifndef consus_common_coordinator_link_h_
#define consus_common_coordinator_link_h_

// STL
#include <string>

// po6
#include <po6/net/location.h>
#include <po6/threads/mutex.h>

// replicant
#include <replicant.h>

// consus
#include "namespace.h"
#include "common/coordinator_returncode.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class coordinator_link
{
    public:
        class callback;

    public:
        coordinator_link(const std::string& rendezvous,
                         comm_id id, po6::net::location bind_to,
                         const std::string& data_center,
                         callback* c/*ownership not transferred*/);
        ~coordinator_link() throw ();

    // registration:  call these before going into steady state
    // call either "initial_registration" or "establish"
    // other calls are options that affect behavior
    public:
        // claim the unique token and establish a connection
        bool initial_registration();
        // establish a connection
        bool establish();

        // claim the token again in steady state if removed; default is to set
        // "orphaned" and cease further activity (expecting the process to
        // self-terminate).
        void allow_reregistration();

    // maintenance:  steady-state operation
    public:
        // keep connected to the coordinator and pull new configs
        // returns when a maintenance call will return a different value than
        // prior to the call, or a callback has been made on "c"
        void maintain_connection();
        // a permanent error has occurred and the connection will never recover
        bool error();
        // has this instance's comm_id been removed from the configuration?
        bool orphaned();

    // piggy-back on the replicant connection
    public:
        bool call(const char* func, const char* input, size_t input_sz, coordinator_returncode* coord);
        void fire_and_forget(const char* func, const char* input, size_t input_sz);

    private:
        void invariant_check();
        bool call_no_lock(const char* func, const char* input, size_t input_sz, coordinator_returncode* coord);
        bool registration();
        bool online();

    private:
        po6::threads::mutex m_mtx;
        replicant_client* const m_repl;
        const comm_id m_id;
        const po6::net::location m_bind_to;
        const std::string m_data_center;
        callback* const m_cb;
        int64_t m_config_id;
        replicant_returncode m_config_status;
        uint64_t m_config_state;
        char* m_config_data;
        size_t m_config_data_sz;
        uint64_t m_last_config_state;
        bool m_last_config_valid;
        uint64_t m_last_online_call;
        replicant_returncode m_faf_status;
        bool m_allow_rereg;
        bool m_error;
        bool m_orphaned;
        bool m_online_once;
        uint64_t m_backoff;

    private:
        coordinator_link(const coordinator_link&);
        coordinator_link& operator = (const coordinator_link&);
};

class coordinator_link::callback
{
    public:
        callback();
        virtual ~callback() throw ();

    public:
        virtual std::string prefix() = 0;
        virtual bool new_config(const char* data, size_t data_sz) = 0;
        virtual bool has_id(comm_id id) = 0;
        virtual po6::net::location address(comm_id id) = 0;
        virtual bool is_steady_state(comm_id id) = 0;
};

END_CONSUS_NAMESPACE

#endif // consus_common_coordinator_link_h_
