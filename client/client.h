// Copyright (c) 2015-2017, Robert Escriva, Cornell University
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

#ifndef consus_client_client_h_
#define consus_client_client_h_

// C
#include <stdint.h>

// STL
#include <map>
#include <list>

// e
#include <e/error.h>
#include <e/flagfd.h>

// BusyBee
#include <busybee.h>

// Replicant
#include <replicant.h>

// consus
#include <consus.h>
#include <consus-admin.h>
#include "namespace.h"
#include "client/configuration.h"
#include "client/controller.h"
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE

class client
{
    public:
        client(const char* host, uint16_t port);
        client(const char* conn_str);
        ~client() throw ();

    public:
        // public API
        int64_t loop(int timeout, consus_returncode* status);
        int64_t wait(int64_t id, int timeout, consus_returncode* status);
        int64_t begin_transaction(consus_returncode* status,
                                  consus_transaction** xact);
        int64_t unsafe_get(const char* table,
                           const char* key, size_t key_sz,
                           consus_returncode* status,
                           char** value, size_t* value_sz);
        int64_t unsafe_put(const char* table,
                           const char* key, size_t key_sz,
                           const char* value, size_t value_sz,
                           consus_returncode* status);
        int64_t unsafe_lock(const char* table,
                            const char* key, size_t key_sz,
                            consus_returncode* status);
        int64_t unsafe_unlock(const char* table,
                              const char* key, size_t key_sz,
                              consus_returncode* status);
        // admin API
        int create_data_center(const char* name, consus_returncode* status);
        int set_default_data_center(const char* name, consus_returncode* status);
        int availability_check(consus_availability_requirements* reqs,
                               int timeout, consus_returncode* status);
        // internal semi-public API
        int debug_client_configuration(consus_returncode* status, const char** str);
        int debug_txman_configuration(consus_returncode* status, const char** str);
        int debug_kvs_configuration(consus_returncode* status, const char** str);
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);
        e::error* set_error_message() { return &m_last_error; }

    public:
        uint64_t generate_new_nonce();
        int64_t generate_new_client_id();
        void initialize(server_selector* ss);
        void add_to_returnable(pending* p);
        bool send(uint64_t nonce, comm_id id, std::auto_ptr<e::buffer> msg, pending* p);
        void handle_disruption(const comm_id& id);
        bool replicant_finish(int64_t id, replicant_returncode* rc, consus_returncode* status);
        bool replicant_finish(int64_t id, int timeout, replicant_returncode* rc, consus_returncode* status);

    private:
        friend class transaction;
        // returns the ID of something that made progress; does not guarantee
        // that it can return, so verify that at the callsite
        int64_t inner_loop(int timeout, consus_returncode* status);
        int64_t post_loop(consus_returncode* status);
        bool maintain_coord_connection(consus_returncode* status);

    private:
        // configuration
        replicant_client* m_coord;
        configuration m_config;
        int64_t m_config_id;
        replicant_returncode m_config_status;
        uint64_t m_config_state;
        char* m_config_data;
        size_t m_config_data_sz;
        // communication
        controller m_busybee_controller;
        const std::auto_ptr<busybee_client> m_busybee;
        // nonces
        int64_t m_next_client_id;
        uint64_t m_next_server_nonce;
        // operations
        std::map<std::pair<comm_id, uint64_t>, e::intrusive_ptr<pending> > m_pending;
        std::list<e::intrusive_ptr<pending> > m_returnable;
        e::intrusive_ptr<pending> m_returned;
        // misc
        e::flagfd m_flagfd;
        e::error m_last_error;

    private:
        client(const client&);
        client& operator = (const client&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_client_h_
