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

#ifndef consus_coordinator_coordinator_h_
#define consus_coordinator_coordinator_h_

// STL
#include <set>

// Replicant
#include <rsm.h>

// consus
#include "namespace.h"
#include "common/data_center.h"
#include "common/ids.h"
#include "common/kvs.h"
#include "common/kvs_state.h"
#include "common/paxos_group.h"
#include "common/ring.h"
#include "common/txman.h"
#include "common/txman_state.h"

BEGIN_CONSUS_NAMESPACE

class coordinator
{
    public:
        coordinator();
        ~coordinator() throw ();

    public:
        void init(rsm_context* ctx, uint64_t token);
        cluster_id cluster() const { return m_cluster; }
        void invariant_check(rsm_context* ctx);

    // data centers
    public:
        data_center* get_data_center(data_center_id id);
        data_center* get_data_center(const std::string& name);
        data_center* new_data_center(const std::string& name);
        void data_center_create(rsm_context* ctx, const std::string& name);
        void data_center_default(rsm_context* ctx, const std::string& name);

    // transaction managers
    public:
        txman_state* get_txman(comm_id tx);
        txman_state* new_txman(const txman& t);
        void txman_register(rsm_context* ctx, const txman& t, const std::string& data_center);
        void txman_online(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce);
        void txman_offline(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce);

    // key value stores
    public:
        kvs_state* get_kvs(comm_id tx);
        kvs_state* new_kvs(const kvs& t);
        void kvs_register(rsm_context* ctx, const kvs& t, const std::string& data_center);
        void kvs_online(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce);
        void kvs_offline(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce);
        void kvs_migrated(rsm_context* ctx, partition_id part);

    // maintenance
    public:
        void is_stable(rsm_context* ctx);
        void tick(rsm_context* ctx);

    // backup/restore
    public:
        static coordinator* recreate(rsm_context* ctx,
                                     const char* data, size_t data_sz);
        int snapshot(rsm_context* ctx,
                     char** data, size_t* data_sz);

    // utilities
    private:
        void generate_next_configuration(rsm_context* ctx);
        void txman_availability_changed();
        void kvs_availability_changed();
        void regenerate_paxos_groups(rsm_context* ctx);
        ring* get_or_create_ring(data_center_id id);
        void maintain_kvs_rings(rsm_context* ctx);
        bool finish_migrations(rsm_context* ctx);

    private:
        // meta state
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_flags;
        uint64_t m_counter;
        // data centers
        data_center_id m_dc_default;
        std::vector<data_center> m_dcs;
        // transaction managers
        std::vector<txman_state> m_txmans;
        // transaction manager groups
        std::vector<paxos_group> m_txman_groups;
        unsigned m_txman_quiescence_counter;
        bool m_txmans_changed;
        // key value stores
        std::vector<kvs_state> m_kvss;
        unsigned m_kvs_quiescence_counter;
        bool m_kvss_changed;
        // rings
        std::vector<ring> m_rings;
        std::vector<partition_id> m_migrated;

    private:
        coordinator(const coordinator&);
        coordinator& operator = (const coordinator&);
};

END_CONSUS_NAMESPACE

#endif // consus_coordinator_coordinator_h_
