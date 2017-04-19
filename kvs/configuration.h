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

#ifndef consus_kvs_configuration_h_
#define consus_kvs_configuration_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"
#include "common/ring.h"
#include "kvs/replica_set.h"

BEGIN_CONSUS_NAMESPACE

class configuration
{
    public:
        configuration();
        ~configuration() throw ();

    // metadata
    public:
        cluster_id cluster() const { return m_cluster; }
        version_id version() const { return m_version; }

    // kvs daemons
    public:
        bool exists(comm_id id) const;
        data_center_id get_data_center(comm_id id) const;
        po6::net::location get_address(comm_id id) const;
        kvs_state::state_t get_state(comm_id id) const;
        size_t daemons() const;

    // hashing
    public:
        bool hash(data_center_id dc,
                  const e::slice& table,
                  const e::slice& key,
                  replica_set* rs);

    // XXX these APIs could be better designed or use better datastructures;
    // reevaluate them and their consistency with respect to other calls in this
    // class.
    public:
        std::vector<comm_id> ids();
        std::vector<partition_id> migratable_partitions(comm_id id);
        comm_id owner_from_next_id(partition_id id);

    // debug/internal
    public:
        std::string dump() const;

    private:
        struct cached_ring;

    private:
        void reconstruct_cache();
        void lookup(ring* r, uint16_t idx, replica_set* rs);

    // XXX same as above xxx about APIs
    private:
        void migratable_partitions(comm_id id, ring* r, std::vector<partition_id>* parts);

    private:
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_flags;
        std::vector<kvs_state> m_kvss;
        std::vector<ring> m_rings;

        // cached data
        std::vector<replica_set> m_cached_replica_sets;
        std::vector<cached_ring> m_cached_rings;

    private:
        configuration(const configuration& other);
        configuration& operator = (const configuration& rhs);
};

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);
e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);

END_CONSUS_NAMESPACE

#endif // consus_kvs_configuration_h_
