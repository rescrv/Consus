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

#ifndef consus_txman_configuration_h_
#define consus_txman_configuration_h_

// consus
#include "namespace.h"
#include "common/data_center.h"
#include "common/ids.h"
#include "common/kvs.h"
#include "common/paxos_group.h"
#include "common/txman.h"
#include "common/txman_state.h"

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
        uint64_t timestamp_bottom() const { return 0; /* XXX */ }

    // transaction managers
    public:
        bool exists(comm_id id) const;
        data_center_id get_data_center(comm_id id) const;
        po6::net::location get_address(comm_id id) const;
        txman_state::state_t get_state(comm_id id) const;

    // transaction manager paxos groups
    public:
        std::vector<paxos_group_id> groups_for(comm_id id) const;
        const paxos_group* get_group(paxos_group_id id) const;
        bool is_member(paxos_group_id g, comm_id id) const;
        bool choose_groups(paxos_group_id g, std::vector<paxos_group_id>* groups) const;
        comm_id first_alive(paxos_group_id id) const; // XXX coordinator failure sensitive

    // key-value stores
    public:
        comm_id choose_kvs(data_center_id dc) const;

    // debug/internal
    public:
        std::string dump() const;

    private:
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_flags;
        std::vector<data_center> m_dcs;
        std::vector<txman_state> m_txmans;
        std::vector<paxos_group> m_paxos_groups;
        std::vector<kvs> m_kvss;

    private:
        configuration(const configuration& other);
        configuration& operator = (const configuration& rhs);
};

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);
e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);

END_CONSUS_NAMESPACE

#endif // consus_txman_configuration_h_
