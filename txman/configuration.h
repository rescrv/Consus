// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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
