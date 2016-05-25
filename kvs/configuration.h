// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_configuration_h_
#define consus_kvs_configuration_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"
#include "common/ring.h"

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
        bool hash(data_center_id dc, unsigned index,
                  comm_id replicas[CONSUS_MAX_REPLICATION_FACTOR],
                  unsigned* num_replicas);
        void map(data_center_id dc, unsigned index,
                 comm_id* owner, comm_id* next_owner);

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
