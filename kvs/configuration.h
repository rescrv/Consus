// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_configuration_h_
#define consus_kvs_configuration_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"

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
        po6::net::location get_address(comm_id id) const;
        kvs_state::state_t get_state(comm_id id) const;

    // debug/internal
    public:
        std::string dump() const;

    private:
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_flags;
        std::vector<kvs_state> m_kvss;

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
