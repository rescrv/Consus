// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#include "common/kvs_configuration.h"
#include "kvs/configuration.h"

using consus::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_kvss()
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: exists(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return true;
        }
    }

    return false;
}

po6::net::location
configuration :: get_address(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return m_kvss[i].kv.bind_to;
        }
    }

    return po6::net::location();
}

consus::kvs_state::state_t
configuration :: get_state(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return m_kvss[i].state;
        }
    }

    return kvs_state::state_t();
}

std::string
configuration :: dump() const
{
    return kvs_configuration(m_cluster, m_version, m_flags, m_kvss);
}

e::unpacker
consus :: operator >> (e::unpacker up, configuration& c)
{
    return kvs_configuration(up, &c.m_cluster, &c.m_version, &c.m_flags, &c.m_kvss);
}
