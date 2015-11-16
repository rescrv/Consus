// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/client_configuration.h"
#include "client/configuration.h"

using consus::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_txmans()
{
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_flags(other.m_flags)
    , m_txmans(other.m_txmans)
{
}

configuration :: ~configuration() throw ()
{
}

po6::net::location
configuration :: get_address(const comm_id& id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].id == id)
        {
            return m_txmans[i].bind_to;
        }
    }

    return po6::net::location();
}

void
configuration :: initialize(server_selector* ss)
{
    std::vector<comm_id> ids;

    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        ids.push_back(m_txmans[i].id);
    }

    ss->set(&ids[0], ids.size());
}

configuration&
configuration :: operator = (const configuration& rhs)
{
    if (this != &rhs)
    {
        m_cluster = rhs.m_cluster;
        m_version = rhs.m_version;
        m_flags = rhs.m_flags;
        m_txmans = rhs.m_txmans;
    }

    return *this;
}

e::unpacker
consus :: operator >> (e::unpacker up, configuration& rhs)
{
    return client_configuration(up, &rhs.m_cluster, &rhs.m_version, &rhs.m_flags, &rhs.m_txmans);
}
