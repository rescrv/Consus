// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// C
#include "client/server_selector.h"

using consus::server_selector;

server_selector :: server_selector()
    : m_ids()
    , m_consumed_idx()
{
}

server_selector :: ~server_selector() throw ()
{
}

void
server_selector :: set(const comm_id* ids, size_t ids_sz)
{
    m_ids = std::vector<comm_id>(ids, ids + ids_sz);
    m_consumed_idx = 0;
}

consus::comm_id
server_selector :: next()
{
    if (m_consumed_idx >= m_ids.size())
    {
        return comm_id();
    }

    return m_ids[m_consumed_idx++];
}
