// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "txman/daemon.h"
#include "txman/mapper.h"

using consus::mapper;

mapper :: mapper(daemon* d)
    : m_d(d)
{
}

mapper :: ~mapper() throw ()
{
}

bool
mapper :: lookup(uint64_t server_id, po6::net::location* bound_to)
{
    po6::net::location loc;

    if (m_d->m_config->exists(comm_id(server_id)))
    {
        *bound_to = m_d->m_config->get_address(comm_id(server_id));
        return true;
    }

    return false;
}
