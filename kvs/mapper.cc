// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "kvs/daemon.h"
#include "kvs/mapper.h"

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
    configuration* c = m_d->get_config();

    if (c && c->exists(comm_id(server_id)))
    {
        *bound_to = c->get_address(comm_id(server_id));
        return true;
    }

    return false;
}
