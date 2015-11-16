// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "client/mapper.h"

using consus::mapper;

mapper :: mapper(const configuration* config)
    : m_config(config)
{
}

mapper :: ~mapper() throw ()
{
}

bool
mapper :: lookup(uint64_t id, po6::net::location* addr)
{
    *addr = m_config->get_address(comm_id(id));
    return *addr != po6::net::location();
}
