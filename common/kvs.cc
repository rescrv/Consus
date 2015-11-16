// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/kvs.h"

using consus::kvs;

kvs :: kvs()
    : id()
    , bind_to()
    , dc()
{
}

kvs :: kvs(comm_id i, const po6::net::location& b)
    : id(i)
    , bind_to(b)
    , dc()
{
}

kvs :: kvs(const kvs& other)
    : id(other.id)
    , bind_to(other.bind_to)
    , dc(other.dc)
{
}

kvs :: ~kvs() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& lhs, const kvs& rhs)
{
    return lhs << "kvs(id=" << rhs.id.get() << ", bind_to=" << rhs.bind_to << ", dc=" << rhs.dc.get() << ")";
}

e::packer
consus :: operator << (e::packer lhs, const kvs& rhs)
{
    return lhs << rhs.id << rhs.bind_to << rhs.dc;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, kvs& rhs)
{
    return lhs >> rhs.id >> rhs.bind_to >> rhs.dc;
}
