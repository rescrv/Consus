// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/txman.h"

using consus::txman;

txman :: txman()
    : id()
    , bind_to()
    , dc()
{
}

txman :: txman(comm_id i, const po6::net::location& b)
    : id(i)
    , bind_to(b)
    , dc()
{
}

txman :: txman(const txman& other)
    : id(other.id)
    , bind_to(other.bind_to)
    , dc(other.dc)
{
}

txman :: ~txman() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& lhs, const txman& rhs)
{
    return lhs << "txman(id=" << rhs.id.get() << ", bind_to=" << rhs.bind_to << ", dc=" << rhs.dc.get() << ")";
}

e::packer
consus :: operator << (e::packer lhs, const txman& rhs)
{
    return lhs << rhs.id << rhs.bind_to << rhs.dc;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, txman& rhs)
{
    return lhs >> rhs.id >> rhs.bind_to >> rhs.dc;
}
