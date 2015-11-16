// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/macros.h"
#include "common/paxos_group.h"

using consus::paxos_group;

paxos_group :: paxos_group()
    : id()
    , dc()
    , members_sz(0)
{
    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        members[i] = comm_id();
    }
}

paxos_group :: paxos_group(const paxos_group& other)
    : id(other.id)
    , dc(other.dc)
    , members_sz(other.members_sz)
{
    for (unsigned i = 0; i < members_sz; ++i)
    {
        members[i] = other.members[i];
    }

    for (unsigned i = members_sz; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        members[i] = comm_id();
    }
}

paxos_group :: ~paxos_group() throw ()
{
}

unsigned
paxos_group :: quorum() const
{
    return members_sz / 2 + 1;
}

unsigned
paxos_group :: index(comm_id c) const
{
    unsigned i = 0;

    for (; i < members_sz; ++i)
    {
        if (members[i] == c)
        {
            break;
        }
    }

    return i;
}

paxos_group&
paxos_group :: operator = (const paxos_group& rhs)
{
    if (this != &rhs)
    {
        id = rhs.id;
        dc = rhs.dc;
        members_sz = rhs.members_sz;

        for (unsigned i = 0; i < members_sz; ++i)
        {
            members[i] = rhs.members[i];
        }

        for (unsigned i = members_sz; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
        {
            members[i] = comm_id();
        }
    }

    return *this;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const paxos_group& rhs)
{
    lhs << "paxos_group(id=" << rhs.id.get() << ", dc=" << rhs.dc.get() << ", [";

    for (unsigned i = 0; i < rhs.members_sz; ++i)
    {
        if (i > 0)
        {
            lhs << ", ";
        }

        lhs << rhs.members[i].get();
    }

    lhs << "])";
    return lhs;
}

e::packer
consus :: operator << (e::packer pa, const paxos_group& rhs)
{
    pa = pa << rhs.id << rhs.dc << e::pack_varint(rhs.members_sz);

    for (unsigned i = 0; i < rhs.members_sz; ++i)
    {
        pa = pa << rhs.members[i];
    }

    return pa;
}

e::unpacker
consus :: operator >> (e::unpacker up, paxos_group& rhs)
{
    uint64_t sz = 0;
    up = up >> rhs.id >> rhs.dc >> e::unpack_varint(sz);

    if (sz > CONSUS_MAX_REPLICATION_FACTOR)
    {
        return e::unpacker::error_out();
    }

    rhs.members_sz = sz;

    for (unsigned i = 0; i < rhs.members_sz; ++i)
    {
        up = up >> rhs.members[i];
    }

    for (unsigned i = rhs.members_sz; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        rhs.members[i] = comm_id();
    }

    return up;
}
