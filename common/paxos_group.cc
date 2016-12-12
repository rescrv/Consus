// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
