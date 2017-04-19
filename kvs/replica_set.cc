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
#include "kvs/replica_set.h"

using consus::replica_set;

replica_set :: replica_set()
    : num_replicas(0)
    , desired_replication(0)
{
    for (size_t i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        replicas[i] = comm_id();
        transitioning[i] = comm_id();
    }
}

replica_set :: ~replica_set() throw ()
{
}

unsigned
replica_set :: index(comm_id id) const
{
    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        if (replicas[i] == id)
        {
            return i;
        }
    }

    return CONSUS_MAX_REPLICATION_FACTOR;
}

bool
consus :: replica_sets_agree(comm_id target,
                             const replica_set& a,
                             const replica_set& b)
{
    unsigned idx_a = a.index(target);
    unsigned idx_b = b.index(target);
    assert(idx_a >= CONSUS_MAX_REPLICATION_FACTOR || a.replicas[idx_a] == target);
    assert(idx_b >= CONSUS_MAX_REPLICATION_FACTOR || b.replicas[idx_b] == target);
    return idx_a < CONSUS_MAX_REPLICATION_FACTOR &&
           idx_b < CONSUS_MAX_REPLICATION_FACTOR &&
           a.replicas[idx_a] == b.replicas[idx_b] &&
           a.transitioning[idx_a] == b.transitioning[idx_b];
}

bool
consus :: operator == (const replica_set& lhs, const replica_set& rhs)
{
    if (lhs.num_replicas != rhs.num_replicas ||
        lhs.desired_replication != rhs.desired_replication)
    {
        return false;
    }

    for (size_t i = 0; i < lhs.num_replicas; ++i)
    {
        if (lhs.replicas[i] != rhs.replicas[i] ||
            lhs.transitioning[i] != rhs.transitioning[i])
        {
            return false;
        }
    }

    return true;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const replica_set& rhs)
{
    return lhs << "replica_set(XXX)"; // XXX
}

e::packer
consus :: operator << (e::packer lhs, const replica_set& rhs)
{
    return lhs << e::pack_varint(rhs.num_replicas)
               << e::pack_varint(rhs.desired_replication)
               << e::pack_array<comm_id>(rhs.replicas, rhs.num_replicas)
               << e::pack_array<comm_id>(rhs.transitioning, rhs.num_replicas);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, replica_set& rhs)
{
    uint64_t num_replicas = 0;
    uint64_t desired_replication = 0;
    lhs = lhs >> e::unpack_varint(num_replicas)
              >> e::unpack_varint(desired_replication);
    rhs.num_replicas = num_replicas;
    rhs.desired_replication = desired_replication;
    lhs = lhs >> e::unpack_array<comm_id>(rhs.replicas, rhs.num_replicas)
              >> e::unpack_array<comm_id>(rhs.transitioning, rhs.num_replicas);
    return lhs;
}

size_t
consus :: pack_size(const replica_set& r)
{
    return e::varint_length(r.num_replicas)
         + e::varint_length(r.desired_replication)
         + 2 * r.num_replicas * sizeof(uint64_t);
}
