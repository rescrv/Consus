// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
