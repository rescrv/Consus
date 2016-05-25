// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// consus
#include "common/ring.h"

#pragma GCC diagnostic ignored "-Wlarger-than="

using consus::ring;

ring :: ring()
    : dc()
{
    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        partitions[i].index = i;
    }
}

ring :: ring(data_center_id _dc)
    : dc(_dc)
{
    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        partitions[i].index = i;
    }
}

ring :: ~ring() throw ()
{
}

void
ring :: get_owners(comm_id owners[CONSUS_KVS_PARTITIONS])
{
    for (unsigned i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        owners[i] = partitions[i].owner;
    }
}

void
ring :: set_owners(comm_id owners[CONSUS_KVS_PARTITIONS], uint64_t* counter)
{
    for (unsigned i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        partition* part = &partitions[i];

        if (part->owner == comm_id())
        {
            part->id = partition_id(*counter);
            ++*counter;
            part->owner = owners[i];
            part->next_id = partition_id();
            part->next_owner = comm_id();
        }
        else if (part->owner == owners[i])
        {
            part->next_id = partition_id();
            part->next_owner = comm_id();
        }
        else if (part->next_owner != owners[i])
        {
            part->next_id = partition_id(*counter);
            ++*counter;
            part->next_owner = owners[i];
        }
    }
}

e::packer
consus :: operator << (e::packer lhs, const ring& rhs)
{
    return lhs << rhs.dc << e::pack_array<partition>(rhs.partitions, CONSUS_KVS_PARTITIONS);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, ring& rhs)
{
    return lhs >> rhs.dc >> e::unpack_array<partition>(rhs.partitions, CONSUS_KVS_PARTITIONS);
}
