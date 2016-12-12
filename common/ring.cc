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
