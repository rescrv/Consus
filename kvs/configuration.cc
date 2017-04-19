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

// e
#include <e/endian.h>

// consus
#include "common/kvs_configuration.h"
#include "kvs/configuration.h"

using consus::configuration;

struct configuration::cached_ring
{
    cached_ring() : dc() {}
    ~cached_ring() throw () {}

    data_center_id dc;
    size_t replica_sets[CONSUS_KVS_PARTITIONS];
};

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_kvss()
    , m_rings()
    , m_cached_replica_sets()
    , m_cached_rings()
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: exists(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return true;
        }
    }

    return false;
}

consus::data_center_id
configuration :: get_data_center(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return m_kvss[i].kv.dc;
        }
    }

    return data_center_id();
}

po6::net::location
configuration :: get_address(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return m_kvss[i].kv.bind_to;
        }
    }

    return po6::net::location();
}

consus::kvs_state::state_t
configuration :: get_state(comm_id id) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].kv.id == id)
        {
            return m_kvss[i].state;
        }
    }

    return kvs_state::state_t();
}

size_t
configuration :: daemons() const
{
    return m_kvss.size();
}

bool
configuration :: hash(data_center_id dc,
                      const e::slice& /* table XXX */,
                      const e::slice& key,
                      replica_set* rs)
{
    // XXX use a better mapping scheme
    char buf[sizeof(uint16_t)];
    memset(buf, 0, sizeof(buf));
    memmove(buf, key.data(), key.size() < 2 ? key.size() : 2);
    uint16_t index;
    e::unpack16be(buf, &index);

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        if (m_cached_rings[i].dc == dc)
        {
            size_t r = m_cached_rings[i].replica_sets[index];
            assert(r < m_cached_replica_sets.size());
            *rs = m_cached_replica_sets[r];
            rs->desired_replication = 5;//XXX

            if (rs->num_replicas > rs->desired_replication)
            {
                rs->num_replicas = rs->desired_replication;
            }

            return true;
        }
    }

    return false;
}

std::vector<consus::comm_id>
configuration :: ids()
{
    std::vector<comm_id> kvss;

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        kvss.push_back(m_kvss[i].kv.id);
    }

    return kvss;
}

std::vector<consus::partition_id>
configuration :: migratable_partitions(comm_id id)
{
    std::vector<partition_id> parts;

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        migratable_partitions(id, &m_rings[i], &parts);
    }

    return parts;
}

consus::comm_id
configuration :: owner_from_next_id(partition_id id)
{
    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        for (unsigned p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
        {
            if (m_rings[i].partitions[p].next_id == id)
            {
                return m_rings[i].partitions[p].owner;
            }
        }
    }

    return comm_id();
}

std::string
configuration :: dump() const
{
    return kvs_configuration(m_cluster, m_version, m_flags, m_kvss, m_rings);
}

void
configuration :: reconstruct_cache()
{
    m_cached_replica_sets.clear();
    m_cached_rings.clear();
    m_cached_rings.resize(m_rings.size());

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        m_cached_rings[i].dc = m_rings[i].dc;

        for (size_t idx = 0; idx < CONSUS_KVS_PARTITIONS; ++idx)
        {
            replica_set rs;
            rs.desired_replication = CONSUS_MAX_REPLICATION_FACTOR;

            if (m_cached_replica_sets.empty() ||
                m_cached_replica_sets.back().replicas[0] != m_rings[i].partitions[idx].owner ||
                m_cached_replica_sets.back().transitioning[0] != m_rings[i].partitions[idx].next_owner)
            {
                lookup(&m_rings[i], idx, &rs);

                if (m_cached_replica_sets.empty() || m_cached_replica_sets.back() != rs)
                {
                    m_cached_replica_sets.push_back(rs);
                }
            }

            m_cached_rings[i].replica_sets[idx] = m_cached_replica_sets.size() - 1;
        }
    }
}

void
configuration :: lookup(ring* r, uint16_t idx, replica_set* rs)
{
    assert(rs->desired_replication <= CONSUS_MAX_REPLICATION_FACTOR);
    rs->num_replicas = 0;
    rs->replicas[0] = comm_id();

    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS && rs->num_replicas < rs->desired_replication; ++i)
    {
        partition* p = &r->partitions[(idx + i) % CONSUS_KVS_PARTITIONS];

        // if the partition is assigned, we haven't wrapped around, and it's not
        // the same as the previous partition
        if (p->owner != comm_id() && p->owner != rs->replicas[0] &&
            (rs->num_replicas == 0 || rs->replicas[rs->num_replicas - 1] != p->owner))
        {
            rs->replicas[rs->num_replicas] = p->owner;
            rs->transitioning[rs->num_replicas] = p->next_owner;
            ++rs->num_replicas;
        }
    }
}

void
configuration :: migratable_partitions(comm_id id, ring* r, std::vector<partition_id>* parts)
{
    const partition* end_of_ring = r->partitions + CONSUS_KVS_PARTITIONS;
    partition* ptr = NULL;

    for (unsigned p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
    {
        if (r->partitions[p].owner == id)
        {
            ptr = &r->partitions[p];
            break;
        }
    }

    if (ptr)
    {
        // add partition directly in front of ptr if we're commandeering it
        if (ptr - r->partitions > 0 &&
            (ptr - 1)->next_owner == id)
        {
            parts->push_back((ptr - 1)->next_id);
        }

        // find first partition not owned by id
        while (ptr < end_of_ring && ptr->owner == id)
        {
            ++ptr;
        }

        if (ptr < end_of_ring && ptr->next_owner == id)
        {
            parts->push_back(ptr->next_id);
        }

        return;
    }

    for (unsigned p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
    {
        if (r->partitions[p].next_owner == id)
        {
            ptr = &r->partitions[p];
            break;
        }
    }

    if (!ptr)
    {
        return;
    }

    partition* boundary = NULL;

    while (ptr < end_of_ring && ptr->next_owner == id)
    {
        if (ptr == r->partitions ||
            ptr + 1 == end_of_ring ||
            (ptr - 1)->owner != ptr->owner ||
            ptr->owner != (ptr + 1)->owner)
        {
            boundary = ptr;
            break;
        }

        ++ptr;
    }

    if (boundary)
    {
        parts->push_back(boundary->next_id);
    }
}

e::unpacker
consus :: operator >> (e::unpacker up, configuration& c)
{
    up = kvs_configuration(up, &c.m_cluster, &c.m_version, &c.m_flags, &c.m_kvss, &c.m_rings);

    if (up.error())
    {
        return up;
    }

    c.reconstruct_cache();
    return up;
}
