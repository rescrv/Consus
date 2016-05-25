// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#include "common/kvs_configuration.h"
#include "kvs/configuration.h"

using consus::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_kvss()
    , m_rings()
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
configuration :: hash(data_center_id dc, unsigned index,
                      comm_id replicas[CONSUS_MAX_REPLICATION_FACTOR],
                      unsigned* num_replicas)
{
    ring* r = NULL;

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        if (m_rings[i].dc == dc)
        {
            r = &m_rings[i];
            break;
        }
    }

    if (!r)
    {
        return false;
    }

    *num_replicas = 0;
    replicas[0] = comm_id();

    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS && *num_replicas < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        partition* p = &r->partitions[(index + i) % CONSUS_KVS_PARTITIONS];

        if (p->owner != comm_id() && p->owner != replicas[0] &&
            (*num_replicas == 0 || replicas[*num_replicas - 1] != p->owner))
        {
            replicas[*num_replicas] = p->owner;
            ++*num_replicas;
        }
    }

    return true;
}

void
configuration :: map(data_center_id dc, unsigned index,
                     comm_id* owner, comm_id* next_owner)
{
    if (index >= CONSUS_KVS_PARTITIONS)
    {
        *owner = comm_id();
        *next_owner = comm_id();
    }

    ring* r = NULL;

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        if (m_rings[i].dc == dc)
        {
            r = &m_rings[i];
            break;
        }
    }

    if (!r)
    {
        return;
    }

    *owner = r->partitions[index].owner;
    *next_owner = r->partitions[index].next_owner;
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

e::unpacker
consus :: operator >> (e::unpacker up, configuration& c)
{
    return kvs_configuration(up, &c.m_cluster, &c.m_version, &c.m_flags, &c.m_kvss, &c.m_rings);
}
