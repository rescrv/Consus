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

// STL
#include <set>

// consus
#include "common/txman_configuration.h"
#include "txman/configuration.h"

using consus::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_dcs()
    , m_txmans()
    , m_paxos_groups()
    , m_kvss()
{
}

configuration :: ~configuration() throw ()
{
}

bool
configuration :: exists(comm_id id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].tx.id == id)
        {
            return true;
        }
    }

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].id == id)
        {
            return true;
        }
    }

    return false;
}

consus::data_center_id
configuration :: get_data_center(comm_id id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].tx.id == id)
        {
            return m_txmans[i].tx.dc;
        }
    }

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].id == id)
        {
            return m_kvss[i].dc;
        }
    }

    return data_center_id();
}

po6::net::location
configuration :: get_address(comm_id id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].tx.id == id)
        {
            return m_txmans[i].tx.bind_to;
        }
    }

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].id == id)
        {
            return m_kvss[i].bind_to;
        }
    }

    return po6::net::location();
}

consus::txman_state::state_t
configuration :: get_state(comm_id id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].tx.id == id)
        {
            return m_txmans[i].state;
        }
    }

    return txman_state::state_t();
}

std::vector<consus::paxos_group_id>
configuration :: groups_for(comm_id id) const
{
    std::vector<paxos_group_id> gs;

    for (size_t i = 0; i < m_paxos_groups.size(); ++i)
    {
        for (size_t p = 0; p < m_paxos_groups[i].members_sz; ++p)
        {
            if (m_paxos_groups[i].members[p] == id)
            {
                gs.push_back(m_paxos_groups[i].id);
                break;
            }
        }
    }

    return gs;
}

const consus::paxos_group*
configuration :: get_group(paxos_group_id id) const
{
    for (size_t i = 0; i < m_paxos_groups.size(); ++i)
    {
        if (m_paxos_groups[i].id == id)
        {
            return &m_paxos_groups[i];
        }
    }

    return NULL;
}

bool
configuration :: is_member(paxos_group_id gid, comm_id id) const
{
    const paxos_group* g = get_group(gid);

    if (!g)
    {
        return false;
    }

    for (unsigned i = 0; i < g->members_sz; ++i)
    {
        if (id == g->members[i])
        {
            return true;
        }
    }

    return false;
}

bool
configuration :: choose_groups(paxos_group_id g, std::vector<paxos_group_id>* groups) const
{
    groups->push_back(g);
    std::set<data_center_id> dcs;
    const paxos_group* gptr = get_group(g);
    assert(gptr);
    dcs.insert(gptr->dc);

    for (size_t i = 0; i < m_paxos_groups.size(); ++i)
    {
        if (dcs.find(m_paxos_groups[i].dc) == dcs.end())
        {
            groups->push_back(m_paxos_groups[i].id);
            dcs.insert(m_paxos_groups[i].dc);
        }
    }

    return groups->size() >= (m_dcs.size() / 2 + 1);
}

consus::comm_id
configuration :: choose_kvs(data_center_id dc) const
{
    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].dc == dc)
        {
            return m_kvss[i].id;
        }
    }

    return comm_id();
}

std::string
configuration :: dump() const
{
    return txman_configuration(m_cluster, m_version, m_flags, m_dcs, m_txmans, m_paxos_groups, m_kvss);
}

e::unpacker
consus :: operator >> (e::unpacker up, configuration& c)
{
    return txman_configuration(up, &c.m_cluster, &c.m_version, &c.m_flags, &c.m_dcs, &c.m_txmans, &c.m_paxos_groups, &c.m_kvss);
}
