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
#include "txman/paxos_synod.h"

using consus::paxos_synod;

paxos_synod :: paxos_synod()
    : m_init(false)
    , m_us()
    , m_group()
    , m_acceptor_ballot()
    , m_acceptor_pvalue()
    , m_leader_phase()
    , m_leader_ballot()
    , m_leader_pvalue()
    , m_value()
{
}

paxos_synod :: ~paxos_synod() throw ()
{
}

void
paxos_synod :: init(comm_id us, const paxos_group& pg)
{
    assert(!m_init);
    m_init = true;
    m_us = us;
    m_group = pg;
}

paxos_synod::phase_t
paxos_synod :: phase()
{
    assert(m_init);
    return m_leader_phase;
}

void
paxos_synod :: phase1(ballot* b)
{
    assert(m_init);
    assert(m_leader_phase == PHASE1);
    uint64_t number = m_leader_ballot.number;

    for (unsigned i = 0; i < m_group.members_sz; ++i)
    {
        number = std::max(number, m_promises[i].current_ballot.number);
    }

    if (m_leader_ballot.leader != m_us)
    {
        ++number;
    }

    *b = m_leader_ballot = ballot(number, m_us);
}

void
paxos_synod :: phase1a(const ballot& b, ballot* a, pvalue* p)
{
    assert(m_init);

    if (b > m_acceptor_ballot)
    {
        m_acceptor_ballot = b;
    }

    *a = m_acceptor_ballot;
    *p = m_acceptor_pvalue;
    set_phase();
}

void
paxos_synod :: phase1b(comm_id m, const ballot& a, const pvalue& p)
{
    assert(m_init);
    unsigned idx = m_group.index(m);

    if (idx < m_group.members_sz &&
        m_leader_phase >= PHASE1 &&
        m_leader_ballot == a)
    {
        m_promises[idx].current_ballot = a;
        m_promises[idx].current_pvalue = p;
    }

    set_phase();
}

void
paxos_synod :: phase2(pvalue* p, uint64_t preferred_value)
{
    assert(m_init);
    assert(m_leader_phase == PHASE2);
    *p = pvalue();

    for (unsigned i = 0; i < m_group.members_sz; ++i)
    {
        *p = std::max(*p, m_promises[i].current_pvalue);
    }

    if (*p == pvalue())
    {
        *p = pvalue(m_leader_ballot, preferred_value);
    }
    else
    {
        *p = pvalue(m_leader_ballot, p->v);
    }

    m_leader_pvalue = *p;
    set_phase();
}

void
paxos_synod :: phase2a(const pvalue& p, bool* a)
{
    assert(m_init);

    if (p.b == m_acceptor_ballot)
    {
        m_acceptor_pvalue = p;
        *a = true;
    }
    else
    {
        *a = false;
    }

    set_phase();
}

void
paxos_synod :: phase2b(comm_id m, const pvalue& p)
{
    assert(m_init);
    unsigned idx = m_group.index(m);

    if (idx < m_group.members_sz &&
        m_leader_phase == PHASE2 &&
        m_leader_ballot == p.b)
    {
        m_promises[idx].current_pvalue = p;
    }

    set_phase();
}

void
paxos_synod :: force_learn(uint64_t value)
{
    assert(m_init);
    m_leader_phase = LEARNED;
    m_value = value;
}

uint64_t
paxos_synod :: learned()
{
    assert(m_init);
    assert(m_leader_phase == LEARNED);
    return m_value;
}

void
paxos_synod :: set_phase()
{
    for (size_t i = 0; i < m_group.members_sz; ++i)
    {
        if (m_promises[i].current_ballot > m_leader_ballot)
        {
            m_leader_phase = PHASE1;
            return;
        }
    }

    if (m_leader_phase == PHASE1)
    {
        unsigned accepted = 0;

        for (size_t i = 0; i < m_group.members_sz; ++i)
        {
            if (m_leader_ballot != ballot() &&
                m_promises[i].current_ballot == m_leader_ballot)
            {
                ++accepted;
            }
        }

        if (accepted >= m_group.quorum())
        {
            m_leader_phase = PHASE2;
        }
    }

    if (m_leader_phase == PHASE2)
    {
        unsigned accepted = 0;

        for (size_t i = 0; i < m_group.members_sz; ++i)
        {
            if (m_promises[i].current_ballot == m_leader_ballot &&
                m_promises[i].current_pvalue == m_leader_pvalue &&
                m_leader_pvalue.b == m_leader_ballot)
            {
                ++accepted;
            }
        }

        if (accepted >= m_group.quorum())
        {
            m_leader_phase = LEARNED;
            m_value = m_leader_pvalue.v;
        }
    }
}

paxos_synod :: ballot :: ballot()
    : number(0)
    , leader()
{
}

paxos_synod :: ballot :: ballot(uint64_t n, comm_id l)
    : number(n)
    , leader(l)
{
}

paxos_synod :: ballot :: ballot(const ballot& other)
    : number(other.number)
    , leader(other.leader)
{
}

paxos_synod :: ballot :: ~ballot() throw ()
{
}

int
paxos_synod :: ballot :: compare(const ballot& rhs) const
{
    if (number < rhs.number)
    {
        return -1;
    }
    if (number > rhs.number)
    {
        return 1;
    }

    if (leader < rhs.leader)
    {
        return -1;
    }
    if (leader > rhs.leader)
    {
        return 1;
    }

    return 0;
}

paxos_synod::ballot&
paxos_synod :: ballot :: operator = (const ballot& rhs)
{
    if (this != &rhs)
    {
        number = rhs.number;
        leader = rhs.leader;
    }

    return *this;
}

bool paxos_synod :: ballot :: operator < (const ballot& rhs) const { return compare(rhs) < 0; }
bool paxos_synod :: ballot :: operator <= (const ballot& rhs) const { return compare(rhs) <= 0; }
bool paxos_synod :: ballot :: operator == (const ballot& rhs) const { return compare(rhs) == 0; }
bool paxos_synod :: ballot :: operator != (const ballot& rhs) const { return compare(rhs) != 0; }
bool paxos_synod :: ballot :: operator >= (const ballot& rhs) const { return compare(rhs) >= 0; }
bool paxos_synod :: ballot :: operator > (const ballot& rhs) const { return compare(rhs) > 0; }

paxos_synod :: pvalue :: pvalue()
    : b()
    , v()
{
}

paxos_synod :: pvalue :: pvalue(const ballot& _b, uint64_t _v)
    : b(_b)
    , v(_v)
{
}

paxos_synod :: pvalue :: pvalue(const pvalue& other)
    : b(other.b)
    , v(other.v)
{
}

paxos_synod :: pvalue :: ~pvalue() throw ()
{
}

int
paxos_synod :: pvalue :: compare(const pvalue& rhs) const
{
    return b.compare(rhs.b);
}

paxos_synod::pvalue&
paxos_synod :: pvalue :: operator = (const pvalue& rhs)
{
    if (this != &rhs)
    {
        b = rhs.b;
        v = rhs.v;
    }

    return *this;
}

bool paxos_synod :: pvalue :: operator < (const pvalue& rhs) const { return compare(rhs) < 0; }
bool paxos_synod :: pvalue :: operator <= (const pvalue& rhs) const { return compare(rhs) <= 0; }
bool paxos_synod :: pvalue :: operator == (const pvalue& rhs) const { return compare(rhs) == 0; }
bool paxos_synod :: pvalue :: operator != (const pvalue& rhs) const { return compare(rhs) != 0; }
bool paxos_synod :: pvalue :: operator >= (const pvalue& rhs) const { return compare(rhs) >= 0; }
bool paxos_synod :: pvalue :: operator > (const pvalue& rhs) const { return compare(rhs) > 0; }

paxos_synod :: promise :: promise()
    : current_ballot()
    , current_pvalue()
{
}

paxos_synod :: promise :: ~promise() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& out, const paxos_synod::ballot& b)
{
    return out << "ballot(number=" << b.number
               << ", leader=" << b.leader.get() << ")";
}

std::ostream&
consus :: operator << (std::ostream& out, const paxos_synod::pvalue& p)
{
    return out << "pvalue(number=" << p.b.number
               << ", leader=" << p.b.leader.get()
               << ", value=" << p.v << ")";
}

e::packer
consus :: operator << (e::packer pa, const paxos_synod::ballot& rhs)
{
    return pa << rhs.number << rhs.leader;
}

e::unpacker
consus :: operator >> (e::unpacker up, paxos_synod::ballot& rhs)
{
    return up >> rhs.number >> rhs.leader;
}

size_t
consus :: pack_size(const paxos_synod::ballot& b)
{
    return sizeof(uint64_t) + pack_size(b.leader);
}

e::packer
consus :: operator << (e::packer pa, const paxos_synod::pvalue& rhs)
{
    return pa << rhs.b << rhs.v;
}

e::unpacker
consus :: operator >> (e::unpacker up, paxos_synod::pvalue& rhs)
{
    return up >> rhs.b >> rhs.v;
}

size_t
consus :: pack_size(const paxos_synod::pvalue& p)
{
    return sizeof(uint64_t) + pack_size(p.b);
}
