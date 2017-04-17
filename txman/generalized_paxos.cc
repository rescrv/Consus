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

#define __STDC_LIMIT_MACROS

// STL
#include <algorithm>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>

// e
#include <e/serialization.h>
#include <e/strescape.h>

// consus
#include "txman/generalized_paxos.h"

#ifdef GENERALIZED_PAXOS_THROW
#define GP_ASSERT(X) do { if (!(X)) throw std::runtime_error("bad shit happened"); } while (false)
#define GP_ABORT() do { abort(); } while (false)
#elif GENERALIZED_PAXOS_DEBUG
#define GP_ASSERT(X) do { assert((X)); } while (false)
#define GP_ABORT() do { abort(); } while (false)
#else
#define GP_ASSERT(X) do { } while (false)
#define GP_ABORT() do { } while (false)
#endif

using consus::generalized_paxos;

generalized_paxos :: internal_cstruct :: internal_cstruct()
    : ids()
    , transitive_closure()
    , transitive_closure_N(0)
{
}

generalized_paxos :: internal_cstruct :: ~internal_cstruct() throw ()
{
}

bool
generalized_paxos :: internal_cstruct :: has_command(uint64_t c) const
{
    return are_adjacent(c, c);
}

bool
generalized_paxos :: internal_cstruct :: are_adjacent(uint64_t u, uint64_t v) const
{
    return u < transitive_closure_N &&
           v < transitive_closure_N &&
           (transitive_closure[byte(u, v)] & (1ULL << bit(u, v)));
}

void
generalized_paxos :: internal_cstruct :: set_adjacent(uint64_t u, uint64_t v)
{
    assert(u < transitive_closure_N);
    assert(v < transitive_closure_N);
    transitive_closure[byte(u, v)] |= 1ULL << bit(u, v);
}

void
generalized_paxos :: internal_cstruct :: set_N(uint64_t N)
{
    transitive_closure_N = N;
    const uint64_t bits = N * N;
    const uint64_t quads = (bits + 63) / 64;
    transitive_closure.resize(quads);

    for (size_t i = 0; i < quads; ++i)
    {
        transitive_closure[i] = 0;
    }
}

void
generalized_paxos :: internal_cstruct :: close_transitively()
{
    for (uint64_t i = 0; i < ids.size(); ++i)
    {
        const uint64_t u = ids[i];

        for (uint64_t j = 0; j < ids.size(); ++j)
        {
            const uint64_t v = ids[j];

            for (uint64_t k = 0; k < ids.size(); ++k)
            {
                const uint64_t w = ids[k];

                if (are_adjacent(u, v) && are_adjacent(v, w))
                {
                    set_adjacent(u, w);
                }
            }
        }
    }
}

void
generalized_paxos :: internal_cstruct :: swap(internal_cstruct* other)
{
    std::swap(ids, other->ids);
    std::swap(transitive_closure, other->transitive_closure);
    std::swap(transitive_closure_N, other->transitive_closure_N);
}

uint64_t
generalized_paxos :: internal_cstruct :: byte(uint64_t u, uint64_t v) const
{
    return (u * transitive_closure_N + v) / 64;
}

uint64_t
generalized_paxos :: internal_cstruct :: bit(uint64_t u, uint64_t v) const
{
    return (u * transitive_closure_N + v) % 64;
}

generalized_paxos :: generalized_paxos()
    : m_init(false)
    , m_interfere(NULL)
    , m_state(PARTICIPATING)
    , m_us()
    , m_acceptors()
    , m_proposed()
    , m_commands()
    , m_command_ids()
    , m_acceptor_ballot()
    , m_acceptor_value()
    , m_acceptor_ivalue()
    , m_acceptor_value_src()
    , m_leader_ballot()
    , m_leader_value()
    , m_leader_ivalue()
    , m_promises()
    , m_ipromises()
    , m_accepted()
    , m_iaccepted()
    , m_learned_cached()
{
}

generalized_paxos :: ~generalized_paxos() throw ()
{
}

void
generalized_paxos :: init(const comparator* cmp, abstract_id us, const abstract_id* acceptors, size_t acceptors_sz)
{
    assert(!m_init);
    assert(acceptors_sz <= 63);
    m_init = true;
    m_interfere = cmp;
    m_us = us;
    m_acceptors = std::vector<abstract_id>(acceptors, acceptors + acceptors_sz);
    m_promises.resize(acceptors_sz);
    m_ipromises.resize(acceptors_sz);
    m_accepted.resize(acceptors_sz);
    m_iaccepted.resize(acceptors_sz);
}

void
generalized_paxos :: default_leader(abstract_id leader, ballot::type_t t)
{
    assert(m_init);
    m_acceptor_ballot = ballot(t, 1, leader);
    m_acceptor_value_src = m_acceptor_ballot;

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        m_accepted[i] = message_p2b(m_acceptor_ballot, m_acceptors[i], cstruct());
    }

    if (leader == m_us)
    {
        m_state = LEADING_PHASE2;
        m_leader_ballot = m_acceptor_ballot;

        for (size_t i = 0; i < m_promises.size(); ++i)
        {
            m_promises[i].b = m_acceptor_ballot;
            m_promises[i].acceptor = m_acceptors[i];
        }
    }
    else
    {
        m_state = PARTICIPATING;
    }
}

bool
generalized_paxos :: propose(const command& c)
{
    assert(m_init);

    if (std::find(m_proposed.begin(), m_proposed.end(), c) != m_proposed.end())
    {
        return false;
    }

    m_proposed.push_back(c);
    return true;
}

bool
generalized_paxos :: propose_from_p2b(const message_p2b& m)
{
    assert(m_init);

    bool ret = false;

    for (size_t i = 0; i < m.v.commands.size(); ++i)
    {
        ret = propose(m.v.commands[i]) || ret;
    }

    return ret;
}

void
generalized_paxos :: advance(bool may_attempt_leadership,
                             bool* send_m1, message_p1a* m1,
                             bool* send_m2, message_p2a* m2,
                             bool* send_m3, message_p2b* m3)
{
    assert(m_init);

    *send_m1 = false;
    *send_m2 = false;
    *send_m3 = false;
    internal_cstruct learn;
    bool conflict = false;
    learned(&learn, &conflict);

    if (m_state >= LEADING_PHASE2 &&
        m_leader_ballot.type == ballot::CLASSIC &&
        icstruct_eq(learn, m_leader_ivalue))
    {
        may_attempt_leadership = true;
        m_state = PARTICIPATING;
    }

    if (may_attempt_leadership &&
        (m_leader_ballot.leader != m_us ||
         m_state == PARTICIPATING ||
         (m_leader_ballot.type == ballot::FAST && conflict)))
    {
        ballot::type_t t = conflict ? ballot::CLASSIC : ballot::FAST;
        uint64_t number = std::max(m_acceptor_ballot, m_leader_ballot).number + 1;
        m_leader_ballot = ballot(t, number, m_us);
        m_state = LEADING_PHASE1;
    }

    if (m_state >= LEADING_PHASE1)
    {
        size_t promised = 0;

        for (size_t i = 0; i < m_promises.size(); ++i)
        {
            if (m_promises[i].b == m_leader_ballot)
            {
                ++promised;
            }
        }

        if (promised < m_promises.size())
        {
            *send_m1 = true;
            *m1 = message_p1a(m_leader_ballot);
        }

        if (promised >= quorum() && m_state < LEADING_PHASE2)
        {
            m_state = LEADING_PHASE2;
            proven_safe(&m_leader_ivalue);
            icstruct_to_cstruct(m_leader_ivalue, &m_leader_value);
            GP_ASSERT(icstruct_compatible(m_leader_ivalue, m_learned_cached));

            for (size_t i = 0; i < m_promises.size(); ++i)
            {
                for (size_t c = 0; c < m_promises[i].v.commands.size(); ++c)
                {
                    propose(m_promises[i].v.commands[c]);
                }
            }

            std::sort(m_proposed.begin(), m_proposed.end());
        }
    }

    if (m_state >= LEADING_PHASE2 && m_leader_ballot.type == ballot::CLASSIC)
    {
        bool changed = false;

        for (size_t i = 0; i < m_proposed.size(); ++i)
        {
            if (std::find(m_leader_value.commands.begin(),
                          m_leader_value.commands.end(),
                          m_proposed[i]) == m_leader_value.commands.end())
            {
                m_leader_value.commands.push_back(m_proposed[i]);
                changed = true;
            }
        }

        if (changed)
        {
            cstruct_to_icstruct(m_leader_value, &m_leader_ivalue);
        }

        *send_m2 = true;
        *m2 = message_p2a(m_leader_ballot, m_leader_value);
    }

    if (m_acceptor_ballot.type == ballot::FAST)
    {
        bool changed = false;

        for (size_t i = 0; i < m_proposed.size(); ++i)
        {
            if (std::find(m_acceptor_value.commands.begin(),
                          m_acceptor_value.commands.end(),
                          m_proposed[i]) == m_acceptor_value.commands.end())
            {
                m_acceptor_value_src = m_acceptor_ballot;
                m_acceptor_value.commands.push_back(m_proposed[i]);
                changed = true;
            }
        }

        if (changed)
        {
            cstruct_to_icstruct(m_acceptor_value, &m_acceptor_ivalue);
        }
    }

    if (m_acceptor_value_src > ballot())
    {
        *send_m3 = true;
        *m3 = message_p2b(m_acceptor_value_src, m_us, m_acceptor_value);
    }
}

// this implements Phase1b of the distributed abstract algorithm
void
generalized_paxos :: process_p1a(const message_p1a& m, bool* send, message_p1b* r)
{
    assert(m_init);
    *send = false;
    size_t idx = index_of(m.b.leader);

    if (idx >= m_acceptors.size())
    {
        return;
    }

    if (m.b > m_leader_ballot && m_state > PARTICIPATING)
    {
        if (m.b.leader == m_us)
        {
            m_leader_ballot = m.b;
            m_state = LEADING_PHASE1;
        }
        else
        {
            m_state = PARTICIPATING;
        }
    }

    if (m.b >= m_acceptor_ballot)
    {
        m_acceptor_ballot = m.b;
        *send = true;
        *r = message_p1b(m_acceptor_ballot, m_us, m_acceptor_value_src, m_acceptor_value);
    }
}

bool
generalized_paxos :: process_p1b(const message_p1b& m)
{
    assert(m_init);
    size_t idx = index_of(m.acceptor);

    if (idx >= m_acceptors.size())
    {
        return false;
    }

    if (m.b == m_leader_ballot &&
        m_promises[idx].b <= m_leader_ballot &&
        m_state >= LEADING_PHASE1)
    {
        m_promises[idx] = m;
        cstruct_to_icstruct(m_promises[idx].v, &m_ipromises[idx]);
        return true;
    }

    return false;
}

// this implements Phase2bClassic of the distributed abstract algorithm
void
generalized_paxos :: process_p2a(const message_p2a& m, bool* send, message_p2b* r)
{
    assert(m_init);
    *send = false;

    internal_cstruct imv;
    cstruct_to_icstruct(m.v, &imv);

    if (m.b.type == ballot::CLASSIC &&
        m.b == m_acceptor_ballot &&
        (m_acceptor_value_src != m_acceptor_ballot ||
         icstruct_le(m_acceptor_ivalue, imv)))
    {
        m_acceptor_value_src = m_acceptor_ballot;
        m_acceptor_value = m.v;
        m_acceptor_ivalue = imv;
        *send = true;
        *r = message_p2b(m_acceptor_value_src, m_us, m_acceptor_value);
    }
}

bool
generalized_paxos :: process_p2b(const message_p2b& m)
{
    assert(m_init);

    size_t idx = index_of(m.acceptor);

    if (idx >= m_acceptors.size())
    {
        return false;
    }

    internal_cstruct imv;
    cstruct_to_icstruct(m.v, &imv);

    if (m_accepted[idx].b < m.b ||
        (m_accepted[idx].b == m.b &&
         icstruct_lt(m_iaccepted[idx], imv)))
    {
        m_accepted[idx] = m;
        m_iaccepted[idx] = imv;
        return true;
    }

    return false;
}

generalized_paxos::cstruct
generalized_paxos :: learned()
{
    internal_cstruct iret;
    bool conflict;
    learned(&iret, &conflict);
    cstruct ret;
    icstruct_to_cstruct(iret, &ret);
    return ret;
}

void
generalized_paxos :: all_accepted_commands(std::vector<command>* commands)
{
    *commands = m_commands;
    std::sort(commands->begin(), commands->end());
}

std::string
generalized_paxos :: debug_dump(e::compat::function<std::string(cstruct)> pcst,
                                e::compat::function<std::string(command)> pcmd)
{
    if (!m_init)
    {
        return "uninitialized";
    }

    std::ostringstream ostr;

    switch (m_state)
    {
        case PARTICIPATING: ostr << "participating\n"; break;
        case LEADING_PHASE1: ostr << "leading phase1\n"; break;
        case LEADING_PHASE2: ostr << "leading phase2\n"; break;
        default: ostr << "corrupted state\n"; break;
    }

    ostr << m_us.get() << " in [";

    for (size_t i = 0; i < m_acceptors.size(); ++i)
    {
        if (i > 0)
        {
            ostr << ", ";
        }

        ostr << m_acceptors[i].get();
    }

    ostr << "]\n";

    for (size_t i = 0; i < m_proposed.size(); ++i)
    {
        ostr << "proposal[" << i << "] " << pcmd(m_proposed[i]) << "\n";
    }

    ostr << "acceptor " << m_acceptor_ballot << "\n";
    ostr << "acceptor value " << pcst(m_acceptor_value) << "\n";
    ostr << "accepted by " << m_acceptor_value_src << "\n";

    ostr << "leader " << m_leader_ballot << "\n";
    ostr << "leader value " << pcst(m_leader_value) << "\n";

    for (size_t i = 0; i < m_promises.size(); ++i)
    {
        ostr << "leader promise[" << i << "].ballot " << m_promises[i].b << "\n";
        ostr << "leader promise[" << i << "].acceptor " << m_promises[i].acceptor.get() << "\n";
        ostr << "leader promise[" << i << "].vb " << m_promises[i].vb << "\n";
        ostr << "leader promise[" << i << "].v " << pcst(m_promises[i].v) << "\n";
    }

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        ostr << "accepted[" << i << "].b " << m_accepted[i].b << "\n";
        ostr << "accepted[" << i << "].acceptor " << m_accepted[i].acceptor.get() << "\n";
        ostr << "accepted[" << i << "].v " << pcst(m_accepted[i].v) << "\n";
    }

    ostr << "learned " << pcst(learned()) << "\n";
    return ostr.str();
}

size_t
generalized_paxos :: index_of(abstract_id a)
{
    for (size_t i = 0; i < m_acceptors.size(); ++i)
    {
        if (a == m_acceptors[i])
        {
            return i;
        }
    }

    return m_acceptors.size();
}

size_t
generalized_paxos :: quorum()
{
    return 2 * m_acceptors.size() / 3 + 1;
}

void
generalized_paxos :: learned(internal_cstruct* ret, bool* conflict)
{
    *conflict = false;
    typedef std::map<ballot, uint64_t> ballot_map_t;
    ballot_map_t ballots;

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        ++ballots[m_accepted[i].b];
    }

    std::vector<internal_cstruct> learned_values;
    learned_values.reserve(ballots.size() * quorum() * (1U << CONSUS_MAX_REPLICATION_FACTOR));

    for (ballot_map_t::iterator it = ballots.begin(); it != ballots.end(); ++it)
    {
        if (it->second >= quorum())
        {
            learned(it->first, &learned_values, conflict);
        }
    }

#if GENERALIZED_PAXOS_DEBUG
    for (size_t i = 0; i < learned_values.size(); ++i)
    {
        GP_ASSERT(icstruct_compatible(m_learned_cached, learned_values[i]));
        GP_ASSERT(icstruct_compatible(learned_values[i], m_learned_cached));

        for (size_t j = i + 1; j < learned_values.size(); ++j)
        {
            GP_ASSERT(icstruct_compatible(learned_values[i], learned_values[j]));
            GP_ASSERT(icstruct_compatible(learned_values[j], learned_values[i]));
        }
    }
#endif

    std::vector<const internal_cstruct*> lv_ptrs;
    lv_ptrs.reserve(learned_values.size() + 1);

    for (size_t i = 0; i < learned_values.size(); ++i)
    {
        lv_ptrs.push_back(&learned_values[i]);
    }

    lv_ptrs.push_back(&m_learned_cached);
    *ret = internal_cstruct();
    icstruct_lub(&lv_ptrs[0], lv_ptrs.size(), ret);
    GP_ASSERT(icstruct_le(m_learned_cached, *ret));
    m_learned_cached = *ret;
}

void
generalized_paxos :: learned(const ballot& b, std::vector<internal_cstruct>* lv, bool* conflict)
{
    std::vector<const internal_cstruct*> vs;

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        if (m_accepted[i].b == b)
        {
            vs.push_back(&m_iaccepted[i]);
        }
    }

    assert(vs.size() > 0);
    learned(&vs[0], vs.size(), quorum(), lv, conflict);
}

void
generalized_paxos :: learned(const internal_cstruct** vs, size_t vs_sz, size_t max_sz,
                             std::vector<internal_cstruct>* lv, bool* conflict)
{
    assert(vs_sz >  0);

    if (vs_sz == 1)
    {
        lv->push_back(*vs[0]);
    }
    else if (vs_sz <= max_sz)
    {
        lv->push_back(internal_cstruct());
        icstruct_glb(vs, vs_sz, &lv->back(), conflict);
    }
    else
    {
        // enumerate every subset of vs of size max_sz
        uint64_t limit = 1ULL << vs_sz;
        uint64_t v = (1ULL << max_sz) - 1ULL;
        assert(v < limit);

        while (v < limit)
        {
            std::vector<const internal_cstruct*> nvs;

            for (size_t i = 0; i < vs_sz; ++i)
            {
                if ((v & (1ULL << i)))
                {
                    nvs.push_back(vs[i]);
                }
            }

            assert(nvs.size() == max_sz);
            lv->push_back(internal_cstruct());
            icstruct_glb(&nvs[0], nvs.size(), &lv->back(), conflict);

            // lexicographically next bit permutation
            // http://graphics.stanford.edu/~seander/bithacks.html#NextBitPermutation
            uint64_t t = (v | (v - 1)) + 1;
            v = t | ((((t & -t) / (v & -v)) >> 1) - 1);
        }
    }
}

void
generalized_paxos :: proven_safe(internal_cstruct* ret)
{
    ballot k;

    for (size_t i = 0; i < m_promises.size(); ++i)
    {
        if (m_promises[i].b == m_leader_ballot && !m_promises[i].v.is_none())
        {
            k = std::max(k, m_promises[i].vb);
        }
    }

    // enumerate every R \in Quorum(k)
    const uint64_t limit = 1ULL << m_promises.size();
    uint64_t v = (1ULL << quorum()) - 1ULL;
    assert(v < limit);
    std::vector<internal_cstruct> gamma_R;
    gamma_R.reserve(limit);

    while (v < limit)
    {
        std::vector<const internal_cstruct*> vs;

        for (size_t i = 0; i < m_promises.size(); ++i)
        {
            // if in R            && in Q
            if ((v & (1ULL << i)) && m_promises[i].b == m_leader_ballot)
            {
                if (m_promises[i].vb == k && !m_promises[i].v.is_none())
                {
                    vs.push_back(&m_ipromises[i]);
                }
            }
        }

        if (!vs.empty())
        {
            gamma_R.push_back(internal_cstruct());
            bool conflict = false;
            icstruct_glb(&vs[0], vs.size(), &gamma_R.back(), &conflict);
        }

        // lexicographically next bit permutation
        // http://graphics.stanford.edu/~seander/bithacks.html#NextBitPermutation
        uint64_t t = (v | (v - 1)) + 1;
        v = t | ((((t & -t) / (v & -v)) >> 1) - 1);
    }

    if (gamma_R.empty())
    {
        for (size_t i = 0; i < m_promises.size(); ++i)
        {
            if (m_promises[i].b == m_leader_ballot && m_promises[i].vb == k)
            {
                *ret = m_ipromises[i];
                return;
            }
        }
    }

    std::vector<const internal_cstruct*> gr_ptrs;
    gr_ptrs.reserve(gamma_R.size());

    for (size_t i = 0; i < gamma_R.size(); ++i)
    {
        gr_ptrs.push_back(&gamma_R[i]);
    }

    *ret = internal_cstruct();
    icstruct_lub(&gr_ptrs[0], gr_ptrs.size(), ret);
}

const generalized_paxos::command&
generalized_paxos :: id_command(uint64_t c)
{
    for (command_map_t::iterator it = m_command_ids.begin();
            it != m_command_ids.end(); ++it)
    {
        if (it->second == c)
        {
            return it->first;
        }
    }

    abort();
}

uint64_t
generalized_paxos :: command_id(const command& c)
{
    command_map_t::iterator it = m_command_ids.find(c);

    if (it != m_command_ids.end())
    {
        return it->second;
    }

    uint64_t id = m_commands.size();
    m_commands.push_back(c);
    m_command_ids.insert(std::make_pair(c, id));
    return id;
}

void
generalized_paxos :: cstruct_to_icstruct(const cstruct& cs, internal_cstruct* ics)
{
    ics->ids.resize(cs.commands.size());
    uint64_t largest = 0;

    for (size_t i = 0; i < cs.commands.size(); ++i)
    {
        ics->ids[i] = command_id(cs.commands[i]);
        largest = std::max(largest, ics->ids[i]);
    }

    ++largest;
    ics->set_N(largest);

    for (uint64_t i = 0; i < ics->ids.size(); ++i)
    {
        const uint64_t c = ics->ids[i];
        ics->set_adjacent(c, c);
    }

    for (uint64_t i = 0; i < ics->ids.size(); ++i)
    {
        for (uint64_t j = i + 1; j < ics->ids.size(); ++j)
        {
            if (m_interfere->conflict(cs.commands[i], cs.commands[j]))
            {
                ics->set_adjacent(ics->ids[i], ics->ids[j]);
            }
        }
    }

    ics->close_transitively();
}

void
generalized_paxos :: icstruct_to_cstruct(const internal_cstruct& ics, cstruct* cs)
{
    cs->commands.resize(ics.ids.size());

    for (size_t i = 0; i < ics.ids.size(); ++i)
    {
        cs->commands[i] = id_command(ics.ids[i]);
    }
}

bool
generalized_paxos :: icstruct_lt(const internal_cstruct& lhs,
                                 const internal_cstruct& rhs)
{
    return lhs.ids.size() < rhs.ids.size() && icstruct_le(lhs, rhs);
}

bool
generalized_paxos :: icstruct_le(const internal_cstruct& lhs,
                                 const internal_cstruct& rhs)
{
    // lhs is a prefix of rhs if all elements in lhs are in rhs and the ordering
    // of the common elements is compatible

    for (size_t i = 0; i < lhs.ids.size(); ++i)
    {
        const uint64_t c = lhs.ids[i];

        if (!rhs.has_command(c))
        {
            return false;
        }
    }

    return icstruct_compatible(lhs, rhs);
}

bool
generalized_paxos :: icstruct_eq(const internal_cstruct& lhs,
                                 const internal_cstruct& rhs)
{
    return lhs.ids.size() == rhs.ids.size() && icstruct_le(lhs, rhs);
}

bool
generalized_paxos :: icstruct_compatible(const internal_cstruct& lhs,
                                         const internal_cstruct& rhs)
{
    // From the GP Tech Report:
    // [σ] and [τ] are compatible iff the subgraphs of G(σ) and G(τ) consisting
    // of the nodes they have in common are identical, and C does not conflict
    // with D for every node C in G(σ) that is not in G(τ) and every node D in
    // G(τ) that is not in G(σ).
    //
    // We will say σ=lhs and τ=rhs
    const uint64_t N = std::max(lhs.transitive_closure_N,
                                rhs.transitive_closure_N);
    std::vector<uint64_t> Cs;
    std::vector<uint64_t> Ds;
    Cs.reserve(lhs.ids.size());
    Ds.reserve(rhs.ids.size());

    for (size_t i = 0; i < lhs.ids.size(); ++i)
    {
        const uint64_t c = lhs.ids[i];
        GP_ASSERT(lhs.has_command(c));

        // If the command is in rhs, both posets must have the same transitive
        // closure of commands that come before.
        if (rhs.has_command(c))
        {
            for (size_t n = 0; n < N; ++n)
            {
                if (lhs.are_adjacent(n, c) != rhs.are_adjacent(n, c))
                {
                    return false;
                }
            }
        }
        else
        {
            Cs.push_back(c);
        }
    }

    for (size_t i = 0; i < rhs.ids.size(); ++i)
    {
        const uint64_t d = rhs.ids[i];
        GP_ASSERT(rhs.has_command(d));

        // If both have the command, its transitive closure was checked above,
        // so only take the second half of the conditional from the above loop.
        if (!lhs.has_command(d))
        {
            Ds.push_back(d);
        }
    }

    for (size_t i = 0; i < Cs.size(); ++i)
    {
        for (size_t j = 0; j < Ds.size(); ++j)
        {
            if (m_interfere->conflict(id_command(Cs[i]), id_command(Ds[j])))
            {
                return false;
            }
        }
    }

    return true;
}

void
generalized_paxos :: icstruct_glb(const internal_cstruct** ics, size_t ics_sz,
                                  internal_cstruct* ret, bool* conflict)
{
    uint64_t min_N = UINT64_MAX;
    uint64_t max_N = 0;

    for (size_t i = 0; i < ics_sz; ++i)
    {
        min_N = std::min(min_N, ics[i]->transitive_closure_N);
        max_N = std::max(max_N, ics[i]->transitive_closure_N);
    }

    *ret = internal_cstruct();

    if (ics_sz == 0)
    {
        return;
    }

    ret->set_N(min_N);

    std::vector<size_t> adjmat;
    adjmat.resize(max_N * max_N);

    for (size_t i = 0; i < adjmat.size(); ++i)
    {
        adjmat[i] = 0;
    }

    // Compute an adjacency matrix for a directed, weighted graph.  Vertices in
    // the graph are the internal, consecutively assigned ids for commands.  An
    // edge u, v exists in the graph iff a transitive relation between u, v
    // exists in any of the internal cstructs.  The weight of the edge indicates
    // the number of cstructs that have the edge.
    //
    // Ignoring the edge weights, this graph is equivalent to its transitive
    // closure because the internal cstructs' transitive closures are
    // pre-computed.  The weights don't represent anything about the transitive
    // closure or path lengths.
    //
    // The first max_N entries of adjmat represent the commands ordered before
    // command 0.  In general entries [max_N * i, max_N * (i +1)) represent the
    // all commands ordered before command i.
    for (size_t i = 0; i < ics_sz; ++i)
    {
        for (size_t j = 0; j < max_N; ++j)
        {
            for (size_t k = 0; k < max_N; ++k)
            {
                if (ics[i]->are_adjacent(j, k))
                {
                    ++adjmat[k * max_N + j];
                }
            }
        }
    }

    std::vector<bool> conflicts;
    conflicts.resize(max_N, false);

    // Figure out which commands belong to the glb and detect conflicts.
    //
    // A conflict exists if there is a cycle.  Because adjmat is transitively
    // closed, we can simply check for each edge u,v that exists that edge v,u
    // does not exist.
    //
    // A command/vertex belongs to the glb iff every command preceding it has
    // the same weight and that weight equals the total number of cstructs and
    // the command is not part of a conflict.
    //
    // Do a first pass to mark conflicted values.
    // Do a second pass to deletect values belonging to glb.

    for (size_t i = 0; i < max_N; ++i)
    {
        for (size_t j = 0; j < max_N; ++j)
        {
            if (i == j)
            {
                continue;
            }

            uint64_t forward_count = adjmat[i * max_N + j];
            uint64_t reverse_count = adjmat[j * max_N + i];

            if (forward_count > 0 && reverse_count > 0)
            {
                *conflict = true;
                conflicts[i] = true;
                conflicts[j] = true;
            }
        }
    }

    for (size_t i = 0; i < max_N; ++i)
    {
        // Assume it belongs; prove otherwise
        bool belongs_to_glb = !conflicts[i];

        for (size_t j = 0; j < max_N; ++j)
        {
            uint64_t forward_count = adjmat[i * max_N + j];

            if (i == j && forward_count != ics_sz)
            {
                belongs_to_glb = false;
            }

            if (forward_count > 0 &&
                (forward_count < ics_sz || conflicts[j]))
            {
                belongs_to_glb = false;
            }
        }

        assert(!belongs_to_glb || i < min_N);

        if (belongs_to_glb)
        {
            ret->set_adjacent(i, i);
        }
    }

    for (size_t i = 0; i < ics[0]->ids.size(); ++i)
    {
        if (ret->has_command(ics[0]->ids[i]))
        {
            ret->ids.push_back(ics[0]->ids[i]);
        }
    }

    for (uint64_t i = 0; i < ret->ids.size(); ++i)
    {
        const command& x(id_command(ret->ids[i]));

        for (uint64_t j = i + 1; j < ret->ids.size(); ++j)
        {
            const command& y(id_command(ret->ids[j]));

            if (m_interfere->conflict(x, y))
            {
                ret->set_adjacent(ret->ids[i], ret->ids[j]);
            }
        }
    }

    ret->close_transitively();
}

void
generalized_paxos :: icstruct_lub(const internal_cstruct** ics, size_t ics_sz, internal_cstruct* ret)
{
#if GENERALIZED_PAXOS_DEBUG
    for (size_t i = 0; i < ics_sz; ++i)
    {
        for (size_t j = i + 1; j < ics_sz; ++j)
        {
            GP_ASSERT(icstruct_compatible(*ics[i], *ics[j]));
            GP_ASSERT(icstruct_compatible(*ics[j], *ics[i]));
        }
    }
#endif

    uint64_t N = 0;

    for (size_t i = 0; i < ics_sz; ++i)
    {
        N = std::max(N, ics[i]->transitive_closure_N);
    }

    *ret = internal_cstruct();
    ret->set_N(N);

    for (size_t i = 0; i < ics_sz; ++i)
    {
        for (size_t j = 0; j < ics[i]->ids.size(); ++j)
        {
            uint64_t c = ics[i]->ids[j];

            if (!ret->has_command(c))
            {
                ret->ids.push_back(c);
                ret->set_adjacent(c, c);
                GP_ASSERT(ret->has_command(c));
            }
        }
    }

    for (uint64_t i = 0; i < ret->ids.size(); ++i)
    {
        const command& x(id_command(ret->ids[i]));

        for (uint64_t j = i + 1; j < ret->ids.size(); ++j)
        {
            const command& y(id_command(ret->ids[j]));

            if (m_interfere->conflict(x, y))
            {
                ret->set_adjacent(ret->ids[i], ret->ids[j]);
            }
        }
    }

    ret->close_transitively();
}

generalized_paxos :: command :: command()
    : type(0)
    , value()
{
}

generalized_paxos :: command :: command(uint16_t _type, const std::string& _value)
    : type(_type)
    , value(_value)
{
}

generalized_paxos :: command :: command(const command& other)
    : type(other.type)
    , value(other.value)
{
}

generalized_paxos :: command :: ~command() throw ()
{
}

int
generalized_paxos :: command :: compare(const command& rhs) const
{
    if (type < rhs.type)
    {
        return -1;
    }
    if (type > rhs.type)
    {
        return 1;
    }

    if (value < rhs.value)
    {
        return -1;
    }
    if (value > rhs.value)
    {
        return 1;
    }

    return 0;
}

generalized_paxos::command&
generalized_paxos :: command :: operator = (const command& rhs)
{
    type = rhs.type;
    value = rhs.value;
    return *this;
}

bool generalized_paxos :: command :: operator < (const command& rhs) const { return compare(rhs) < 0; }
bool generalized_paxos :: command :: operator <= (const command& rhs) const { return compare(rhs) <= 0; }
bool generalized_paxos :: command :: operator == (const command& rhs) const { return compare(rhs) == 0; }
bool generalized_paxos :: command :: operator != (const command& rhs) const { return compare(rhs) != 0; }
bool generalized_paxos :: command :: operator >= (const command& rhs) const { return compare(rhs) >= 0; }
bool generalized_paxos :: command :: operator > (const command& rhs) const { return compare(rhs) > 0; }

generalized_paxos :: comparator :: comparator()
{
}

generalized_paxos :: comparator :: ~comparator() throw ()
{
}

generalized_paxos :: cstruct :: cstruct()
    : commands()
{
}

generalized_paxos :: cstruct :: ~cstruct() throw ()
{
}

bool
generalized_paxos :: cstruct :: operator == (const cstruct& rhs) const
{
    if (commands.size() != rhs.commands.size())
    {
        return false;
    }

    for (size_t i = 0; i < commands.size(); ++i)
    {
        if (commands[i] != rhs.commands[i])
        {
            return false;
        }
    }

    return true;
}

generalized_paxos :: ballot :: ballot()
    : type(CLASSIC)
    , number()
    , leader()
{
}

generalized_paxos :: ballot :: ballot(type_t _type, uint64_t _number, abstract_id _leader)
    : type(_type)
    , number(_number)
    , leader(_leader)
{
}

generalized_paxos :: ballot :: ballot(const ballot& other)
    : type(other.type)
    , number(other.number)
    , leader(other.leader)
{
}

generalized_paxos :: ballot :: ~ballot() throw ()
{
}

int
generalized_paxos :: ballot :: compare(const ballot& rhs) const
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

    if (type == rhs.type)
    {
        return 0;
    }
    else if (type == FAST)
    {
        return -1;
    }
    else
    {
        return 1;
    }
}

generalized_paxos::ballot&
generalized_paxos :: ballot :: operator = (const ballot& rhs)
{
    if (this != &rhs)
    {
        type = rhs.type;
        number = rhs.number;
        leader = rhs.leader;
    }

    return *this;
}

bool generalized_paxos :: ballot :: operator < (const ballot& rhs) const { return compare(rhs) < 0; }
bool generalized_paxos :: ballot :: operator <= (const ballot& rhs) const { return compare(rhs) <= 0; }
bool generalized_paxos :: ballot :: operator == (const ballot& rhs) const { return compare(rhs) == 0; }
bool generalized_paxos :: ballot :: operator != (const ballot& rhs) const { return compare(rhs) != 0; }
bool generalized_paxos :: ballot :: operator >= (const ballot& rhs) const { return compare(rhs) >= 0; }
bool generalized_paxos :: ballot :: operator > (const ballot& rhs) const { return compare(rhs) > 0; }

generalized_paxos :: message_p1a :: message_p1a()
    : b()
{
}

generalized_paxos :: message_p1a :: message_p1a(const ballot& _b)
    : b(_b)
{
}

generalized_paxos :: message_p1a :: ~message_p1a() throw ()
{
}

bool
generalized_paxos :: message_p1a :: operator == (const message_p1a& rhs) const
{
    return this->b == rhs.b;
}

generalized_paxos :: message_p1b :: message_p1b()
    : b()
    , acceptor()
    , vb()
    , v()
{
}

generalized_paxos :: message_p1b :: message_p1b(const ballot& _b, abstract_id _acceptor, const ballot& _vb, const cstruct& _v)
    : b(_b)
    , acceptor(_acceptor)
    , vb(_vb)
    , v(_v)
{
}

generalized_paxos :: message_p1b :: ~message_p1b() throw ()
{
}

bool
generalized_paxos :: message_p1b :: operator == (const message_p1b& rhs) const
{
    return this->b == rhs.b && this->acceptor == rhs.acceptor && this->vb == rhs.vb && this->v == rhs.v;
}

generalized_paxos :: message_p2a :: message_p2a()
    : b()
    , v()
{
}

generalized_paxos :: message_p2a :: message_p2a(const ballot& _b, const cstruct& _v)
    : b(_b)
    , v(_v)
{
}

generalized_paxos :: message_p2a :: ~message_p2a() throw ()
{
}

bool
generalized_paxos :: message_p2a :: operator == (const message_p2a& rhs) const
{
    return this->b == rhs.b && this->v == rhs.v;
}

generalized_paxos :: message_p2b :: message_p2b()
    : b()
    , acceptor()
    , v()
{
}

generalized_paxos :: message_p2b :: message_p2b(const ballot& _b, abstract_id _acceptor, const cstruct& _v)
    : b(_b)
    , acceptor(_acceptor)
    , v(_v)
{
}

generalized_paxos :: message_p2b :: ~message_p2b() throw ()
{
}

bool
generalized_paxos :: message_p2b :: operator == (const message_p2b& rhs) const
{
    return this->b == rhs.b && this->acceptor == rhs.acceptor && this->v == rhs.v;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::command& rhs)
{
    return lhs << "command(type=" << rhs.type << ", value=\"" << e::strescape(rhs.value) << "\")";
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::cstruct& rhs)
{
    lhs << "cstruct([";

    for (size_t i = 0; i < rhs.commands.size(); ++i)
    {
        if (i > 0)
        {
            lhs << ", ";
        }

        lhs << rhs.commands[i];
    }

    lhs << "])";
    return lhs;
}

std::ostream&
consus :: operator << (std::ostream& out, const generalized_paxos::ballot& b)
{
    return out << "ballot(type=" << b.type
               << ", number=" << b.number
               << ", leader=" << b.leader.get() << ")";
}

std::ostream&
consus :: operator << (std::ostream& out, const generalized_paxos::ballot::type_t& t)
{
    return out << (t == generalized_paxos::ballot::FAST ? "FAST" : "CLASSIC");
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::message_p1a& m1a)
{
    return lhs << "message_p1a(b=" << m1a.b << ")";
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::message_p1b& m1b)
{
    return lhs << "message_p1b(b=" << m1b.b
               << ", acceptor=" << m1b.acceptor
               << ", vb=" << m1b.vb
               << ", v=" << m1b.v;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::message_p2a& m2a)
{
    return lhs << "message_p2a(b=" << m2a.b
               << ", v=" << m2a.v;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::message_p2b& m2b)
{
    return lhs << "message_p2b(b=" << m2b.b
               << ", acceptor=" << m2b.acceptor
               << ", v=" << m2b.v;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const generalized_paxos::internal_cstruct& rhs)
{
    lhs << "[";

    for (size_t i = 0; i < rhs.ids.size(); ++i)
    {
        if (i > 0)
        {
            lhs << ", ";
        }

        lhs << rhs.ids[i];
    }

    lhs << "]";
    return lhs;
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::command& rhs)
{
    return pa << rhs.type << e::slice(rhs.value);
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::command& rhs)
{
    e::slice v;
    up = up >> rhs.type >> v;
    rhs.value.assign(v.cdata(), v.size());
    return up;
}

size_t
consus :: pack_size(const generalized_paxos::command& c)
{
    return sizeof(uint16_t) + pack_size(e::slice(c.value));
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::cstruct& rhs)
{
    return pa << rhs.commands;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::cstruct& rhs)
{
    return up >> rhs.commands;
}

size_t
consus :: pack_size(const generalized_paxos::cstruct& cs)
{
    return ::pack_size(cs.commands);
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::ballot& rhs)
{
    return pa << e::pack_uint8<generalized_paxos::ballot::type_t>(rhs.type) << rhs.number << rhs.leader;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::ballot& rhs)
{
    return up >> e::unpack_uint8<generalized_paxos::ballot::type_t>(rhs.type) >> rhs.number >> rhs.leader;
}

size_t
consus :: pack_size(const generalized_paxos::ballot& b)
{
    return sizeof(uint8_t) + sizeof(uint64_t) + pack_size(b.leader);
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::message_p1a& rhs)
{
    return pa << rhs.b;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::message_p1a& rhs)
{
    return up >> rhs.b;
}

size_t
consus :: pack_size(const generalized_paxos::message_p1a& m)
{
    return pack_size(m.b);
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::message_p1b& rhs)
{
    return pa << rhs.b << rhs.acceptor << rhs.vb << rhs.v;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::message_p1b& rhs)
{
    return up >> rhs.b >> rhs.acceptor >> rhs.vb >> rhs.v;
}

size_t
consus :: pack_size(const generalized_paxos::message_p1b& m)
{
    return pack_size(m.b) + pack_size(m.acceptor) + pack_size(m.vb) + pack_size(m.v);
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::message_p2a& rhs)
{
    return pa << rhs.b << rhs.v;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::message_p2a& rhs)
{
    return up >> rhs.b >> rhs.v;
}

size_t
consus :: pack_size(const generalized_paxos::message_p2a& m)
{
    return pack_size(m.b) + pack_size(m.v);
}

e::packer
consus :: operator << (e::packer pa, const generalized_paxos::message_p2b& rhs)
{
    return pa << rhs.b << rhs.acceptor << rhs.v;
}

e::unpacker
consus :: operator >> (e::unpacker up, generalized_paxos::message_p2b& rhs)
{
    return up >> rhs.b >> rhs.acceptor >> rhs.v;
}

size_t
consus :: pack_size(const generalized_paxos::message_p2b& m)
{
    return pack_size(m.b) + pack_size(m.acceptor) + pack_size(m.v);
}
