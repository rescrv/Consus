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
#include <algorithm>
#include <map>
#include <set>
#include <sstream>

// e
#include <e/serialization.h>
#include <e/strescape.h>

// consus
#include "txman/generalized_paxos.h"

#if GENERALIZED_PAXOS_DEBUG
#define GP_ASSERT(X) do { assert((X)); } while (false)
#define GP_ABORT() do { abort(); } while (false)
#else
#define GP_ASSERT(X) do { } while (false)
#define GP_ABORT() do { } while (false)
#endif

using consus::generalized_paxos;

generalized_paxos :: generalized_paxos()
    : m_init(false)
    , m_interfere(NULL)
    , m_state(PARTICIPATING)
    , m_us()
    , m_acceptors()
    , m_proposed()
    , m_acceptor_ballot()
    , m_acceptor_value()
    , m_acceptor_value_src()
    , m_leader_ballot()
    , m_leader_value()
    , m_promises()
    , m_accepted()
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
    m_accepted.resize(acceptors_sz);
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
    cstruct learn;
    bool conflict = false;
    learned(&learn, &conflict);

    if (m_state >= LEADING_PHASE2 &&
        m_leader_ballot.type == ballot::CLASSIC &&
        cstruct_eq(learn, m_leader_value))
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
            m_leader_value = proven_safe();

            for (size_t i = 0; i < m_promises.size(); ++i)
            {
                if (m_promises[i].b == m_leader_ballot && cstruct_compatible(m_leader_value, m_promises[i].v))
                {
                    m_leader_value = cstruct_lub(m_leader_value, m_promises[i].v);
                }
            }

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
        for (size_t i = 0; i < m_proposed.size(); ++i)
        {
            if (std::find(m_leader_value.commands.begin(),
                          m_leader_value.commands.end(),
                          m_proposed[i]) == m_leader_value.commands.end())
            {
                m_leader_value.commands.push_back(m_proposed[i]);
            }
        }

        *send_m2 = true;
        *m2 = message_p2a(m_leader_ballot, m_leader_value);
    }

    if (m_acceptor_ballot.type == ballot::FAST)
    {
        for (size_t i = 0; i < m_proposed.size(); ++i)
        {
            if (std::find(m_acceptor_value.commands.begin(),
                          m_acceptor_value.commands.end(),
                          m_proposed[i]) == m_acceptor_value.commands.end())
            {
                m_acceptor_value_src = m_acceptor_ballot;
                m_acceptor_value.commands.push_back(m_proposed[i]);
            }
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

    if (m.b.type == ballot::CLASSIC &&
        m.b == m_acceptor_ballot &&
        (m_acceptor_value_src != m_acceptor_ballot ||
         cstruct_le(m_acceptor_value, m.v)))
    {
        m_acceptor_value_src = m_acceptor_ballot;
        m_acceptor_value = m.v;
    }

    *send = true;
    *r = message_p2b(m_acceptor_value_src, m_us, m_acceptor_value);
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

    bool changed = false;

    if (m_accepted[idx].b < m.b)
    {
        m_accepted[idx] = m;
        changed = true;
    }

    if (m_accepted[idx].b == m.b &&
        cstruct_lt(m_accepted[idx].v, m.v))
    {
        m_accepted[idx] = m;
        changed = true;
    }

    return changed;
}

generalized_paxos::cstruct
generalized_paxos :: learned()
{
    cstruct ret;
    bool conflict;
    learned(&ret, &conflict);
    return ret;
}

void
generalized_paxos :: all_accepted_commands(std::vector<command>* commands)
{
    commands->clear();

    for (size_t i = 0; i < m_acceptor_value.commands.size(); ++i)
    {
        commands->push_back(m_acceptor_value.commands[i]);
    }

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        for (size_t j = 0; j < m_accepted[i].v.commands.size(); ++j)
        {
            commands->push_back(m_accepted[i].v.commands[j]);
        }
    }

    std::sort(commands->begin(), commands->end());
    std::vector<command>::iterator it;
    it = std::unique(commands->begin(), commands->end());
    commands->resize(it - commands->begin());
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
generalized_paxos :: learned(cstruct* ret, bool* conflict)
{
    *conflict = false;
    typedef std::map<ballot, uint64_t> ballot_map_t;
    ballot_map_t ballots;

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        ++ballots[m_accepted[i].b];
    }

    std::vector<cstruct> learned_values;

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
        assert(cstruct_compatible(m_learned_cached, learned_values[i]));
        assert(cstruct_compatible(learned_values[i], m_learned_cached));

        for (size_t j = i + 1; j < learned_values.size(); ++j)
        {
            assert(cstruct_compatible(learned_values[i], learned_values[j]));
            assert(cstruct_compatible(learned_values[j], learned_values[i]));
        }
    }
#endif

    *ret = cstruct();

    for (size_t i = 0; i < learned_values.size(); ++i)
    {
        *ret = cstruct_lub(*ret, learned_values[i]);
    }

#if GENERALIZED_PAXOS_DEBUG
    assert(cstruct_compatible(*ret, m_learned_cached));
    *ret = cstruct_lub(*ret, m_learned_cached);
    assert(cstruct_le(m_learned_cached, *ret));
    m_learned_cached = *ret;
#endif
}

void
generalized_paxos :: learned(const ballot& b, std::vector<cstruct>* lv, bool* conflict)
{
    std::vector<cstruct*> vs;

    for (size_t i = 0; i < m_accepted.size(); ++i)
    {
        if (m_accepted[i].b == b)
        {
            vs.push_back(&m_accepted[i].v);
        }
    }

    assert(vs.size() > 0);
    learned(&vs[0], vs.size(), quorum(), lv, conflict);
}

void
generalized_paxos :: learned(cstruct** vs, size_t vs_sz, size_t max_sz,
                             std::vector<cstruct>* lv, bool* conflict)
{
    assert(vs_sz >  0);

    if (vs_sz == 1)
    {
        lv->push_back(*vs[0]);
    }
    else if (vs_sz <= max_sz)
    {
        cstruct tmp;
        learned(vs, vs_sz, &tmp, conflict);
        lv->push_back(tmp);
    }
    else
    {
        // enumerate every subset of vs of size max_sz
        uint64_t limit = 1ULL << vs_sz;
        uint64_t v = (1ULL << max_sz) - 1ULL;
        assert(v < limit);

        while (v < limit)
        {
            std::vector<cstruct*> nvs;

            for (size_t i = 0; i < vs_sz; ++i)
            {
                if ((v & (1ULL << i)))
                {
                    nvs.push_back(vs[i]);
                }
            }

            assert(nvs.size() == max_sz);
            cstruct tmp;
            learned(&nvs[0], nvs.size(), &tmp, conflict);
            lv->push_back(tmp);

            // lexicographically next bit permutation
            // http://graphics.stanford.edu/~seander/bithacks.html#NextBitPermutation
            uint64_t t = (v | (v - 1)) + 1;
            v = t | ((((t & -t) / (v & -v)) >> 1) - 1);
        }
    }
}

void
generalized_paxos :: learned(cstruct** vs, size_t vs_sz,
                             cstruct* v, bool* conflict)
{
    assert(vs_sz > 0);
    *v = *vs[0];

    for (size_t i = 0; i < vs_sz; ++i)
    {
        *v = cstruct_glb(*v, *vs[i], conflict);
    }
}

generalized_paxos::cstruct
generalized_paxos :: proven_safe()
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
    std::vector<cstruct> gamma_R;

    while (v < limit)
    {
        std::vector<cstruct*> vs;

        for (size_t i = 0; i < m_promises.size(); ++i)
        {
            // if in R            && in Q
            if ((v & (1ULL << i)) && m_promises[i].b == m_leader_ballot)
            {
                if (m_promises[i].vb == k && !m_promises[i].v.is_none())
                {
                    vs.push_back(&m_promises[i].v);
                }
            }
        }

        if (!vs.empty())
        {
            cstruct tmp;
            bool conflict = false;
            learned(&vs[0], vs.size(), &tmp, &conflict);
            gamma_R.push_back(tmp);
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
                return m_promises[i].v;
            }
        }
    }

    cstruct ret;

    for (size_t i = 0; i < gamma_R.size(); ++i)
    {
        for (size_t j = i + 1; j < gamma_R.size(); ++j)
        {
            if (!cstruct_compatible(gamma_R[i], gamma_R[j]))
            {
                return ret;
            }
        }
    }

    for (size_t i = 0; i < gamma_R.size(); ++i)
    {
        ret = cstruct_lub(ret, gamma_R[i]);
    }

    return ret;
}

bool
generalized_paxos :: cstruct_lt(const cstruct& lhs, const cstruct& rhs)
{
    if (lhs.commands.size() >= rhs.commands.size())
    {
        return false;
    }

    return cstruct_le(lhs, rhs) && !cstruct_eq(lhs, rhs);
}

bool
generalized_paxos :: cstruct_le(const cstruct& lhs, const cstruct& rhs)
{
    if (lhs.commands.size() > rhs.commands.size())
    {
        return false;
    }

    std::vector<command> lhs_elem;
    std::vector<command> rhs_elem;
    partial_order_t lhs_order;
    partial_order_t rhs_order;
    cstruct_pieces(lhs, &lhs_elem, &lhs_order);
    cstruct_pieces(rhs, &rhs_elem, &rhs_order);

    // if rhs doesn't include every element included by lhs
    if (!std::includes(rhs_elem.begin(), rhs_elem.end(),
                       lhs_elem.begin(), lhs_elem.end()))
    {
        return false;
    }

    // if rhs doesn't order every element that was ordered by lhs
    //
    // the above check and symmetry of interference means that rhs must have an
    // order between every pair of elements ordered by lhs, so this will really
    // fail when elements in rhs are ordered differently than those in lhs
    if (!std::includes(rhs_order.begin(), rhs_order.end(),
                       lhs_order.begin(), lhs_order.end()))
    {
        return false;
    }

    // a sequence of commands that were added to [lhs] to get [rhs]
    // these are only in rhs;
    std::vector<command> seq;
    std::set_difference(rhs_elem.begin(), rhs_elem.end(),
                        lhs_elem.begin(), lhs_elem.end(),
                        std::inserter(seq, seq.begin()));
    // sort for fast binary search
    std::sort(seq.begin(), seq.end());

    // check that for every pairwise ordering v, w in rhs:
    //      if v in seq => w in seq
    // if this is not true, then w is in lhs, and rhs cannot add an element
    // earlier in the relation than lhs has and be considered >=
    for (partial_order_t::iterator it = rhs_order.begin();
            it != rhs_order.end(); ++it)
    {
        if (std::binary_search(seq.begin(), seq.end(), it->first) &&
            !std::binary_search(seq.begin(), seq.end(), it->second))
        {
            return false;
        }
    }

    return true;
}

bool
generalized_paxos :: cstruct_eq(const cstruct& lhs, const cstruct& rhs)
{
    if (lhs.commands.size() != rhs.commands.size())
    {
        return false;
    }

    std::vector<command> lhs_elem;
    std::vector<command> rhs_elem;
    partial_order_t lhs_order;
    partial_order_t rhs_order;
    cstruct_pieces(lhs, &lhs_elem, &lhs_order);
    cstruct_pieces(rhs, &rhs_elem, &rhs_order);
    return lhs_elem.size() == rhs_elem.size() && lhs_order.size() == rhs_order.size() &&
           std::equal(lhs_elem.begin(), lhs_elem.end(), rhs_elem.begin()) &&
           std::equal(lhs_order.begin(), lhs_order.end(), rhs_order.begin());
}

bool
generalized_paxos :: cstruct_compatible(const cstruct& lhs, const cstruct& rhs)
{
    std::vector<command> lhs_elem;
    std::vector<command> rhs_elem;
    partial_order_t lhs_order;
    partial_order_t rhs_order;
    cstruct_pieces(lhs, &lhs_elem, &lhs_order);
    cstruct_pieces(rhs, &rhs_elem, &rhs_order);

    // From the GP Tech Report:
    // [σ] and [τ] are compatible iff the subgraphs of G(σ) and G(τ) consisting
    // of the nodes they have in common are identical, and C does not conflict
    // with D for every node C in G(σ) that is not in G(τ) and every node D in
    // G(τ) that is not in G(σ).
    //
    // We will say σ=lhs and τ=rhs

    std::vector<command> C;
    std::set_difference(lhs_elem.begin(), lhs_elem.end(),
                        rhs_elem.begin(), rhs_elem.end(),
                        std::back_inserter(C));
    std::sort(C.begin(), C.end());
    std::vector<command> D;
    std::set_difference(rhs_elem.begin(), rhs_elem.end(),
                        lhs_elem.begin(), lhs_elem.end(),
                        std::back_inserter(D));
    std::sort(D.begin(), D.end());

    for (size_t c = 0; c < C.size(); ++c)
    {
        for (size_t d = 0; d < D.size(); ++d)
        {
            if (m_interfere->conflict(C[c], D[d]))
            {
                return false;
            }
        }
    }

    std::vector<command> commands_in_common;
    std::set_intersection(lhs_elem.begin(), lhs_elem.end(),
                          rhs_elem.begin(), rhs_elem.end(),
                          std::back_inserter(commands_in_common));
    std::sort(commands_in_common.begin(), commands_in_common.end());

    partial_order_t edges_not_in_common;
    std::set_symmetric_difference(lhs_order.begin(), lhs_order.end(),
                                  rhs_order.begin(), rhs_order.end(),
                                  std::inserter(edges_not_in_common,
                                                edges_not_in_common.begin()));

    for (partial_order_t::iterator it = edges_not_in_common.begin();
            it != edges_not_in_common.end(); ++it)
    {
        if (std::binary_search(commands_in_common.begin(),
                               commands_in_common.end(),
                               it->first) &&
            std::binary_search(commands_in_common.begin(),
                               commands_in_common.end(),
                               it->second))
        {
            return false;
        }
    }

    return true;
}

generalized_paxos::cstruct
generalized_paxos :: cstruct_glb(const cstruct& lhs, const cstruct& rhs, bool* conflict)
{
    std::vector<command> lhs_cmds;
    std::vector<command> rhs_cmds;
    partial_order_t edge_list;
    cstruct_pieces(lhs, &lhs_cmds, &edge_list);
    cstruct_pieces(rhs, &rhs_cmds, &edge_list);
    std::vector<command> all_cmds;
    std::set_union(lhs_cmds.begin(), lhs_cmds.end(),
                   rhs_cmds.begin(), rhs_cmds.end(),
                   std::back_inserter(all_cmds));
    std::sort(all_cmds.begin(), all_cmds.end());

    // determine commands to exclude
    std::vector<command> exclude;
    exclude.reserve(lhs_cmds.size() + rhs_cmds.size());
    std::set_symmetric_difference(lhs_cmds.begin(), lhs_cmds.end(),
                                  rhs_cmds.begin(), rhs_cmds.end(),
                                  std::back_inserter(exclude));

    for (size_t i = 0; i < all_cmds.size(); ++i)
    {
        const command& e(all_cmds[i]);

        if (directed_path_exists(e, e, edge_list))
        {
            *conflict = true;
            exclude.push_back(e);
        }
    }

    for (size_t i = 0; i < exclude.size(); ++i)
    {
        const command& u(exclude[i]);

        for (size_t j = 0; j < all_cmds.size(); ++j)
        {
            const command& v(all_cmds[j]);

            if (directed_path_exists(u, v, edge_list) &&
                std::find(exclude.begin(), exclude.end(), v) == exclude.end())
            {
                exclude.push_back(v);
            }
        }
    }

    std::sort(exclude.begin(), exclude.end());
    std::vector<command>::iterator it;
    it = std::unique(exclude.begin(), exclude.end());
    exclude.resize(it - exclude.begin());

    // create a new cstruct including only those commands that are not excluded
    cstruct tmp;

    for (size_t i = 0; i < lhs.commands.size(); ++i)
    {
        const command& c(lhs.commands[i]);

        if (!std::binary_search(exclude.begin(), exclude.end(), c))
        {
            tmp.commands.push_back(c);
        }
    }

    return tmp;
}

generalized_paxos::cstruct
generalized_paxos :: cstruct_lub(const cstruct& lhs, const cstruct& rhs)
{
#if GENERALIZED_PAXOS_DEBUG
    assert(cstruct_compatible(lhs, rhs));
#endif
    std::vector<command> lhs_elem(lhs.commands);
    std::vector<command> rhs_elem(rhs.commands);
    std::sort(lhs_elem.begin(), lhs_elem.end());
    std::sort(rhs_elem.begin(), rhs_elem.end());

    std::vector<command> D;
    std::set_difference(rhs_elem.begin(), rhs_elem.end(),
                        lhs_elem.begin(), lhs_elem.end(),
                        std::back_inserter(D));
    std::sort(D.begin(), D.end());
    cstruct tmp(lhs);

    for (size_t i = 0; i < rhs.commands.size(); ++i)
    {
        if (std::binary_search(D.begin(), D.end(), rhs.commands[i]))
        {
            tmp.commands.push_back(rhs.commands[i]);
        }
    }

    return tmp;
}

void
generalized_paxos :: cstruct_pieces(const cstruct& c,
                                    std::vector<command>* commands,
                                    partial_order_t* order)
{
    if (commands)
    {
        *commands = c.commands;
        std::sort(commands->begin(), commands->end());
    }

    for (size_t i = 0; i < c.commands.size(); ++i)
    {
        const command& v(c.commands[i]);

        for (size_t j = i + 1; j < c.commands.size(); ++j)
        {
            const command& w(c.commands[j]);

            if (m_interfere->conflict(v, w))
            {
                order->insert(std::make_pair(v, w));
            }
        }
    }
}

bool
generalized_paxos :: directed_path_exists(const command& from,
                                          const command& to,
                                          const partial_order_t& edge_list)
{
    std::set<command> seen;
    return directed_path_exists(from, to, edge_list, &seen);
}

bool
generalized_paxos :: directed_path_exists(const command& from,
                                          const command& to,
                                          const partial_order_t& edge_list,
                                          std::set<command>* seen)
{
    for (partial_order_t::iterator it = edge_list.lower_bound(std::make_pair(from, command()));
            it != edge_list.end() && it->first == from; ++it)
    {
        if (it->second == to)
        {
            return true;
        }

        if (seen->find(it->second) == seen->end())
        {
            seen->insert(it->second);

            if (directed_path_exists(it->second, to, edge_list, seen))
            {
                return true;
            }
        }
    }

    return false;
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
