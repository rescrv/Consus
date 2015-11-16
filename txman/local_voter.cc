// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/network_msgtype.h"
#include "txman/daemon.h"
#include "txman/local_voter.h"
#include "txman/log_entry_t.h"

#pragma GCC diagnostic ignored "-Wlarger-than="

using consus::local_voter;

local_voter :: local_voter(const transaction_group& tg)
    : m_tg(tg)
    , m_mtx()
    , m_initialized(false)
    , m_group()
    , m_votes()
    , m_timestamps()
    , m_has_preferred_vote(false)
    , m_preferred_vote(0)
    , m_has_outcome(false)
    , m_outcome(0)
    , m_outcome_in_dispositions(false)
{
    po6::threads::mutex::hold hold(&m_mtx);

    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        m_phases[i] = paxos_synod::PHASE1;
        m_timestamps[i] = 0;
    }
}

local_voter :: ~local_voter() throw ()
{
}

const consus::transaction_group&
local_voter :: state_key() const
{
    return m_tg;
}

bool
local_voter :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return !m_initialized || m_outcome_in_dispositions;
}

void
local_voter :: set_preferred_vote(uint64_t v)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!m_has_preferred_vote)
    {
        m_has_preferred_vote = true;
        m_preferred_vote = v;
    }
}

void
local_voter :: vote_1a(comm_id id, unsigned idx, const paxos_synod::ballot& b, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_paxos(d))
    {
        return;
    }

    if (idx >= m_group.members_sz ||
        id != b.leader)
    {
        return;
    }

    paxos_synod::ballot a;
    paxos_synod::pvalue p;
    m_votes[idx].phase1a(b, &a, &p);

    std::string entry;
    e::packer(&entry)
        << LOG_ENTRY_LOCAL_VOTE_1A << m_tg << uint8_t(idx) << b;
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(LV_VOTE_1B)
                    + pack_size(m_tg)
                    + sizeof(uint8_t)
                    + pack_size(a)
                    + pack_size(p);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << LV_VOTE_1B << m_tg << uint8_t(idx) << a << p;
    d->send_when_durable(entry, b.leader, msg);
}

void
local_voter :: vote_1b(comm_id id, unsigned idx,
                       const paxos_synod::ballot& b,
                       const paxos_synod::pvalue& p,
                       daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (idx >= m_group.members_sz)
    {
        return;
    }

    m_votes[idx].phase1b(id, b, p);
    work_state_machine(d);
}

void
local_voter :: vote_2a(comm_id id, unsigned idx, const paxos_synod::pvalue& p, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (idx >= m_group.members_sz ||
        id != p.b.leader)
    {
        return;
    }

    bool send = false;
    m_votes[idx].phase2a(p, &send);

    if (send)
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_LOCAL_VOTE_2A << m_tg << uint8_t(idx) << p;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(LV_VOTE_2B)
                        + pack_size(m_tg)
                        + sizeof(uint8_t)
                        + pack_size(p);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << LV_VOTE_2B << m_tg << uint8_t(idx) << p;
        d->send_when_durable(entry, p.b.leader, msg);
    }
}

void
local_voter :: vote_2b(comm_id id, unsigned idx,
                       const paxos_synod::pvalue& p,
                       daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (idx >= m_group.members_sz)
    {
        return;
    }

    paxos_synod::phase_t x = m_votes[idx].phase();
    m_votes[idx].phase2b(id, p);

    if (x != paxos_synod::LEARNED &&
        m_votes[idx].phase() == paxos_synod::LEARNED)
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_LOCAL_LEARN << m_tg << uint8_t(idx) << m_votes[idx].learned();
        d->m_log.append(entry.data(), entry.size());
    }

    work_state_machine(d);
}

void
local_voter :: vote_learn(unsigned idx, uint64_t v, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (idx >= m_group.members_sz)
    {
        return;
    }

    if (m_votes[idx].phase() == paxos_synod::LEARNED && 
        m_votes[idx].learned() != v)
    {
        // this should never happen; let's catch if it does so we can make sure
        // it doesn't happen in the future
        LOG(ERROR) << "synod_commit learned inconsistent values: " << m_votes[idx].learned() << " vs " << v;
    }
    else if (m_votes[idx].phase() != paxos_synod::LEARNED)
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_LOCAL_LEARN << m_tg << uint8_t(idx) << v;
        d->m_log.append(entry.data(), entry.size());
    }

    m_votes[idx].force_learn(v);
    work_state_machine(d);
}

void
local_voter :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

bool
local_voter :: outcome(uint64_t* v)
{
    po6::threads::mutex::hold hold(&m_mtx);
    *v = m_outcome;
    return m_has_outcome;
}

bool
local_voter :: preconditions_for_paxos(daemon* d)
{
    uint64_t outcome;

    if (d->m_dispositions.get(m_tg, &outcome))
    {
        m_outcome_in_dispositions = true;
        return false;
    }

    if (!m_initialized)
    {
        const paxos_group* group = d->get_config()->get_group(m_tg.group);

        if (!group)
        {
            return false;
        }

        m_group = *group;

        for (size_t i = 0; i < m_group.members_sz; ++i)
        {
            m_votes[i].init(d->m_us.id, m_group);
            m_timestamps[i] = 0;
        }

        m_initialized = true;
    }

    return true;
}

void
local_voter :: work_state_machine(daemon* d)
{
    if (!preconditions_for_paxos(d))
    {
        return;
    }

    unsigned our_idx = m_group.index(d->m_us.id);
    assert(our_idx < m_group.members_sz);

    if (m_has_preferred_vote)
    {
        work_paxos_vote(our_idx, d, m_preferred_vote);
    }

    for (unsigned i = 1; i < m_group.members_sz; ++i)
    {
        unsigned idx = (our_idx + i) % m_group.members_sz;

        // XXX this is not robust if the coordinator totally goes missing
        if (d->get_config()->get_state(m_group.members[idx]) == txman_state::ONLINE)
        {
            break;
        }

        work_paxos_vote(idx, d, CONSUS_VOTE_ABORT);
    }

    unsigned voted = 0;
    unsigned committed = 0;

    for (size_t i = 0; i < m_group.members_sz; ++i)
    {
        if (m_votes[i].phase() == paxos_synod::LEARNED)
        {
            ++voted;

            if (m_votes[i].learned() == CONSUS_VOTE_COMMIT)
            {
                ++committed;
            }
            else if (m_votes[i].learned() != CONSUS_VOTE_ABORT)
            {
                LOG(ERROR) << m_tg << ".synod_commit[" << i << "] learned invalid value " << m_votes[i].learned();
            }
        }
    }

    assert(voted <= m_group.members_sz);
    unsigned aborted = voted - committed;
    assert(aborted < m_group.quorum() || committed < m_group.quorum());

    if (aborted >= m_group.quorum())
    {
        m_has_outcome = true;
        m_outcome = CONSUS_VOTE_ABORT;
    }

    if (committed >= m_group.quorum())
    {
        m_has_outcome = true;
        m_outcome = CONSUS_VOTE_COMMIT;
    }

    if (d->m_dispositions.has(m_tg))
    {
        m_outcome_in_dispositions = true;
    }
}

void
local_voter :: work_paxos_vote(unsigned idx, daemon* d, uint64_t preference)
{
    paxos_synod::ballot b;
    paxos_synod::pvalue p;
    paxos_synod* ps = &m_votes[idx];
    std::auto_ptr<e::buffer> msg(e::buffer::create(pack_size(m_tg) + 64));
    const uint64_t now = po6::monotonic_time();

    switch (ps->phase())
    {
        case paxos_synod::PHASE1:
            ps->phase1(&b);
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << LV_VOTE_1A << m_tg << uint8_t(idx) << b;

            if (m_phases[idx] != paxos_synod::PHASE1 ||
                m_timestamps[idx] + d->resend_interval() < now)
            {
                d->send(m_group, msg);
                m_phases[idx] = paxos_synod::PHASE1;
                m_timestamps[idx] = now;
            }

            break;
        case paxos_synod::PHASE2:
            ps->phase2(&p, preference);
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << LV_VOTE_2A << m_tg << uint8_t(idx) << p;

            if (m_phases[idx] != paxos_synod::PHASE2 ||
                m_timestamps[idx] + d->resend_interval() < now)
            {
                d->send(m_group, msg);
                m_phases[idx] = paxos_synod::PHASE2;
                m_timestamps[idx] = now;
            }

            break;
        case paxos_synod::LEARNED:
            if (m_phases[idx] != paxos_synod::LEARNED ||
                m_timestamps[idx] + d->resend_interval() < now)
            {
                msg->pack_at(BUSYBEE_HEADER_SIZE)
                    << LV_VOTE_LEARN << m_tg << uint8_t(idx) << m_votes[idx].learned();
                d->send(m_group, msg);
                m_phases[idx] = paxos_synod::LEARNED;
                m_timestamps[idx] = now;
            }

            break;
        default:
            ::abort();
    }
}
