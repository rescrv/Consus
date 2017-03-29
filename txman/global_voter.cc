// Copyright (c) 2015-2017, Robert Escriva, Cornell University
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

// Google Log
#include <glog/logging.h>

// e
#include <e/base64.h>
#include <e/endian.h>
#include <e/varint.h>
#include <e/strescape.h>

// BusyBee
#include <busybee.h>

// consus
#include "common/util.h"
#include "txman/daemon.h"
#include "txman/global_voter.h"

using consus::global_voter;

extern bool s_debug_mode;

static const char*
value_to_string(uint64_t v)
{
    const char* value = NULL;

    if (v == CONSUS_VOTE_COMMIT)
    {
        value = "COMMIT";
    }
    else if (v == CONSUS_VOTE_ABORT)
    {
        value = "ABORT";
    }
    else
    {
        value = "???";
    }

    assert(value);
    return value;
}

namespace
{
using namespace consus;

template<typename T>
void
copy(T* ids, size_t ids_sz, abstract_id* aids)
{
    for (size_t i = 0; i < ids_sz; ++i)
    {
        aids[i] = abstract_id(ids[i].get());
    }
}

// ph->pretty_hash

std::string
ph(const generalized_paxos::ballot& b)
{
    const char* t = b.type == generalized_paxos::ballot::FAST ? "F" : "C";
    unsigned char buf[2 * VARINT_64_MAX_SIZE];
    unsigned char* ptr = buf;
    ptr = e::packvarint64(b.leader.get(), ptr);
    ptr = e::packvarint64(b.number, ptr);
    char b64[2 * sizeof(buf)];
    size_t sz = e::b64_ntop(buf, ptr - buf, b64, sizeof(b64));
    assert(sz <= sizeof(b64));
    return std::string("ballot:") + t + std::string(b64, sz);
}

} // namespace

struct global_voter::data_center_comparator : public generalized_paxos::comparator
{
    data_center_comparator(global_comparator* _gc) : gc(_gc) {}
    virtual ~data_center_comparator() throw () {}

    virtual bool conflict(const generalized_paxos::command& a, const generalized_paxos::command& b) const;

    private:
        global_comparator* gc;

        data_center_comparator(const data_center_comparator&);
        data_center_comparator& operator = (const data_center_comparator&);
};

bool
global_voter :: data_center_comparator :: conflict(const generalized_paxos::command& a,
                                                   const generalized_paxos::command& b) const
{
    (void)a;// XXX
    (void)b;// XXX
    return true;//XXX
}

struct global_voter::global_comparator : public generalized_paxos::comparator
{
    global_comparator() {}
    virtual ~global_comparator() throw () {}

    virtual bool conflict(const generalized_paxos::command& a, const generalized_paxos::command& b) const;
};

bool
global_voter :: global_comparator :: conflict(const generalized_paxos::command& a,
                                              const generalized_paxos::command& b) const
{
    return a.type >= CONSUS_MAX_REPLICATION_FACTOR ||
           b.type >= CONSUS_MAX_REPLICATION_FACTOR;
}

global_voter :: global_voter(const transaction_group& tg)
    : m_tg(tg)
    , m_mtx()
    , m_data_center_init(false)
    , m_global_init(false)
    , m_outcome_in_dispositions(false)
    , m_data_center_cmp(new data_center_comparator(m_global_cmp.get()))
    , m_data_center_gp()
    , m_highest_log_entry(0)
    , m_dc_prev_learned()
    , m_xmit_vote()
    , m_xmit_outer_m1a()
    , m_xmit_outer_m2a()
    , m_xmit_outer_m2b()
    , m_xmit_inner_m1a()
    , m_xmit_inner_m1b()
    , m_xmit_inner_m2a()
    , m_xmit_inner_m2b()
    , m_local_vote(0)
    , m_dcs_sz(0)
    , m_global_cmp(new global_comparator())
    , m_global_gp()
    , m_global_exec()
    , m_has_outcome(false)
    , m_outcome(0)
{
    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        m_dcs_timestamps[i] = 0;
    }
}

global_voter :: ~global_voter() throw ()
{
}

const consus::transaction_group&
global_voter :: state_key() const
{
    return m_tg;
}

bool
global_voter :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return (!m_data_center_init && !m_global_init)/* XXX || m_outcome_in_dispositions*/;
}

bool
global_voter :: initialized()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_global_init;
}

void
global_voter :: init(uint64_t vote, const paxos_group_id* dcs, size_t dcs_sz, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    assert(!m_global_init);
    assert(dcs_sz < CONSUS_MAX_REPLICATION_FACTOR);
    assert(std::find(dcs, dcs + dcs_sz, m_tg.group) != dcs + dcs_sz);

    if (!preconditions_for_data_center_paxos(d))
    {
        return;
    }

    m_local_vote = vote;
    // XXX inefficient to do always; do only in debug mode
    std::ostringstream ostr;
    ostr << "[";

    for (unsigned i = 0; i < dcs_sz; ++i)
    {
        if (i > 0)
        {
            ostr << ", ";
        }

        if (dcs[i] == m_tg.group)
        {
            ostr << "(" << dcs[i] << ")";
        }
        else
        {
            ostr << dcs[i];
        }

        m_dcs[i] = dcs[i];
    }

    ostr << "]";
    m_dcs_sz = dcs_sz;
    abstract_id global_acceptors[CONSUS_MAX_REPLICATION_FACTOR];
    copy(m_dcs, m_dcs_sz, global_acceptors);
    m_global_gp.init(m_global_cmp.get(), abstract_id(m_tg.group.get()), global_acceptors, m_dcs_sz);
    // do this at the very end so any failures lead to this being GC'd
    LOG_IF(INFO, s_debug_mode) << logid() << "data centers: " << ostr.str();
    m_global_init = true;
    work_state_machine(d);
}

bool
global_voter :: propose(const generalized_paxos::command& c, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_data_center_paxos(d))
    {
        return m_has_outcome;
    }

    bool proposed = m_data_center_gp.propose(c);

    if (proposed && s_debug_mode)
    {
        LOG_IF(INFO, s_debug_mode)
            << logid()
            << "proposing state machine transition "
            << pretty_print_outer(c);
    }

    if (proposed)
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_GLOBAL_PROPOSE << m_tg << c;
        int64_t id = d->m_log.append(entry.data(), entry.size());
        m_highest_log_entry = std::max(m_highest_log_entry, id);
        work_state_machine(d);
    }

    return m_has_outcome;
}

bool
global_voter :: process_p1a(comm_id id, const generalized_paxos::message_p1a& m, daemon* d)
{
    bool send = false;
    generalized_paxos::message_p1b r;
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_data_center_paxos(d))
    {
        return m_has_outcome;
    }

    if (id != comm_id(m.b.leader.get()))
    {
        LOG(ERROR) << logid() << "dropping 1a message led by " << comm_id(m.b.leader.get()) << " received from " << id;
        return m_has_outcome;
    }

    if (m.b > m_data_center_gp.acceptor_ballot())
    {
        // separated out so we log just once
        //
        // send will always be true when m.b >= m_data_center_gp.acceptor_ballot(),
        // leading to a log entry on each and every rexmit of p1a if this were
        // folded into the if(send) down below.
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_GLOBAL_VOTE_1A << m_tg << m;
        int64_t x = d->m_log.append(entry.data(), entry.size());
        m_highest_log_entry = std::max(m_highest_log_entry, x);
        LOG_IF(INFO, s_debug_mode) << logid() << "following " << ph(m.b);
    }

    m_data_center_gp.process_p1a(m, &send, &r);

    if (send)
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_VOTE_1B)
                        + pack_size(m_tg)
                        + pack_size(r);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_VOTE_1B << m_tg << r;
        d->send_when_durable(m_highest_log_entry, id, msg);
    }

    work_state_machine(d);
    return m_has_outcome;
}

bool
global_voter :: process_p1b(const generalized_paxos::message_p1b& m, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_data_center_paxos(d))
    {
        return m_has_outcome;
    }

    bool processed = m_data_center_gp.process_p1b(m);

    if (processed)
    {
        LOG_IF(INFO, s_debug_mode) << logid() << ph(m.b) << " followed by " << comm_id(m.acceptor.get());
        work_state_machine(d);
    }

    return m_has_outcome;
}

bool
global_voter :: process_p2a(comm_id id, const generalized_paxos::message_p2a& m, daemon* d)
{
    bool send = false;
    generalized_paxos::message_p2b r;
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_data_center_paxos(d))
    {
        return m_has_outcome;
    }

    if (id != comm_id(m.b.leader.get()))
    {
        LOG(ERROR) << logid() << "dropping 2a message led by " << comm_id(m.b.leader.get()) << " received from " << id;
        return m_has_outcome;
    }

    m_data_center_gp.process_p2a(m, &send, &r);
    const uint64_t now = po6::monotonic_time();

    if (send && r != m_xmit_outer_m2b.value())
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_GLOBAL_VOTE_2A << m_tg << m;
        int64_t x = d->m_log.append(entry.data(), entry.size());
        m_highest_log_entry = std::max(m_highest_log_entry, x);
        LOG_IF(INFO, s_debug_mode)
            << logid() << ph(m.b)
            << " suggests state machine input "
            << pretty_print_outer(m.v);
    }

    if (send && m_xmit_outer_m2b.may_transmit(r, now, d))
    {
        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_outer_m2b.transmit_params(r, now, m_highest_log_entry, &log_entry, &send_func);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_VOTE_2B)
                        + pack_size(m_tg)
                        + pack_size(r);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_VOTE_2B << m_tg << r;
        (d->*send_func)(log_entry, m_tg.group, msg);
    }

    work_state_machine(d);
    return m_has_outcome;
}

bool
global_voter :: process_p2b(const generalized_paxos::message_p2b& m, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!preconditions_for_data_center_paxos(d))
    {
        return m_has_outcome;
    }

    bool processed = m_data_center_gp.process_p2b(m);

    if (processed)
    {
        std::string entry;
        e::packer(&entry)
            << LOG_ENTRY_GLOBAL_VOTE_2B << m_tg << m;
        int64_t x = d->m_log.append(entry.data(), entry.size());
        m_highest_log_entry = std::max(m_highest_log_entry, x);
        LOG_IF(INFO, s_debug_mode)
            << logid() << comm_id(m.acceptor.get())
            << " accepted state machine input "
            << pretty_print_outer(m.v);
        work_state_machine(d);
    }

    return m_has_outcome;
}

void
global_voter :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

bool
global_voter :: outcome(uint64_t* v)
{
    po6::threads::mutex::hold hold(&m_mtx);
    *v = m_outcome;
    return m_has_outcome;
}

void
global_voter :: unvoted_data_centers(paxos_group_id* dcs, size_t* dcs_sz)
{
    m_mtx.lock();
    *dcs_sz = m_dcs_sz;

    for (size_t i = 0; i < m_dcs_sz; ++i)
    {
        dcs[i] = m_dcs[i];
    }

    std::vector<generalized_paxos::command> cmds;
    m_data_center_gp.all_accepted_commands(&cmds);
    m_mtx.unlock();

    for (std::vector<generalized_paxos::command>::iterator it = cmds.begin();
            it != cmds.end(); ++it)
    {
        const generalized_paxos::command& c(*it);
        generalized_paxos::command inner_c;

        if (static_cast<transition_t>(c.type) != GLOBAL_VOTER_COMMAND)
        {
            continue;
        }

        e::unpacker up(c.value);
        up = up >> inner_c;

        if (up.error())
        {
            continue;
        }

        dcs[inner_c.type % CONSUS_MAX_REPLICATION_FACTOR] = paxos_group_id();
    }

    for (size_t i = 0; i < *dcs_sz; )
    {
        if (dcs[i] == paxos_group_id())
        {
            std::swap(dcs[i], dcs[*dcs_sz - 1]);
            --*dcs_sz;
        }
        else
        {
            ++i;
        }
    }
}

std::string
global_voter :: debug_dump()
{
    po6::threads::mutex::hold hold(&m_mtx);
    std::ostringstream ostr;
    std::string tmp;
    ostr << "data center " << (m_data_center_init ? "" : "un") << "initialized\n";
    ostr << "global " << (m_global_init ? "" : "un") << "initialized\n";
    ostr << "local dc outcome " << value_to_string(m_local_vote) << "\n";
    tmp = m_data_center_gp.debug_dump(e::compat::bind(&global_voter::pretty_print_outer_cstruct, this, e::compat::placeholders::_1),
                                      e::compat::bind(&global_voter::pretty_print_outer_command, this, e::compat::placeholders::_1));
    ostr << prefix_lines("data center paxos: ", tmp);

    for (size_t i = 0; i < m_global_exec.size(); ++i)
    {
        ostr << "command[" << i << "] " << pretty_print_outer(m_global_exec[i]) << "\n";
    }

    tmp = m_global_gp.debug_dump(e::compat::bind(&global_voter::pretty_print_inner_cstruct, this, e::compat::placeholders::_1),
                                 e::compat::bind(&global_voter::pretty_print_inner_command, this, e::compat::placeholders::_1));
    ostr << prefix_lines("global paxos: ", tmp);

    if (m_has_outcome)
    {
        ostr << "outcome " << value_to_string(m_outcome) << "\n";
    }
    else
    {
        ostr << "no outcome\n";
    }

    if (m_outcome_in_dispositions)
    {
        ostr << "outcome in dispositions\n";
    }

    return ostr.str();
}

std::string
global_voter :: logid()
{
    return transaction_group::log(m_tg) + " global voter: ";
}

std::string
global_voter :: pretty_print_outer_cstruct(const generalized_paxos::cstruct& v)
{
    std::ostringstream ostr;
    ostr << "[";

    for (size_t i = 0; i < v.commands.size(); ++i)
    {
        if (i > 0)
        {
            ostr << ", ";
        }

        ostr << pretty_print_outer(v.commands[i]);
    }

    ostr << "]";
    return ostr.str();
}

std::string
global_voter :: pretty_print_outer_command(const generalized_paxos::command& c)
{
    std::ostringstream ostr;
    generalized_paxos::command inner_c;
    generalized_paxos::message_p1a inner_m1a;
    generalized_paxos::message_p1b inner_m1b;
    generalized_paxos::message_p2a inner_m2a;
    generalized_paxos::message_p2b inner_m2b;
    e::unpacker up(c.value);

    switch (static_cast<global_voter::transition_t>(c.type))
    {
        case GLOBAL_VOTER_COMMAND:
            up = up >> inner_c;

            if (!up.error())
            {
                ostr << pretty_print_inner(inner_c);
            }
            else
            {
                ostr << "invalid command";
            }

            break;
        case GLOBAL_VOTER_MESSAGE_1A:
            up = up >> inner_m1a;

            if (!up.error())
            {
                ostr << "1A:" << ph(inner_m1a.b);
            }
            else
            {
                ostr << "invalid m1a";
            }

            break;
        case GLOBAL_VOTER_MESSAGE_1B:
            up = up >> inner_m1b;

            if (!up.error())
            {
                unsigned idx = std::find(m_dcs, m_dcs + m_dcs_sz, paxos_group_id(inner_m1b.acceptor.get())) - m_dcs;
                ostr << idx << ":1B:" << ph(inner_m1b.b);
            }
            else
            {
                ostr << "invalid m1b";
            }

            break;
        case GLOBAL_VOTER_MESSAGE_2A:
            up = up >> inner_m2a;

            if (!up.error())
            {
                ostr << "2A:" << ph(inner_m2a.b) << ":" << pretty_print_inner(inner_m2a.v);
            }
            else
            {
                ostr << "invalid m2a";
            }

            break;
        case GLOBAL_VOTER_MESSAGE_2B:
            up = up >> inner_m2b;

            if (!up.error())
            {
                unsigned idx = std::find(m_dcs, m_dcs + m_dcs_sz, paxos_group_id(inner_m2b.acceptor.get())) - m_dcs;
                ostr << idx << ":2B:" << ph(inner_m2b.b) << ":" << pretty_print_inner(inner_m2b.v);
            }
            else
            {
                ostr << "invalid m2b";
            }

            break;
        default:
            ostr << "unknown transition";
            break;
    }

    return ostr.str();
}

std::string
global_voter :: pretty_print_inner_cstruct(const generalized_paxos::cstruct& v)
{
    std::ostringstream ostr;
    ostr << "[";

    for (size_t i = 0; i < v.commands.size(); ++i)
    {
        if (i > 0)
        {
            ostr << ", ";
        }

        ostr << pretty_print_inner(v.commands[i]);
    }

    ostr << "]";
    return ostr.str();
}

std::string
global_voter :: pretty_print_inner_command(const generalized_paxos::command& c)
{
    // It's tempting to clean this up to print paxos_group_id or something even
    // more user friendly.  Instead, this method prints member index because
    // there is no guarantee that the m_dcs array is initialized when this is
    // called
    std::ostringstream ostr;
    char v = '?';

    if (c.value == std::string("commit\x00\x00", 8))
    {
        v = 'C';
    }

    if (c.value == std::string("abort\x00\x00\x00", 8))
    {
        v = 'A';
    }

    if (c.type >= 2 * CONSUS_MAX_REPLICATION_FACTOR)
    {
        ostr << "ERR";
    }
    else if (c.type >= CONSUS_MAX_REPLICATION_FACTOR)
    {
        ostr << (c.type - CONSUS_MAX_REPLICATION_FACTOR) << "r" << v;
    }
    else
    {
        ostr << c.type << "v" << v;
    }

    return ostr.str();
}

bool
global_voter :: preconditions_for_data_center_paxos(daemon* d)
{
    if (!m_data_center_init)
    {
        const paxos_group* group = d->get_config()->get_group(m_tg.group);
        abstract_id data_center_acceptors[CONSUS_MAX_REPLICATION_FACTOR];
        copy(group->members, group->members_sz, data_center_acceptors);
        m_data_center_gp.init(m_data_center_cmp.get(), abstract_id(d->m_us.id.get()), data_center_acceptors, group->members_sz);
        m_data_center_init = true;
    }

    return true;
}

bool
global_voter :: preconditions_for_global_paxos(daemon*)
{
    return m_global_init;
}

void
global_voter :: work_state_machine(daemon* d)
{
    if (!preconditions_for_data_center_paxos(d))
    {
        return;
    }

    bool send_m1;
    bool send_m2;
    bool send_m3;
    generalized_paxos::message_p1a m1;
    generalized_paxos::message_p2a m2;
    generalized_paxos::message_p2b m3;
    m_data_center_gp.advance(d->get_config()->get_group(m_tg.group)->members[0] == d->m_us.id/*XXX*/,
                             &send_m1, &m1,
                             &send_m2, &m2,
                             &send_m3, &m3);
    const uint64_t now = po6::monotonic_time();

    if (send_m1 && m_xmit_outer_m1a.may_transmit(m1, now, d))
    {
        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_outer_m1a.transmit_params(m1, now, m_highest_log_entry, &log_entry, &send_func);
        LOG_IF(INFO, s_debug_mode && m1 != m_xmit_outer_m1a.value())
            << logid () << "leading " << ph(m1.b);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_VOTE_1A)
                        + pack_size(m_tg)
                        + pack_size(m1);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_VOTE_1A << m_tg << m1;
        (d->*send_func)(m_highest_log_entry, m_tg.group, msg);
    }

    if (send_m2 && m_xmit_outer_m2a.may_transmit(m2, now, d))
    {
        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_outer_m2a.transmit_params(m2, now, m_highest_log_entry, &log_entry, &send_func);
        LOG_IF(INFO, s_debug_mode && m2 != m_xmit_outer_m2a.value())
            << logid() << "using " << ph(m2.b)
            << " to suggest state machine input "
            << pretty_print_outer(m2.v);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_VOTE_2A)
                        + pack_size(m_tg)
                        + pack_size(m2);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_VOTE_2A << m_tg << m2;
        (d->*send_func)(m_highest_log_entry, m_tg.group, msg);
    }

    if (send_m3 && m_xmit_outer_m2b.may_transmit(m3, now, d))
    {
        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_outer_m2b.transmit_params(m3, now, m_highest_log_entry, &log_entry, &send_func);
        LOG_IF(INFO, s_debug_mode && m3 != m_xmit_outer_m2b.value())
            << logid() << comm_id(m3.acceptor.get())
            << "/this-node accepted state machine input "
            << pretty_print_outer(m3.v);
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_VOTE_2B)
                        + pack_size(m_tg)
                        + pack_size(m3);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_VOTE_2B << m_tg << m3;
        (d->*send_func)(m_highest_log_entry, m_tg.group, msg);
    }

    if (!preconditions_for_global_paxos(d))
    {
        return;
    }

    generalized_paxos::command cv;
    cv.type = member();
    e::packer(&cv.value) << m_local_vote;
    generalized_paxos::command cg;
    cg.type = GLOBAL_VOTER_COMMAND;
    e::packer(&cg.value) << cv;

    if (m_xmit_vote.may_transmit(cg, now, d))
    {
        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_vote.transmit_params(cg, now, m_highest_log_entry, &log_entry, &send_func);
        const paxos_group* group = d->get_config()->get_group(m_tg.group);

        // XXX excessive retransmit
        if (group && group->members_sz > 0)
        {
            for (unsigned i = 0; i < m_dcs_sz; ++i)
            {
                if (m_global_gp.acceptor_seen(abstract_id(m_dcs[i].get()), cv))
                {
                    continue;
                }

                transaction_group tg(m_tg);
                tg.group = m_dcs[i];
                const size_t sz = BUSYBEE_HEADER_SIZE
                                + pack_size(GV_PROPOSE)
                                + pack_size(tg)
                                + pack_size(cg);
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_PROPOSE << tg << cg;
                (d->*send_func)(log_entry, m_dcs[i], msg);
            }
        }
    }

    generalized_paxos::cstruct dc_learned = m_data_center_gp.learned();

    if (dc_learned.commands.size() < m_dc_prev_learned.commands.size())
    {
        LOG(ERROR) << logid() << "outer generalized paxos unlearned state machine's input: "
                   << pretty_print_outer(m_dc_prev_learned)
                   << " truncated to "
                   << pretty_print_outer(dc_learned);
    }

    if (s_debug_mode && dc_learned != m_dc_prev_learned)
    {
        LOG(INFO) << logid() << "learned state machine input "
                  << pretty_print_outer(dc_learned);
        m_dc_prev_learned = dc_learned;
    }

    size_t executed = 0;

    for (size_t i = 0; i < dc_learned.commands.size(); ++i)
    {
        const generalized_paxos::command& c(dc_learned.commands[i]);

        if (std::find(m_global_exec.begin(), m_global_exec.end(), c) != m_global_exec.end())
        {
            continue;
        }

        LOG_IF(INFO, s_debug_mode) << logid() << "executing " << pretty_print_outer(c);
        m_global_exec.push_back(c);
        ++executed;
        generalized_paxos::command inner_c;
        generalized_paxos::message_p1a inner_m1a;
        generalized_paxos::message_p1b inner_m1b;
        generalized_paxos::message_p2a inner_m2a;
        generalized_paxos::message_p2b inner_m2b;
        bool send = false;
        e::unpacker up(c.value);

        switch (static_cast<transition_t>(c.type))
        {
            case GLOBAL_VOTER_COMMAND:
                up = up >> inner_c;

                if (!up.error())
                {
                    m_global_gp.propose(inner_c);
                }
                else
                {
                    LOG(ERROR) << logid() << "invalid command";
                }

                break;
            case GLOBAL_VOTER_MESSAGE_1A:
                up = up >> inner_m1a;

                if (!up.error())
                {
                    m_global_gp.process_p1a(inner_m1a, &send, &inner_m1b);

                    if (send)
                    {
                        send_global(inner_m1b, d);
                    }
                }
                else
                {
                    LOG(ERROR) << logid() << "invalid 1A message";
                }

                break;
            case GLOBAL_VOTER_MESSAGE_1B:
                up = up >> inner_m1b;

                if (!up.error())
                {
                    m_global_gp.process_p1b(inner_m1b);
                }
                else
                {
                    LOG(ERROR) << logid() << "invalid 1B message";
                }

                break;
            case GLOBAL_VOTER_MESSAGE_2A:
                up = up >> inner_m2a;

                if (!up.error())
                {
                    m_global_gp.process_p2a(inner_m2a, &send, &inner_m2b);

                    if (send)
                    {
                        send_global(inner_m2b, d);
                    }
                }
                else
                {
                    LOG(ERROR) << logid() << "invalid 2A message";
                }

                break;
            case GLOBAL_VOTER_MESSAGE_2B:
                up = up >> inner_m2b;

                if (!up.error())
                {
                    m_global_gp.process_p2b(inner_m2b);
                }
                else
                {
                    LOG(ERROR) << logid() << "invalid 2B message";
                }

                break;
            default:
                break;
        }
    }

    bool lead = m_tg.group == m_tg.txid.group;//XXX failure sensitive

    if (executed == 0 && !lead)
    {
        return;
    }

    send_m1 = send_m2 = send_m3 = false;
    m_global_gp.advance(lead,
                        &send_m1, &m1,
                        &send_m2, &m2,
                        &send_m3, &m3);

    if (send_m1)
    {
        send_global(m1, d);
    }

    if (send_m2)
    {
        send_global(m2, d);
    }

    if (send_m3 && tally_votes("acceptor", m3.v) != 0)
    {
        send_global(m3, d);
    }

    if (!m_has_outcome)
    {
        generalized_paxos::cstruct votes = m_global_gp.learned();
        uint64_t outcome = tally_votes("learned", votes);

        if (outcome != 0)
        {
            m_has_outcome = true;
            m_outcome = outcome;
        }
    }
}

void
global_voter :: send_global(const generalized_paxos::message_p1a& m, daemon* d)
{
    const uint64_t now = po6::monotonic_time();

    if (m_xmit_inner_m1a.may_transmit(m, now, d))
    {
        generalized_paxos::command c;
        c.type = GLOBAL_VOTER_MESSAGE_1A;
        e::packer(&c.value) << m;

        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_inner_m1a.transmit_params(m, now, m_highest_log_entry, &log_entry, &send_func);
        propose_global(c, log_entry, d, send_func);
    }
}

void
global_voter :: send_global(const generalized_paxos::message_p1b& m, daemon* d)
{
    const uint64_t now = po6::monotonic_time();

    if (m_xmit_inner_m1b.may_transmit(m, now, d))
    {
        generalized_paxos::command c;
        c.type = GLOBAL_VOTER_MESSAGE_1B;
        e::packer(&c.value) << m;

        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_inner_m1b.transmit_params(m, now, m_highest_log_entry, &log_entry, &send_func);
        propose_global(c, log_entry, d, send_func);
    }
}

void
global_voter :: send_global(const generalized_paxos::message_p2a& m, daemon* d)
{
    const uint64_t now = po6::monotonic_time();

    if (m_xmit_inner_m2a.may_transmit(m, now, d))
    {
        generalized_paxos::command c;
        c.type = GLOBAL_VOTER_MESSAGE_2A;
        e::packer(&c.value) << m;

        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_inner_m2a.transmit_params(m, now, m_highest_log_entry, &log_entry, &send_func);
        propose_global(c, log_entry, d, send_func);
    }
}

void
global_voter :: send_global(const generalized_paxos::message_p2b& m, daemon* d)
{
    const uint64_t now = po6::monotonic_time();

    if (m_xmit_inner_m2b.may_transmit(m, now, d))
    {
        generalized_paxos::command c;
        c.type = GLOBAL_VOTER_MESSAGE_2B;
        e::packer(&c.value) << m;

        uint64_t log_entry;
        void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>);
        m_xmit_inner_m2b.transmit_params(m, now, m_highest_log_entry, &log_entry, &send_func);
        propose_global(c, log_entry, d, send_func);
    }
}

void
global_voter :: propose_global(const generalized_paxos::command& c, uint64_t log_entry,
                               daemon* d, void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>))
{
    const paxos_group* group = d->get_config()->get_group(m_tg.group);

    // XXX excessive retransmit
    if (!group || group->members_sz == 0)
    {
        return;
    }

    for (unsigned i = 0; i < m_dcs_sz; ++i)
    {
        transaction_group tg(m_tg);
        tg.group = m_dcs[i];
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(GV_PROPOSE)
                        + pack_size(tg)
                        + pack_size(c);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << GV_PROPOSE << tg << c;
        (d->*send_func)(log_entry, m_dcs[i], msg);
    }
}

uint64_t
global_voter :: tally_votes(const char* prefix, const generalized_paxos::cstruct& votes)
{
    unsigned voted = 0;
    unsigned aborted = 0;
    unsigned committed = 0;
    bool seen[CONSUS_MAX_REPLICATION_FACTOR];

    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        seen[i] = false;
    }

    for (size_t i = 0; i < votes.commands.size(); ++i)
    {
        const generalized_paxos::command& c(votes.commands[i]);

        if (c.type >= 2 * CONSUS_MAX_REPLICATION_FACTOR)
        {
            LOG(ERROR) << XXX() << prefix << " invalid command: " << c;
        }
        else if (c.type >= CONSUS_MAX_REPLICATION_FACTOR)
        {
            // XXX re-cast vote
            LOG(ERROR) << XXX() << prefix << " XXX RE-CAST VOTE";
        }
        else
        {
            if (!seen[c.type])
            {
                uint64_t v = 0;
                e::unpacker up(c.value);
                up = up >> v;

                if (up.error())
                {
                    LOG(ERROR) << XXX() << prefix << " corrupt vote: " << c;
                }
                else if (v == CONSUS_VOTE_COMMIT)
                {
                    ++voted;
                    ++committed;
                    seen[c.type] = true;
                }
                else if (v == CONSUS_VOTE_ABORT)
                {
                    ++voted;
                    ++aborted;
                    seen[c.type] = true;
                }
                else
                {
                    LOG(ERROR) << XXX() << prefix << " invalid vote: " << c;
                }
            }
        }
    }

    if (committed >= m_dcs_sz / 2 + 1)
    {
        return CONSUS_VOTE_COMMIT;
    }

    // XXX what aobut an even split here?
    if (aborted >= m_dcs_sz / 2 + 1)
    {
        return CONSUS_VOTE_ABORT;
    }

    return 0;
}
