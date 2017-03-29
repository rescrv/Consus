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

#ifndef consus_txman_global_voter_h_
#define consus_txman_global_voter_h_

// STL
#include <algorithm>

// po6
#include <po6/threads/mutex.h>

// consus
#include "namespace.h"
#include "common/transmit_limiter.h"
#include "common/transaction_group.h"
#include "txman/generalized_paxos.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class global_voter
{
    public:
        enum transition_t
        {
            GLOBAL_VOTER_COMMAND = 1,
            GLOBAL_VOTER_MESSAGE_1A = 2,
            GLOBAL_VOTER_MESSAGE_1B = 3,
            GLOBAL_VOTER_MESSAGE_2A = 4,
            GLOBAL_VOTER_MESSAGE_2B = 5
        };

    public:
        global_voter(const transaction_group& tg);
        ~global_voter() throw ();

    public:
        const transaction_group& state_key() const;
        bool finished();

    public:
        bool initialized();
        void init(uint64_t vote, const paxos_group_id* dcs, size_t dcs_sz, daemon* d);
        bool propose(const generalized_paxos::command& c, daemon* d);
        bool process_p1a(comm_id id, const generalized_paxos::message_p1a& m, daemon* d);
        bool process_p1b(const generalized_paxos::message_p1b& m, daemon* d);
        bool process_p2a(comm_id id, const generalized_paxos::message_p2a& m, daemon* d);
        bool process_p2b(const generalized_paxos::message_p2b& m, daemon* d);
        void wound(daemon*) {}
        void externally_work_state_machine(daemon* d);
        bool outcome(uint64_t* v);
        void unvoted_data_centers(paxos_group_id* dcs, size_t* dcs_sz);
        std::string debug_dump();
        std::string logid();

    private:
        struct data_center_comparator;
        struct global_comparator;
        std::string XXX() { return logid() + " XXX: "; }
        std::string pretty_print_outer(const generalized_paxos::cstruct& c) { return pretty_print_outer_cstruct(c); }
        std::string pretty_print_outer(const generalized_paxos::command& c) { return pretty_print_outer_command(c); }
        std::string pretty_print_inner(const generalized_paxos::cstruct& c) { return pretty_print_inner_cstruct(c); }
        std::string pretty_print_inner(const generalized_paxos::command& c) { return pretty_print_inner_command(c); }
        std::string pretty_print_outer_cstruct(const generalized_paxos::cstruct& c);
        std::string pretty_print_outer_command(const generalized_paxos::command& c);
        std::string pretty_print_inner_cstruct(const generalized_paxos::cstruct& c);
        std::string pretty_print_inner_command(const generalized_paxos::command& c);
        bool preconditions_for_data_center_paxos(daemon* d);
        bool preconditions_for_global_paxos(daemon* d);
        unsigned member() const { return std::find(m_dcs, m_dcs + m_dcs_sz, m_tg.group) - m_dcs; }
        void work_state_machine(daemon* d);
        void send_global(const generalized_paxos::message_p1a& m, daemon* d);
        void send_global(const generalized_paxos::message_p1b& m, daemon* d);
        void send_global(const generalized_paxos::message_p2a& m, daemon* d);
        void send_global(const generalized_paxos::message_p2b& m, daemon* d);
        void propose_global(const generalized_paxos::command& c, uint64_t log_entry,
                            daemon* d, void (daemon::*send_func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>));
        uint64_t tally_votes(const char* prefix, const generalized_paxos::cstruct& v);

    private:
        const transaction_group m_tg;
        po6::threads::mutex m_mtx;
        bool m_data_center_init;
        bool m_global_init;
        bool m_outcome_in_dispositions;
        // data center paxos
        const std::auto_ptr<data_center_comparator> m_data_center_cmp;
        generalized_paxos m_data_center_gp;
        int64_t m_highest_log_entry;
        generalized_paxos::cstruct m_dc_prev_learned;
        // data center paxos: rate limiting
        transmit_limiter<generalized_paxos::command, daemon> m_xmit_vote;
        transmit_limiter<generalized_paxos::message_p1a, daemon> m_xmit_outer_m1a;
        transmit_limiter<generalized_paxos::message_p2a, daemon> m_xmit_outer_m2a;
        transmit_limiter<generalized_paxos::message_p2b, daemon> m_xmit_outer_m2b;
        transmit_limiter<generalized_paxos::message_p1a, daemon> m_xmit_inner_m1a;
        transmit_limiter<generalized_paxos::message_p1b, daemon> m_xmit_inner_m1b;
        transmit_limiter<generalized_paxos::message_p2a, daemon> m_xmit_inner_m2a;
        transmit_limiter<generalized_paxos::message_p2b, daemon> m_xmit_inner_m2b;
        // global paxos
        uint64_t m_local_vote;
        paxos_group_id m_dcs[CONSUS_MAX_REPLICATION_FACTOR];
        uint64_t m_dcs_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
        size_t m_dcs_sz;
        const std::auto_ptr<global_comparator> m_global_cmp;
        generalized_paxos m_global_gp;
        std::vector<generalized_paxos::command> m_global_exec;
        // outcome
        bool m_has_outcome;
        uint64_t m_outcome;

    private:
        global_voter(const global_voter&);
        global_voter& operator = (const global_voter&);
};

e::packer
operator << (e::packer pa, const global_voter::transition_t& rhs);
e::unpacker
operator >> (e::unpacker up, global_voter::transition_t& rhs);
size_t
pack_size(const global_voter::transition_t& txid);

END_CONSUS_NAMESPACE

#endif // consus_txman_global_voter_h_
