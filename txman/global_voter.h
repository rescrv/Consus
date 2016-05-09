// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_txman_global_voter_h_
#define consus_txman_global_voter_h_

// po6
#include <po6/threads/mutex.h>

// consus
#include "namespace.h"
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
        void externally_work_state_machine(daemon* d);
        bool outcome(uint64_t* v);

    private:
        struct data_center_comparator;
        struct global_comparator;
        std::string logid();
        std::string pretty_print_outer(const generalized_paxos::cstruct& c);
        std::string pretty_print_outer(const generalized_paxos::command& c);
        std::string pretty_print_inner(const generalized_paxos::cstruct& c);
        std::string pretty_print_inner(const generalized_paxos::command& c);
        bool preconditions_for_data_center_paxos(daemon* d);
        bool preconditions_for_global_paxos(daemon* d);
        unsigned member() const { return std::find(m_dcs, m_dcs + m_dcs_sz, m_tg.group) - m_dcs; }
        void work_state_machine(daemon* d);
        void send_global(const generalized_paxos::message_p1a& m, daemon* d);
        void send_global(const generalized_paxos::message_p1b& m, daemon* d);
        void send_global(const generalized_paxos::message_p2a& m, daemon* d);
        void send_global(const generalized_paxos::message_p2b& m, daemon* d);
        bool propose_global(const generalized_paxos::command& c, daemon* d);
        uint64_t tally_votes(const generalized_paxos::cstruct& v);

    private:
        const transaction_group m_tg;
        po6::threads::mutex m_mtx;
        bool m_data_center_init;
        bool m_global_init;
        bool m_outcome_in_dispositions;
        // data center paxos
        const std::auto_ptr<data_center_comparator> m_data_center_cmp;
        generalized_paxos m_data_center_gp;
        // data center paxos: rate limiting
        uint64_t m_rate_vote_timestamp;
        generalized_paxos::message_p1a m_outer_rate_m1a;
        uint64_t m_outer_rate_m1a_timestamp;
        generalized_paxos::message_p1b m_outer_rate_m1b;
        uint64_t m_outer_rate_m1b_timestamp;
        generalized_paxos::message_p2a m_outer_rate_m2a;
        uint64_t m_outer_rate_m2a_timestamp;
        generalized_paxos::message_p2b m_outer_rate_m2b;
        uint64_t m_outer_rate_m2b_timestamp;
        generalized_paxos::message_p1a m_inner_rate_m1a;
        uint64_t m_inner_rate_m1a_timestamp;
        generalized_paxos::message_p1b m_inner_rate_m1b;
        uint64_t m_inner_rate_m1b_timestamp;
        generalized_paxos::message_p2a m_inner_rate_m2a;
        uint64_t m_inner_rate_m2a_timestamp;
        generalized_paxos::message_p2b m_inner_rate_m2b;
        uint64_t m_inner_rate_m2b_timestamp;
        // global paxos
        uint64_t m_local_vote;
        paxos_group_id m_dcs[CONSUS_MAX_REPLICATION_FACTOR];
        uint64_t m_dcs_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
        size_t m_dcs_sz;
        const std::auto_ptr<global_comparator> m_global_cmp;
        generalized_paxos m_global_gp;
        std::set<generalized_paxos::command> m_global_exec;
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
