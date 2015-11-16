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
        global_voter(const transaction_group& tg);
        ~global_voter() throw ();

    public:
        const transaction_group& state_key() const;
        bool finished();

    public:
        void set_preferred_vote(uint64_t v);
        void init(paxos_group_id* us, const paxos_group_id* dcs, size_t dcs_sz, daemon* d);
#if 0
        void vote_1a(comm_id id, unsigned idx, const paxos_synod::ballot& b, daemon* d);
        void vote_1b(comm_id id, unsigned idx,
                     const paxos_synod::ballot& b,
                     const paxos_synod::pvalue& p,
                     daemon* d);
        void vote_2a(comm_id id, unsigned idx, const paxos_synod::pvalue& p, daemon* d);
        void vote_2b(comm_id id, unsigned idx, const paxos_synod::pvalue& p, daemon* d);
        void vote_learn(unsigned idx, uint64_t v, daemon* d);
#endif
        void externally_work_state_machine(daemon* d);
        bool outcome(uint64_t* v);

    private:
        bool preconditions(daemon* d);
        void work_state_machine(daemon* d);

    private:
        const transaction_group m_tg;
        po6::threads::mutex m_mtx;
        bool m_initialized;
        paxos_group_id m_us;
        paxos_group_id m_dcs[CONSUS_MAX_REPLICATION_FACTOR];
        uint64_t m_dcs_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
        size_t m_dcs_sz;
        generalized_paxos m_data_center_gp;
        generalized_paxos m_global_gp;
#if 0
        paxos_group m_group;
        paxos_synod m_votes[CONSUS_MAX_REPLICATION_FACTOR];
        paxos_synod::phase_t m_phases[CONSUS_MAX_REPLICATION_FACTOR];
        uint64_t m_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
        bool m_has_preferred_vote;
        uint64_t m_preferred_vote;
        bool m_has_outcome;
        uint64_t m_outcome;
        bool m_outcome_in_dispositions;
#endif

    private:
        global_voter(const global_voter&);
        global_voter& operator = (const global_voter&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_global_voter_h_
