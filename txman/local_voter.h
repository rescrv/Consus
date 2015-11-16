// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_txman_local_voter_h_
#define consus_txman_local_voter_h_

// po6
#include <po6/threads/mutex.h>

// consus
#include "namespace.h"
#include "common/transaction_group.h"
#include "txman/paxos_synod.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class local_voter
{
    public:
        local_voter(const transaction_group& tg);
        ~local_voter() throw ();

    public:
        const transaction_group& state_key() const;
        bool finished();

    public:
        void set_preferred_vote(uint64_t v);
        void vote_1a(comm_id id, unsigned idx, const paxos_synod::ballot& b, daemon* d);
        void vote_1b(comm_id id, unsigned idx,
                     const paxos_synod::ballot& b,
                     const paxos_synod::pvalue& p,
                     daemon* d);
        void vote_2a(comm_id id, unsigned idx, const paxos_synod::pvalue& p, daemon* d);
        void vote_2b(comm_id id, unsigned idx, const paxos_synod::pvalue& p, daemon* d);
        void vote_learn(unsigned idx, uint64_t v, daemon* d);
        void externally_work_state_machine(daemon* d);
        bool outcome(uint64_t* v);

    private:
        bool preconditions_for_paxos(daemon* d);
        void work_state_machine(daemon* d);
        void work_paxos_vote(unsigned idx, daemon* d, uint64_t preferred);

    private:
        const transaction_group m_tg;
        po6::threads::mutex m_mtx;
        bool m_initialized;
        paxos_group m_group;
        paxos_synod m_votes[CONSUS_MAX_REPLICATION_FACTOR];
        paxos_synod::phase_t m_phases[CONSUS_MAX_REPLICATION_FACTOR];
        uint64_t m_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
        bool m_has_preferred_vote;
        uint64_t m_preferred_vote;
        bool m_has_outcome;
        uint64_t m_outcome;
        bool m_outcome_in_dispositions;

    private:
        local_voter(const local_voter&);
        local_voter& operator = (const local_voter&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_local_voter_h_
