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
        void set_preferred_vote(uint64_t v, daemon* d);
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
        uint64_t outcome();
        std::string debug_dump();
        std::string logid();

    private:
        std::string votes();
        bool preconditions_for_paxos(daemon* d);
        void work_state_machine(daemon* d);
        void work_paxos_vote(unsigned idx, daemon* d);

    private:
        const transaction_group m_tg;
        po6::threads::mutex m_mtx;
        bool m_initialized;
        paxos_group m_group;
        paxos_synod m_votes[CONSUS_MAX_REPLICATION_FACTOR];
        transmit_limiter<paxos_synod::ballot, daemon> m_xmit_p1a;
        transmit_limiter<paxos_synod::pvalue, daemon> m_xmit_p2a;
        transmit_limiter<uint64_t, daemon> m_xmit_learn;
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
