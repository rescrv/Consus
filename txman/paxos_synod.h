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

#ifndef consus_txman_paxos_synod_h_
#define consus_txman_paxos_synod_h_

// consus
#include "namespace.h"
#include "common/paxos_group.h"

BEGIN_CONSUS_NAMESPACE

class paxos_synod
{
    public:
        enum phase_t
        {
            PHASE1,
            PHASE2,
            LEARNED
        };

        struct ballot
        {
            ballot();
            ballot(uint64_t number, comm_id leader);
            ballot(const ballot& other);
            ~ballot() throw ();
            int compare(const ballot& rhs) const;
            ballot& operator = (const ballot& rhs);
            bool operator < (const ballot& rhs) const;
            bool operator <= (const ballot& rhs) const;
            bool operator == (const ballot& rhs) const;
            bool operator != (const ballot& rhs) const;
            bool operator >= (const ballot& rhs) const;
            bool operator > (const ballot& rhs) const;

            uint64_t number;
            comm_id leader;
        };
        struct pvalue
        {
            pvalue();
            pvalue(const ballot& b, uint64_t v);
            pvalue(const pvalue& other);
            ~pvalue() throw ();
            int compare(const pvalue& rhs) const;
            pvalue& operator = (const pvalue& rhs);
            bool operator < (const pvalue& rhs) const;
            bool operator <= (const pvalue& rhs) const;
            bool operator == (const pvalue& rhs) const;
            bool operator != (const pvalue& rhs) const;
            bool operator >= (const pvalue& rhs) const;
            bool operator > (const pvalue& rhs) const;

            ballot b;
            uint64_t v;
        };

    public:
        paxos_synod();
        ~paxos_synod() throw ();

    public:
        void init(comm_id us, const paxos_group& pg, comm_id leader);
        void propose(uint64_t value);
        void advance(bool* send_p1a, ballot* p1a,
                     bool* send_p2a, pvalue* p2a,
                     bool* send_learn, uint64_t* learned);
        phase_t phase();
        void phase1(ballot* b);
        void phase1a(const ballot& b, ballot* a, pvalue* p);
        void phase1b(comm_id m, const ballot& a, const pvalue& p);
        void phase2(pvalue* p, uint64_t preferred_value);
        void phase2a(const pvalue& p, bool* a);
        void phase2b(comm_id m, const pvalue& p);
        void force_learn(uint64_t value);
        uint64_t learned();
        std::string debug_dump();

    private:
        struct promise
        {
            promise();
            ~promise() throw ();

            ballot current_ballot;
            pvalue current_pvalue;

            private:
                promise(const promise&);
                promise& operator = (const promise&);
        };
        void set_phase();

    private:
        bool m_init;
        comm_id m_us;
        paxos_group m_group;

        uint64_t m_proposed;

        ballot m_acceptor_ballot;
        pvalue m_acceptor_pvalue;

        phase_t m_leader_phase;
        ballot m_leader_ballot;
        pvalue m_leader_pvalue;

        promise m_promises[CONSUS_MAX_REPLICATION_FACTOR];

        uint64_t m_value;

    private:
        paxos_synod(const paxos_synod&);
        paxos_synod& operator = (const paxos_synod&);
};

std::ostream&
operator << (std::ostream& out, const paxos_synod::ballot& b);
std::ostream&
operator << (std::ostream& out, const paxos_synod::pvalue& p);

e::packer
operator << (e::packer pa, const paxos_synod::ballot& rhs);
e::unpacker
operator >> (e::unpacker up, paxos_synod::ballot& rhs);
size_t
pack_size(const paxos_synod::ballot& b);

e::packer
operator << (e::packer pa, const paxos_synod::pvalue& rhs);
e::unpacker
operator >> (e::unpacker up, paxos_synod::pvalue& rhs);
size_t
pack_size(const paxos_synod::pvalue& p);

END_CONSUS_NAMESPACE

#endif // consus_txman_paxos_synod_h_
