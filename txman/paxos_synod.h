// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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
        void init(comm_id us, const paxos_group& pg);
        phase_t phase();
        void phase1(ballot* b);
        void phase1a(const ballot& b, ballot* a, pvalue* p);
        void phase1b(comm_id m, const ballot& a, const pvalue& p);
        void phase2(pvalue* p, uint64_t preferred_value);
        void phase2a(const pvalue& p, bool* a);
        void phase2b(comm_id m, const pvalue& p);
        void force_learn(uint64_t value);
        uint64_t learned();

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
