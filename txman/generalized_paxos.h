// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_txman_generalized_paxos_h_
#define consus_txman_generalized_paxos_h_

// STL
#include <set>
#include <string>

// consus
#include "namespace.h"
#include "common/paxos_group.h"

BEGIN_CONSUS_NAMESPACE

class generalized_paxos
{
    public:
        struct command
        {
            command();
            command(uint16_t tag, const std::string& value);
            command(const command& other);
            ~command() throw ();
            int compare(const command& rhs) const;
            command& operator = (const command& rhs);
            bool operator < (const command& rhs) const;
            bool operator <= (const command& rhs) const;
            bool operator == (const command& rhs) const;
            bool operator != (const command& rhs) const;
            bool operator >= (const command& rhs) const;
            bool operator > (const command& rhs) const;

            uint16_t type;
            std::string value;
        };
        struct comparator
        {
            comparator();
            virtual bool conflict(const command& a, const command& b) const = 0;
            protected:
                ~comparator() throw ();
        };
        struct cstruct
        {
            cstruct();
            ~cstruct() throw ();
            bool operator == (const cstruct& rhs) const;

            std::vector<command> commands;
        };
        struct ballot
        {
            enum type_t
            {
                CLASSIC,
                FAST
            };

            ballot();
            ballot(type_t type, uint64_t number, abstract_id leader);
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

            type_t type;
            uint64_t number;
            abstract_id leader;
        };
        struct message_p1a
        {
            message_p1a();
            message_p1a(const ballot& b);
            ~message_p1a() throw ();

            ballot b; // called "m" in the paper
        };
        struct message_p1b
        {
            message_p1b();
            message_p1b(const ballot& b, abstract_id acceptor, const ballot& vb, const cstruct& v);
            ~message_p1b() throw ();

            ballot b; // called "m" in the paper
            abstract_id acceptor; // called "a" in the paper
            ballot vb; // called "bA_a" in the paper; vbal in the TLA+
            cstruct v; // called "bA_a" in the paper; vote in the TLA+
        };
        struct message_p2a
        {
            message_p2a();
            message_p2a(const ballot& b, const cstruct& v);
            ~message_p2a() throw ();

            ballot b; // called "m" in the paper
            cstruct v; // called "maxTried[m]•C" or "v" in the paper
        };
        struct message_p2b
        {
            message_p2b();
            message_p2b(const ballot& b, abstract_id acceptor, const cstruct& v);
            ~message_p2b() throw ();

            ballot b; // called "m" in the paper
            abstract_id acceptor; // called "a" in the paper
            cstruct v; // called "bA_a[m]•C" or "v" in the paper
        };

    public:
        generalized_paxos();
        ~generalized_paxos() throw ();

    public:
        void init(const comparator* cmp, abstract_id us, const abstract_id* acceptors, size_t acceptors_sz);

        void propose(const command& c);
        void propose_from_p2b(const message_p2b& m);
        void advance(bool may_attempt_leadership,
                     bool* send_m1, message_p1a* m1,
                     bool* send_m2, message_p2a* m2,
                     bool* send_m3, message_p2b* m3);

        // react to messages
        void process_p1a(const message_p1a& m, bool* send, message_p1b* r);
        void process_p1b(const message_p1b& m);
        void process_p2a(const message_p2a& m, bool* send, message_p2b* r);
        void process_p2b(const message_p2b& m);

        // what has been cumulatively learned throughout the system, from the
        // limited amount that this instance can observe
        cstruct learned();

    private:
        enum state_t
        {
            PARTICIPATING,
            LEADING_PHASE1,
            LEADING_PHASE2
        };

    private:
        size_t index_of(abstract_id a);
        size_t quorum();
        void learned(cstruct* l, bool* conflict);
        void learned(const ballot& b, std::vector<cstruct>* lv, bool* conflict);
        void learned(cstruct** vs, size_t vs_sz, size_t max_sz,
                     std::vector<cstruct>* lv, bool* conflict);
        void learned(cstruct** vs, size_t vs_sz, cstruct* v, bool* conflict);
        cstruct proven_safe();

        // cstructs are command histories as described in the paper,
        // not sequences
        typedef std::set<std::pair<command, command> > partial_order_t;
        bool cstruct_le(const cstruct& lhs, const cstruct& rhs);
        bool cstruct_eq(const cstruct& lhs, const cstruct& rhs);
        bool cstruct_compatible(const cstruct& lhs, const cstruct& rhs);
        cstruct cstruct_glb(const cstruct& lhs, const cstruct& rhs, bool* conflict);
        cstruct cstruct_lub(const cstruct& lhs, const cstruct& rhs);

        // from the cstruct, returns a sorted list of commands, a list of edges
        // defined according to m_interfere
        //
        // will not clear commands or order
        void cstruct_pieces(const cstruct& c,
                            std::vector<command>* commands,
                            partial_order_t* order);

        // Graph operations on the partial order over commands
        static bool directed_path_exists(const command& from,
                                         const command& to,
                                         const partial_order_t& edge_list);
        static bool directed_path_exists(const command& from,
                                         const command& to,
                                         const partial_order_t& edge_list,
                                         std::set<command>* seen);

    private:
        bool m_init;
        const comparator* m_interfere;
        state_t m_state;
        abstract_id m_us;
        std::vector<abstract_id> m_acceptors;
        std::vector<command> m_proposed;

        ballot m_acceptor_ballot;
        cstruct m_acceptor_value;
        ballot m_acceptor_value_src;

        ballot m_leader_ballot;
        cstruct m_leader_value;
        std::vector<message_p1b> m_promises;

        std::vector<message_p2b> m_learned;
        cstruct m_learned_cached;

    private:
        generalized_paxos(const generalized_paxos&);
        generalized_paxos& operator = (const generalized_paxos&);
};

std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::command& rhs);
std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::cstruct& rhs);
std::ostream&
operator << (std::ostream& out, const generalized_paxos::ballot& b);
std::ostream&
operator << (std::ostream& out, const generalized_paxos::ballot::type_t& t);

END_CONSUS_NAMESPACE

#endif // consus_txman_generalized_paxos_h_
