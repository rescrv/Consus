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

#ifndef consus_txman_generalized_paxos_h_
#define consus_txman_generalized_paxos_h_

// STL
#include <set>
#include <string>

// consus
#include "namespace.h"
#include "common/macros.h"
#include "common/paxos_group.h"

#ifndef GENERALIZED_PAXOS_DEBUG
#define GENERALIZED_PAXOS_DEBUG 1
#endif

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
            virtual ~comparator() throw ();
            virtual bool conflict(const command& a, const command& b) const = 0;
        };
        struct cstruct
        {
            cstruct();
            ~cstruct() throw ();
            bool operator == (const cstruct& rhs) const;
            bool operator != (const cstruct& rhs) const { return !(*this == rhs); }

            bool is_none() { return commands.empty(); }
            std::vector<command> commands;
        };
        struct ballot
        {
            enum type_t
            {
                CLASSIC = 1,
                FAST = 2
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
            bool operator == (const message_p1a& rhs) const;
            bool operator != (const message_p1a& rhs) const
            { return !(*this == rhs); }

            ballot b; // called "m" in the paper
        };
        struct message_p1b
        {
            message_p1b();
            message_p1b(const ballot& b, abstract_id acceptor, const ballot& vb, const cstruct& v);
            ~message_p1b() throw ();
            bool operator == (const message_p1b& rhs) const;
            bool operator != (const message_p1b& rhs) const
            { return !(*this == rhs); }

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
            bool operator == (const message_p2a& rhs) const;
            bool operator != (const message_p2a& rhs) const
            { return !(*this == rhs); }

            ballot b; // called "m" in the paper
            cstruct v; // called "maxTried[m]•C" or "v" in the paper
        };
        struct message_p2b
        {
            message_p2b();
            message_p2b(const ballot& b, abstract_id acceptor, const cstruct& v);
            ~message_p2b() throw ();
            bool operator == (const message_p2b& rhs) const;
            bool operator != (const message_p2b& rhs) const
            { return !(*this == rhs); }

            ballot b; // called "m" in the paper
            abstract_id acceptor; // called "a" in the paper
            cstruct v; // called "bA_a[m]•C" or "v" in the paper
        };

    public:
        generalized_paxos();
        ~generalized_paxos() throw ();

    public:
        void init(const comparator* cmp, abstract_id us, const abstract_id* acceptors, size_t acceptors_sz);
        void default_leader(abstract_id leader, ballot::type_t t);

        bool propose(const command& c);
        bool propose_from_p2b(const message_p2b& m);
        void advance(bool may_attempt_leadership,
                     bool* send_m1, message_p1a* m1,
                     bool* send_m2, message_p2a* m2,
                     bool* send_m3, message_p2b* m3);

        // react to messages
        ballot acceptor_ballot() const { return m_acceptor_ballot; }
        cstruct accepted_value() const { return m_acceptor_value; }
        void process_p1a(const message_p1a& m, bool* send, message_p1b* r);
        bool process_p1b(const message_p1b& m);
        void process_p2a(const message_p2a& m, bool* send, message_p2b* r);
        bool process_p2b(const message_p2b& m);

        // what has been cumulatively learned throughout the system, from the
        // limited amount that this instance can observe
        cstruct learned();

        // used to decide retransmits/etc
        void all_accepted_commands(std::vector<command>* commands);

        std::string debug_dump(e::compat::function<std::string(cstruct)> pcst, e::compat::function<std::string(command)> pcmd);

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
        bool cstruct_lt(const cstruct& lhs, const cstruct& rhs);
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
        std::set<command> m_proposed;

        ballot m_acceptor_ballot;
        cstruct m_acceptor_value;
        ballot m_acceptor_value_src;

        ballot m_leader_ballot;
        cstruct m_leader_value;
        std::vector<message_p1b> m_promises;

        std::vector<message_p2b> m_accepted;
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
std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::message_p1a& m1a);
std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::message_p1b& m1b);
std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::message_p2a& m2a);
std::ostream&
operator << (std::ostream& lhs, const generalized_paxos::message_p2b& m2b);

e::packer
operator << (e::packer pa, const generalized_paxos::command& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::command& rhs);
size_t
pack_size(const generalized_paxos::command& c);

e::packer
operator << (e::packer pa, const generalized_paxos::cstruct& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::cstruct& rhs);
size_t
pack_size(const generalized_paxos::cstruct& cs);

e::packer
operator << (e::packer pa, const generalized_paxos::ballot& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::ballot& rhs);
size_t
pack_size(const generalized_paxos::ballot& b);

e::packer
operator << (e::packer pa, const generalized_paxos::message_p1a& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::message_p1a& rhs);
size_t
pack_size(const generalized_paxos::message_p1a& m);

e::packer
operator << (e::packer pa, const generalized_paxos::message_p1b& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::message_p1b& rhs);
size_t
pack_size(const generalized_paxos::message_p1b& m);

e::packer
operator << (e::packer pa, const generalized_paxos::message_p2a& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::message_p2a& rhs);
size_t
pack_size(const generalized_paxos::message_p2a& m);

e::packer
operator << (e::packer pa, const generalized_paxos::message_p2b& rhs);
e::unpacker
operator >> (e::unpacker up, generalized_paxos::message_p2b& rhs);
size_t
pack_size(const generalized_paxos::message_p2b& m);

END_CONSUS_NAMESPACE

#endif // consus_txman_generalized_paxos_h_
