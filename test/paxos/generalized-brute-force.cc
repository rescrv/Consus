// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#define __STDC_LIMIT_MACROS

// STL
#include <algorithm>
#include <queue>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/popt.h>
#include <e/serialization.h>

// consus
#include "common/ids.h"
#include "txman/generalized_paxos.h"

using namespace consus;

class state_machine
{
    public:
        state_machine(unsigned num_lists);
        ~state_machine() throw ();

    public:
        void execute(const generalized_paxos::command* cmds, size_t cmds_sz);
        uint64_t complete() { return e::atomic::increment_64_nobarrier(&m_elements, 0); }
        const std::vector<uint64_t>& list(unsigned idx) { assert(idx < m_lists_sz); return m_lists[idx]; }

    protected:
        void execute(const generalized_paxos::command& cmd);

    private:
        std::set<generalized_paxos::command> m_executed;
        std::vector<uint64_t>* const m_lists;
        const size_t m_lists_sz;
        uint64_t m_elements;

    private:
        state_machine(const state_machine&);
        state_machine& operator = (const state_machine&);
};

state_machine :: state_machine(unsigned num_lists)
    : m_executed()
    , m_lists(new std::vector<uint64_t>[num_lists])
    , m_lists_sz(num_lists)
    , m_elements(0)
{
}

state_machine :: ~state_machine() throw ()
{
    delete[] m_lists;
}

void
state_machine :: execute(const generalized_paxos::command* cmds, size_t cmds_sz)
{
    for (size_t i = 0; i < cmds_sz; ++i)
    {
        const generalized_paxos::command& c(cmds[i]);

        if (m_executed.find(c) == m_executed.end())
        {
            this->execute(c);
            m_executed.insert(c);
        }
    }
}

void
state_machine :: execute(const generalized_paxos::command& cmd)
{
    assert(cmd.value.size() == sizeof(uint64_t));
    uint64_t v = 0;
    e::unpacker up(cmd.value);
    up = up >> v;

    if (up.error())
    {
        abort();
    }

    assert(cmd.type < m_lists_sz);
    m_lists[cmd.type].push_back(v);
    e::atomic::increment_64_nobarrier(&m_elements, 1);
}

struct message
{
    message()
        : has_c(false), c()
        , has_p1a(false), p1a()
        , has_p1b(false), p1b()
        , has_p2a(false), p2a()
        , has_p2b(false), p2b()
    {
    }

    bool has_c;
    generalized_paxos::command c;
    bool has_p1a;
    generalized_paxos::message_p1a p1a;
    bool has_p1b;
    generalized_paxos::message_p1b p1b;
    bool has_p2a;
    generalized_paxos::message_p2a p2a;
    bool has_p2b;
    generalized_paxos::message_p2b p2b;
};

class generalized_paxos_state_machine
{
    public:
        generalized_paxos_state_machine(state_machine** sm, unsigned acceptors);
        ~generalized_paxos_state_machine() throw ();

    public:
        void propose(const generalized_paxos::command& cmd);
        void wait() { po6::threads::mutex::hold hold(&m_mtx); m_cond.wait(); }
        void finish();

    private:
        void acceptor(state_machine* sm, abstract_id id);
        void send_to_all(const generalized_paxos::message_p1a& m);
        void send_to_all(const generalized_paxos::message_p1b& m);
        void send_to_all(const generalized_paxos::message_p2a& m);
        void send_to_all(const generalized_paxos::message_p2b& m);
        void send_to_all(const message& m);

    private:
        unsigned m_num_acceptors;
        e::compat::shared_ptr<po6::threads::thread>* m_acceptors;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_done;
        unsigned short m_randbuf[3];
        std::queue<message>* m_msgs;

    private:
        generalized_paxos_state_machine(const generalized_paxos_state_machine&);
        generalized_paxos_state_machine& operator = (const generalized_paxos_state_machine&);
};

struct comparator : public generalized_paxos::comparator
{
    comparator() {}
    virtual ~comparator() throw () {}
    virtual bool conflict(const generalized_paxos::command& a,
                          const generalized_paxos::command& b) const { return a.type == b.type; }
};

comparator cmp;

generalized_paxos_state_machine :: generalized_paxos_state_machine(state_machine** sm, unsigned acceptors)
    : m_num_acceptors(acceptors)
    , m_acceptors(NULL)
    , m_mtx()
    , m_cond(&m_mtx)
    , m_done(false)
    , m_msgs(NULL)
{
    m_acceptors = new e::compat::shared_ptr<po6::threads::thread>[acceptors];
    m_msgs = new std::queue<message>[acceptors];
    memset(m_randbuf, 0, sizeof(m_randbuf));

    for (unsigned i = 0; i < acceptors; ++i)
    {
        m_acceptors[i].reset(new po6::threads::thread(po6::threads::make_thread_wrapper(&generalized_paxos_state_machine::acceptor, this, sm[i], abstract_id(i + 1))));
        m_acceptors[i]->start();
    }
}

generalized_paxos_state_machine :: ~generalized_paxos_state_machine() throw ()
{
    {
        po6::threads::mutex::hold hold(&m_mtx);
        assert(m_done);
    }

    delete[] m_acceptors;
    delete[] m_msgs;
}

void
generalized_paxos_state_machine :: propose(const generalized_paxos::command& cmd)
{
    po6::threads::mutex::hold hold(&m_mtx);
    uint64_t idx = nrand48(m_randbuf) % m_num_acceptors;
    assert(idx < m_num_acceptors);
    message m;
    m.has_c = true;
    m.c = cmd;
    m_msgs[idx].push(m);
    m_cond.broadcast();
}

void
generalized_paxos_state_machine :: finish()
{
    {
        po6::threads::mutex::hold hold(&m_mtx);
        m_cond.broadcast();
        m_done = true;
    }

    for (size_t i = 0; i < m_num_acceptors; ++i)
    {
        m_acceptors[i]->join();
    }
}

void
generalized_paxos_state_machine :: acceptor(state_machine* sm, abstract_id id)
{
    assert(id > abstract_id());
    const size_t us = id.get() - 1;
    generalized_paxos gp;
    std::vector<abstract_id> ids;

    {
        po6::threads::mutex::hold hold(&m_mtx);
        ids.resize(m_num_acceptors);
    }

    for (size_t i = 0; i < ids.size(); ++i)
    {
        ids[i] = abstract_id(i + 1);
    }

    gp.init(&cmp, id, &ids[0], ids.size());
    uint16_t randbuf[3];
    randbuf[0] = us;

    while (true)
    {
        std::queue<message> messages;

        {
            po6::threads::mutex::hold hold(&m_mtx);

            while (!m_done && m_msgs[us].empty())
            {
                m_cond.wait();
            }

            if (m_done)
            {
                break;
            }

            while (!m_msgs[us].empty())
            {
                messages.push(m_msgs[us].front());
                m_msgs[us].pop();
            }
        }

        while (!messages.empty())
        {
            message m = messages.front();
            messages.pop();

            if (m.has_c)
            {
                gp.propose(m.c);
            }

            if (m.has_p1a)
            {
                bool send = false;
                generalized_paxos::message_p1b r;
                gp.process_p1a(m.p1a, &send, &r);

                if (send)
                {
                    send_to_all(r);
                }
            }

            if (m.has_p1b)
            {
                gp.process_p1b(m.p1b);
            }

            if (m.has_p2a)
            {
                bool send = false;
                generalized_paxos::message_p2b r;
                gp.process_p2a(m.p2a, &send, &r);

                if (send)
                {
                    send_to_all(r);
                }
            }

            if (m.has_p2b)
            {
                gp.process_p2b(m.p2b);
                gp.propose_from_p2b(m.p2b);
            }
        }

        bool may_attempt_leadership = id == abstract_id(1);

        if (nrand48(randbuf) < (1LL << 10))
        {
            may_attempt_leadership = true;
        }

        bool send_m1 = false;
        bool send_m2 = false;
        bool send_m3 = false;
        generalized_paxos::message_p1a m1;
        generalized_paxos::message_p2a m2;
        generalized_paxos::message_p2b m3;
        gp.advance(may_attempt_leadership,
                   &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);

        if (send_m1)
        {
            send_to_all(m1);
        }

        if (send_m2)
        {
            send_to_all(m2);
        }

        if (send_m3)
        {
            send_to_all(m3);
        }

        generalized_paxos::cstruct L = gp.learned();
        sm->execute(&L.commands[0], L.commands.size());
    }
}

void
generalized_paxos_state_machine :: send_to_all(const generalized_paxos::message_p1a& m)
{
    message n;
    n.has_p1a = true;
    n.p1a = m;
    send_to_all(n);
}

void
generalized_paxos_state_machine :: send_to_all(const generalized_paxos::message_p1b& m)
{
    message n;
    n.has_p1b = true;
    n.p1b = m;
    send_to_all(n);
}

void
generalized_paxos_state_machine :: send_to_all(const generalized_paxos::message_p2a& m)
{
    message n;
    n.has_p2a = true;
    n.p2a = m;
    send_to_all(n);
}

void
generalized_paxos_state_machine :: send_to_all(const generalized_paxos::message_p2b& m)
{
    message n;
    n.has_p2b = true;
    n.p2b = m;
    send_to_all(n);
}

void
generalized_paxos_state_machine :: send_to_all(const message& m)
{
    po6::threads::mutex::hold hold(&m_mtx);

    for (size_t i = 0; i < m_num_acceptors; ++i)
    {
        m_msgs[i].push(m);
    }

    m_cond.broadcast();
}

int
main(int argc, const char* argv[])
{
    long lists = 8;
    long acceptors = 5;
    long iterations = 1000000;
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('l', "lists")
            .description("how many lists to create (default: 8)")
            .as_long(&lists);
    ap.arg().name('a', "acceptors")
            .description("how many acceptors to use (default: 5)")
            .as_long(&acceptors);
    ap.arg().name('e', "elements")
            .description("how many elements to add (default: 1,000,000)")
            .as_long(&iterations);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (lists <= 0)
    {
        std::cerr << "must specify a positive number of lists\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (acceptors <= 0)
    {
        std::cerr << "must specify a positive number of acceptors\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    std::vector<state_machine*> sm;

    for (long i = 0; i < acceptors; ++i)
    {
        sm.push_back(new state_machine(lists));
    }

    generalized_paxos_state_machine gp(&sm[0], sm.size());
    unsigned short randbuf[3];
    memset(randbuf, 0, sizeof(randbuf));

    for (uint64_t i = 1; i <= uint64_t(iterations); ++i)
    {
        generalized_paxos::command c;
        c.type = nrand48(randbuf) % lists;
        e::packer(&c.value) << i;
        gp.propose(c);
    }

    for (size_t i = 0; i < sm.size(); ++i)
    {
        while (sm[i]->complete() < uint64_t(iterations))
        {
            gp.wait();
        }
    }

    gp.finish();

    for (long i = 0; i < lists; ++i)
    {
        const std::vector<uint64_t>& ref(sm[0]->list(i));

        for (size_t j = 1; j < sm.size(); ++j)
        {
            const std::vector<uint64_t>& x(sm[j]->list(i));

            if (ref.size() != x.size())
            {
                std::cerr << "incomplete" << std::endl;
                return EXIT_FAILURE;
            }

            if (!std::equal(ref.begin(), ref.end(), x.begin()))
            {
                std::cerr << "inconsistent" << std::endl;
                return EXIT_FAILURE;
            }
        }
    }

    return EXIT_SUCCESS;
}
