// Copyright (c) 2017, Robert Escriva, Cornell University
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

#define MAX_ACCEPTORS 13

using namespace consus;

struct comparator : public generalized_paxos::comparator
{
    comparator() {}
    virtual ~comparator() throw () {}
    virtual bool conflict(const generalized_paxos::command&,
                          const generalized_paxos::command&) const { return true; }
};

comparator cmp;

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
    ~message() throw ();

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

message :: ~message() throw ()
{
}

class queue
{
    public:
        queue();
        ~queue() throw ();

    public:
        const message* pop();
        void push(const message* m);
        void done();

    private:
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cnd;
        std::queue<const message*> m_q;
        bool m_done;
};

queue :: queue()
    : m_mtx()
    , m_cnd(&m_mtx)
    , m_q()
    , m_done(false)
{
}

queue :: ~queue() throw ()
{
}

const message*
queue :: pop()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_q.empty() && !m_done)
    {
        m_cnd.wait();
    }

    if (!m_q.empty())
    {
        const message* m = m_q.front();
        m_q.pop();
        return m;
    }
    else
    {
        return NULL;
    }
}

void
queue :: push(const message* m)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_q.empty())
    {
        m_cnd.signal();
    }

    m_q.push(m);
}

void
queue :: done()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_cnd.broadcast();
    m_done = true;
}

class server
{
    public:
        server(queue* queues, unsigned queues_sz, unsigned idx, long learn);
        ~server() throw ();

    public:
        void run();
        bool done(bool wait);

    private:
        void handle_command(const message* m);
        void handle_p1a(const message* m);
        void handle_p1b(const message* m);
        void handle_p2a(const message* m);
        void handle_p2b(const message* m);
        void work_state_machine();
        void send_to_all(const generalized_paxos::message_p1a& m);
        void send_to_all(const generalized_paxos::message_p1b& m);
        void send_to_all(const generalized_paxos::message_p2a& m);
        void send_to_all(const generalized_paxos::message_p2b& m);
        void send_to_all(message* m);

    private:
        queue* m_queues;
        unsigned m_queues_sz;
        unsigned m_idx;
        uint16_t m_randbuf[3];
        generalized_paxos m_gp;
        long m_learn;
        po6::threads::mutex m_done_mtx;
        po6::threads::cond m_done_cnd;
        bool m_done;
        generalized_paxos::message_p1a m_prev_p1a;
        generalized_paxos::message_p1b m_prev_p1b;
        generalized_paxos::message_p2a m_prev_p2a;
        generalized_paxos::message_p2b m_prev_p2b;

    private:
        server(const server&);
        server& operator = (const server&);
};

server :: server(queue* queues, unsigned queues_sz, unsigned idx, long learn)
    : m_queues(queues)
    , m_queues_sz(queues_sz)
    , m_idx(idx)
    , m_gp()
    , m_learn(learn)
    , m_done_mtx()
    , m_done_cnd(&m_done_mtx)
    , m_done(false)
    , m_prev_p1a()
    , m_prev_p1b()
    , m_prev_p2a()
    , m_prev_p2b()
{
    m_randbuf[0] = m_idx;
    m_randbuf[1] = m_queues_sz;
    m_randbuf[2] = 0xdeadU;
}

server :: ~server() throw ()
{
}

void
server :: run()
{
    abstract_id ids[MAX_ACCEPTORS];

    for (unsigned i = 0; i < m_queues_sz; ++i)
    {
        ids[i] = abstract_id(i + 1);
    }

    m_gp.init(&cmp, ids[m_idx], &ids[0], m_queues_sz);
    const message* msg;

    while ((msg = m_queues[m_idx].pop()))
    {
        handle_command(msg);
        handle_p1a(msg);
        handle_p1b(msg);
        handle_p2a(msg);
        handle_p2b(msg);
    }
}

bool
server :: done(bool wait)
{
    po6::threads::mutex::hold hold(&m_done_mtx);

    while (wait && !m_done)
    {
        m_done_cnd.wait();
    }

    return m_done;
}

void
server :: handle_command(const message* msg)
{
    if (msg->has_c)
    {
        if (m_gp.propose(msg->c))
        {
            m_queues[(m_idx + 1) % m_queues_sz].push(msg);
            work_state_machine();
        }
    }
}

void
server :: handle_p1a(const message* msg)
{
    if (msg->has_p1a)
    {
        bool send = false;
        generalized_paxos::message_p1b r;
        m_gp.process_p1a(msg->p1a, &send, &r);

        if (send)
        {
            send_to_all(r);
        }

        work_state_machine();
    }
}

void
server :: handle_p1b(const message* msg)
{
    if (msg->has_p1b)
    {
        if (m_gp.process_p1b(msg->p1b))
        {
            work_state_machine();
        }
    }
}

void
server :: handle_p2a(const message* msg)
{
    if (msg->has_p2a)
    {
        bool send = false;
        generalized_paxos::message_p2b r;
        m_gp.process_p2a(msg->p2a, &send, &r);

        if (send)
        {
            send_to_all(r);
        }

        work_state_machine();
    }
}

void
server :: handle_p2b(const message* msg)
{
    if (msg->has_p2b)
    {
        if (m_gp.process_p2b(msg->p2b))
        {
            work_state_machine();
        }
    }
}

void
server :: work_state_machine()
{
    bool may_attempt_leadership = m_idx == 0;

    if (nrand48(m_randbuf) < (1LL << 10))
    {
        //may_attempt_leadership = true;
    }

    bool send_m1 = false;
    bool send_m2 = false;
    bool send_m3 = false;
    generalized_paxos::message_p1a m1;
    generalized_paxos::message_p2a m2;
    generalized_paxos::message_p2b m3;
    m_gp.advance(may_attempt_leadership,
                 &send_m1, &m1,
                 &send_m2, &m2,
                 &send_m3, &m3);

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

    generalized_paxos::cstruct L = m_gp.learned();

    if ((long)L.commands.size() >= m_learn)
    {
        po6::threads::mutex::hold hold(&m_done_mtx);
        m_done_cnd.broadcast();
        m_done = true;
    }
}

void
server :: send_to_all(const generalized_paxos::message_p1a& m)
{
    if (m == m_prev_p1a)
    {
        return;
    }

    m_prev_p1a = m;
    message* msg = new message();
    msg->has_p1a = true;
    msg->p1a = m;
    send_to_all(msg);
}

void
server :: send_to_all(const generalized_paxos::message_p1b& m)
{
    if (m == m_prev_p1b)
    {
        return;
    }

    m_prev_p1b = m;
    message* msg = new message();
    msg->has_p1b = true;
    msg->p1b = m;
    send_to_all(msg);
}

void
server :: send_to_all(const generalized_paxos::message_p2a& m)
{
    if (m == m_prev_p2a)
    {
        return;
    }

    m_prev_p2a = m;
    message* msg = new message();
    msg->has_p2a = true;
    msg->p2a = m;
    send_to_all(msg);
}

void
server :: send_to_all(const generalized_paxos::message_p2b& m)
{
    if (m == m_prev_p2b)
    {
        return;
    }

    m_prev_p2b = m;
    message* msg = new message();
    msg->has_p2b = true;
    msg->p2b = m;
    send_to_all(msg);
}

void
server :: send_to_all(message* m)
{
    for (unsigned i = 0; i < m_queues_sz; ++i)
    {
        m_queues[i].push(m);
    }
}

int
main(int argc, const char* argv[])
{
    long acceptors = 5;
    long numbers = 1000000;
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('a', "acceptors")
            .description("how many acceptors to use (default: 5)")
            .as_long(&acceptors);
    ap.arg().name('n', "numbers")
            .description("how many numbers to add (default: 1,000,000)")
            .as_long(&numbers);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (acceptors <= 0)
    {
        std::cerr << "must specify a positive number of acceptors\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (acceptors > MAX_ACCEPTORS)
    {
        std::cerr << "must specify at most " << MAX_ACCEPTORS << " acceptors\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (numbers <= 0)
    {
        std::cerr << "must specify a positive number of elements\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    queue queues[MAX_ACCEPTORS];
    server* servers[MAX_ACCEPTORS];

    for (long i = 0; i < acceptors; ++i)
    {
        servers[i] = new server(queues, acceptors, i, numbers);
    }

    std::vector<e::compat::shared_ptr<po6::threads::thread> > threads;

    for (long i = 0; i < acceptors; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> ptr(new thread(make_obj_func(&server::run, servers[i])));
        threads.push_back(ptr);
    }

    for (long i = 0; i < acceptors; ++i)
    {
        threads[i]->start();
    }

    for (long i = 0; i < numbers; ++i)
    {
        message* m = new message();
        m->has_c = true;
        m->c.type = 1;
        e::packer(&m->c.value) << uint64_t(i);
        queues[i % acceptors].push(m);
    }

    for (long i = 0; i < acceptors; ++i)
    {
        servers[i]->done(true);
    }

    for (long i = 0; i < acceptors; ++i)
    {
        queues[i].push(NULL);
        threads[i]->join();
    }

    return EXIT_SUCCESS;
}
