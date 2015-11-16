// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#define __STDC_LIMIT_MACROS

// consus
#include "test/th.h"
#include "txman/generalized_paxos.h"

using namespace consus;

struct no_conflict_comparator : public generalized_paxos::comparator
{
    no_conflict_comparator() {}
    virtual ~no_conflict_comparator() throw () {}

    virtual bool conflict(const generalized_paxos::command&, const generalized_paxos::command&) const
    {
        return false;
    }
};
no_conflict_comparator ncc;

struct all_conflict_comparator : public generalized_paxos::comparator
{
    all_conflict_comparator() {}
    virtual ~all_conflict_comparator() throw () {}

    virtual bool conflict(const generalized_paxos::command&, const generalized_paxos::command&) const
    {
        return true;
    }
};
all_conflict_comparator acc;

abstract_id _ids[] = {abstract_id(1),
                      abstract_id(2),
                      abstract_id(3),
                      abstract_id(4),
                      abstract_id(5),
                      abstract_id(6),
                      abstract_id(7),
                      abstract_id(8),
                      abstract_id(9)};

TEST(GeneralizedPaxos, FiveAcceptors)
{
    // ignore gp[0] to make 1-indexed for easy reading
    generalized_paxos gp[6];

    gp[1].init(&ncc, abstract_id(1), _ids, 5);
    gp[2].init(&ncc, abstract_id(2), _ids, 5);
    gp[3].init(&ncc, abstract_id(3), _ids, 5);
    gp[4].init(&ncc, abstract_id(4), _ids, 5);
    gp[5].init(&ncc, abstract_id(5), _ids, 5);

    bool send_m1 = false;
    bool send_m2 = false;
    bool send_m3 = false;
    generalized_paxos::message_p1a m1;
    generalized_paxos::message_p2a m2;
    generalized_paxos::message_p2b m3;
    generalized_paxos::message_p1b r1;

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_TRUE(send_m1);
    ASSERT_FALSE(send_m2);
    ASSERT_FALSE(send_m3);

    for (int i = 1; i <= 5; ++i)
    {
        bool send = false;
        gp[i].process_p1a(m1, &send, &r1);

        if (send)
        {
            gp[1].process_p1b(r1);
        }
    }

    gp[1].propose(generalized_paxos::command(1, "hello world from 1"));
    gp[2].propose(generalized_paxos::command(2, "hello world from 2"));
    gp[3].propose(generalized_paxos::command(3, "hello world from 3"));
    gp[4].propose(generalized_paxos::command(4, "hello world from 4"));
    gp[5].propose(generalized_paxos::command(5, "hello world from 5"));

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_FALSE(send_m1);
    ASSERT_FALSE(send_m2);
    ASSERT_TRUE(send_m3);

    for (int i = 1; i <= 5; ++i)
    {
        gp[i].process_p2b(m3);
        gp[i].propose_from_p2b(m3);
    }

    for (int i = 0; i < 10; ++i)
    {
        const unsigned idx = (i % 5) + 1;
        gp[idx].advance(false, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
        ASSERT_FALSE(send_m1);
        ASSERT_FALSE(send_m2);
        ASSERT_TRUE(send_m3);

        for (int j = 1; j <= 5; ++j)
        {
            gp[j].process_p2b(m3);
            gp[j].propose_from_p2b(m3);
        }
    }

    for (int i = 1; i <= 5; ++i)
    {
        generalized_paxos::cstruct v = gp[i].learned();
        ASSERT_EQ(5U, v.commands.size());
    }
}

TEST(GeneralizedPaxos, Conflict)
{
    // ignore gp[0] to make 1-indexed for easy reading
    generalized_paxos gp[4];

    gp[1].init(&acc, abstract_id(1), _ids, 3);
    gp[2].init(&acc, abstract_id(2), _ids, 3);
    gp[3].init(&acc, abstract_id(3), _ids, 3);

    bool send_m1 = false;
    bool send_m2 = false;
    bool send_m3 = false;
    generalized_paxos::message_p1a m1;
    generalized_paxos::message_p2a m2;
    generalized_paxos::message_p2b m3;
    generalized_paxos::message_p1b r1;

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_TRUE(send_m1);
    ASSERT_FALSE(send_m2);
    ASSERT_FALSE(send_m3);

    for (int i = 1; i <= 3; ++i)
    {
        bool send = false;
        gp[i].process_p1a(m1, &send, &r1);

        if (send)
        {
            gp[1].process_p1b(r1);
        }
    }

    gp[1].propose(generalized_paxos::command(1, "operation 1"));
    gp[1].propose(generalized_paxos::command(2, "operation 2"));
    gp[1].propose(generalized_paxos::command(3, "operation 3"));

    gp[2].propose(generalized_paxos::command(2, "operation 2"));
    gp[2].propose(generalized_paxos::command(3, "operation 3"));
    gp[2].propose(generalized_paxos::command(1, "operation 1"));

    gp[3].propose(generalized_paxos::command(3, "operation 3"));
    gp[3].propose(generalized_paxos::command(1, "operation 1"));
    gp[3].propose(generalized_paxos::command(2, "operation 2"));

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_FALSE(send_m1);
    ASSERT_FALSE(send_m2);
    ASSERT_TRUE(send_m3);

    for (int i = 1; i <= 3; ++i)
    {
        gp[i].process_p2b(m3);
    }

    for (int i = 0; i < 6; ++i)
    {
        const unsigned idx = (i % 3) + 1;
        gp[idx].advance(false, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
        ASSERT_FALSE(send_m1);
        ASSERT_FALSE(send_m1);
        ASSERT_FALSE(send_m2);
        ASSERT_TRUE(send_m3);

        for (int j = 1; j <= 3; ++j)
        {
            gp[j].process_p2b(m3);
        }
    }

    generalized_paxos::cstruct v;

    for (int i = 1; i <= 3; ++i)
    {
        v = gp[i].learned();
        ASSERT_EQ(0U, v.commands.size());
    }

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_TRUE(send_m1);
    ASSERT_EQ(generalized_paxos::ballot::CLASSIC, m1.b.type);
    ASSERT_FALSE(send_m2);
    ASSERT_TRUE(send_m3);

    for (int i = 1; i <= 3; ++i)
    {
        bool send = false;
        gp[i].process_p1a(m1, &send, &r1);

        if (send)
        {
            gp[1].process_p1b(r1);
        }
    }

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_FALSE(send_m1);
    ASSERT_TRUE(send_m2);
    ASSERT_FALSE(send_m3);

    for (int i = 1; i <= 3; ++i)
    {
        bool send = false;
        gp[i].process_p2a(m2, &send, &m3);

        if (send)
        {
            for (int j = 1; j <= 3; ++j)
            {
                gp[j].process_p2b(m3);
            }
        }
    }

    for (int i = 1; i <= 3; ++i)
    {
        v = gp[i].learned();
        ASSERT_EQ(3U, v.commands.size());
        generalized_paxos::cstruct u;
        u.commands.push_back(generalized_paxos::command(1, "operation 1"));
        u.commands.push_back(generalized_paxos::command(2, "operation 2"));
        u.commands.push_back(generalized_paxos::command(3, "operation 3"));
        ASSERT_EQ(v, u);
    }

    gp[1].advance(true, &send_m1, &m1, &send_m2, &m2, &send_m3, &m3);
    ASSERT_TRUE(send_m1);
    ASSERT_EQ(generalized_paxos::ballot::FAST, m1.b.type);
    ASSERT_FALSE(send_m2);
    ASSERT_FALSE(send_m3);
}
