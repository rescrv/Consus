// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/constants.h"
#include "common/consus.h"
#include "common/network_msgtype.h"
#include "kvs/daemon.h"
#include "kvs/lock_replicator.h"

using consus::lock_replicator;

extern bool s_debug_mode;

// Locking is a bit different than reading or writing.  With writing, it's
// assumed that at most one writer will write a given timestamp, which is
// enforced by locking.  A write at T_i will always be the same value and can be
// issued endlessly forever until a quorum acknowledges.
//
// Similarly, readers will look for the highest value from a quorum, and be
// protected by a lock that prevents someone from inserting a higher value in
// the interim.  They can endlessly retry until a quorum answers, and will only
// ever see the highest value.
//
// Locking doesn't have the benefit of assuming non-concurrency and repeatable
// requests.  Consider a "lock" operation that gets issued twice (as may happen
// with all messages (actually, it could be issued infinitely many times on an
// infinite time scale)), where one of the messages gets delayed.  If there is a
// subsequent unlock operation for the same transaction, the delayed lock would
// effectively re-lock the lock.  The implementation must prevent this case or
// ensure that it is harmless.  The rest of this comment describes why the
// protocol implemented by the lock_replicator is correct.
//
// In this protocol, a lock is over a (table, key) pair, and is held by a
// particular transaction, identified by its transaction id.  There are two
// invariants that ensure the safety of these locks:
//
// I1:  Transactions will perform "unlock" operations only after it has durably
// recorded its commit/abort outcome.
//
// I2:  The only entities that initiate "unlock" operations for a transaction
// are members of the paxos group(s) that executed it.
//
// From the first invariant, we can see that the outcome of an individual
// transaction is not affected by anything that happens after the first unlock.
// This means that we can safely allow the scenario described above where
// multiple lost "lock" messages cause a lock to become "re-locked" by a
// transaction.  The worst thing that happens is a lock remains held in error.
// This will not affect transactional correctness, but can affect liveness.
//
// To ensure liveness, we need to add some way of unlocking a lock that is held
// in error.  This, however, becomes tricky as any heuristic for guessing when a
// lock is held in error will have a corner case where it guesses wrong.  By
// upholding I2 in the implementation, which is trivial to construct, we ensure
// there is a single point in the entire system where the decision to unlock a
// lock can be made; coincidentally, it's also the place where a transaction's
// outcome is durably recorded.  This ensures that the decision to unlock never
// violates I1.
//
// The mechanism for ensuring liveness is to leak the current lockholder to
// other transactions vying for the same lock.  These transactions signal their
// intent to the transaction holding the lock.  For deadlock avoidance, the
// holder will yield to a transaction of a lower timestamp by aborting its
// transaction and subsequently unlocking the lock; otherwise, it will either
// ignore the signal and continue executing or unlock a spuriously-locked lock.

struct lock_replicator :: lock_stub
{
    lock_stub(comm_id t);
    ~lock_stub() throw () {}

    comm_id target;
    uint64_t last_request_time;
    transaction_group tg;
    replica_set rs;
};

lock_replicator :: lock_stub :: lock_stub(comm_id t)
    : target(t)
    , last_request_time(0)
    , tg()
    , rs()
{
}

lock_replicator :: lock_replicator(uint64_t key)
    : m_state_key(key)
    , m_mtx()
    , m_init(false)
    , m_finished(false)
    , m_id()
    , m_nonce()
    , m_table()
    , m_key()
    , m_tg()
    , m_op()
    , m_backing()
    , m_requests()
{
}

lock_replicator :: ~lock_replicator() throw ()
{
}

uint64_t
lock_replicator :: state_key()
{
    return m_state_key;
}

bool
lock_replicator :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return !m_init || m_finished;
}

void
lock_replicator :: init(comm_id id, uint64_t nonce,
                        const e::slice& table, const e::slice& key,
                        const transaction_group& tg, lock_op op,
                        std::auto_ptr<e::buffer> backing)
{
    po6::threads::mutex::hold hold(&m_mtx);
    assert(!m_init);
    m_id = id;
    m_nonce = nonce;
    m_table = table;
    m_key = key;
    m_tg = tg;
    m_op = op;
    m_backing = backing;
    m_init = true;

    if (s_debug_mode)
    {
        LOG(INFO) << logid()
                  << " table=\"" << e::strescape(table.str())
                  << "\" key=\"" << e::strescape(key.str())
                  << "\" transaction=" << tg
                  << " nonce=" << nonce << " id=" << id;
    }
}

void
lock_replicator :: response(comm_id id, const transaction_group& tg,
                            const replica_set& rs, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    lock_stub* stub = get_stub(id);

    if (!stub)
    {
        return;
    }

    stub->tg = tg;
    stub->rs = rs;
    work_state_machine(d);
}

void
lock_replicator :: abort(const transaction_group& tg, daemon* d)
{
    drop(tg);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(TXMAN_WOUND)
                    + pack_size(tg);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << TXMAN_WOUND << tg;
    po6::threads::mutex::hold hold(&m_mtx);
    LOG_IF(INFO, s_debug_mode) << logid() << " sending wound message for " << transaction_group::log(tg);
    d->send(m_id, msg);
}

void
lock_replicator :: drop(const transaction_group& tg)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (m_tg == tg)
    {
        m_finished = true;
        m_requests.clear();
        LOG_IF(INFO, s_debug_mode) << logid() << " dropping transaction";
    }
}

void
lock_replicator :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

std::string
lock_replicator :: logid()
{
    std::string s = daemon::logid(m_table, m_key)
                  + ":" + transaction_group::log(m_tg);

    switch (m_op)
    {
        case LOCK_LOCK:
            return s + "-LL-REP";
        case LOCK_UNLOCK:
            return s + "-LU-REP";
        default:
            return s + "-L?-REP";
    }
}

lock_replicator::lock_stub*
lock_replicator :: get_stub(comm_id id)
{
    for (size_t j = 0; j < m_requests.size(); ++j)
    {
        if (m_requests[j].target == id)
        {
            return &m_requests[j];
        }
    }

    return NULL;
}

lock_replicator::lock_stub*
lock_replicator :: get_or_create_stub(comm_id id)
{
    lock_stub* ws = get_stub(id);

    if (!ws && id != comm_id())
    {
        m_requests.push_back(lock_stub(id));
        ws = &m_requests.back();
    }

    return ws;
}

void
lock_replicator :: work_state_machine(daemon* d)
{
    configuration* c = d->get_config();
    replica_set rs;

    if (!c->hash(d->m_us.dc, m_table, m_key, &rs))
    {
        // XXX
    }

    const uint64_t now = po6::monotonic_time();
    unsigned complete = 0;

    for (unsigned i = 0; i < rs.num_replicas; ++i)
    {
        lock_stub* owner1 = get_or_create_stub(rs.replicas[i]);
        lock_stub* owner2 = get_or_create_stub(rs.transitioning[i]);
        // need to do it again in case anything was created
        owner1 = get_stub(rs.replicas[i]);
        owner2 = get_stub(rs.transitioning[i]);
        assert(owner1);
        bool agree = !owner2 || replica_sets_agree(rs.replicas[i], owner1->rs, owner2->rs);

        if (owner1->tg == m_tg && (!owner2 || owner2->tg == m_tg) && agree)
        {
            ++complete;
            continue;
        }

        if (owner1->last_request_time + d->resend_interval() < now &&
            (owner1->tg != m_tg || !agree))
        {
            send_lock_request(owner1, now, d);
        }

        if (owner2 && owner2->last_request_time + d->resend_interval() < now &&
            (owner2->tg != m_tg || !agree))
        {
            send_lock_request(owner2, now, d);
        }
    }

    bool short_lock = false;

    if (rs.desired_replication > rs.num_replicas)
    {
        LOG_EVERY_N(WARNING, 1000) << "too few kvs daemons to achieve desired replication factor: "
                                   << rs.desired_replication - rs.num_replicas
                                   << " more daemons needed";
        rs.desired_replication = rs.num_replicas;
        short_lock = true;
    }

    const unsigned quorum = rs.desired_replication / 2 + 1;

    if (complete >= quorum)
    {
        consus_returncode rc = short_lock ? CONSUS_LESS_DURABLE : CONSUS_SUCCESS;
        m_finished = true;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(KVS_LOCK_OP_RESP)
                        + sizeof(uint64_t)
                        + pack_size(rc);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_LOCK_OP_RESP << m_nonce << rc;
        d->send(m_id, msg);

        if (s_debug_mode)
        {
            LOG(INFO) << logid() << " response=" << rc;
        }
    }
}

void
lock_replicator :: send_lock_request(lock_stub* stub, uint64_t now, daemon* d)
{
    if (s_debug_mode)
    {
        LOG(INFO) << logid() << " sending target=" << stub->target;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_LK)
                    + sizeof(uint64_t)
                    + pack_size(m_table)
                    + pack_size(m_key)
                    + pack_size(m_tg)
                    + pack_size(m_op);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RAW_LK << m_state_key << m_table << m_key << m_tg << m_op;
    d->send(stub->target, msg);
    stub->last_request_time = now;
}
