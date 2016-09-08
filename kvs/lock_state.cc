// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// Google Log
#include <glog/logging.h>

// consus
#include "common/network_msgtype.h"
#include "kvs/configuration.h"
#include "kvs/daemon.h"
#include "kvs/lock_state.h"

using consus::lock_state;

extern bool s_debug_mode;

struct lock_state::request
{
    request() : id(), nonce(), tg() {}
    request(comm_id i, uint64_t n, const transaction_group& x)
        : id(i), nonce(n), tg(x) {}
    ~request() throw () {}
    comm_id id;
    uint64_t nonce;
    transaction_group tg;
};

lock_state :: lock_state(const table_key_pair& tk)
    : m_state_key(tk)
    , m_mtx()
    , m_init(false)
    , m_holder()
    , m_reqs()
{
}

lock_state :: ~lock_state() throw ()
{
}

consus :: table_key_pair
lock_state :: state_key()
{
    return m_state_key;
}

bool
lock_state :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return !m_init || (m_reqs.empty() && m_holder == transaction_group());
}

void
lock_state :: enqueue_lock(comm_id id, uint64_t nonce,
                           const transaction_group& tg,
                           daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    invariant_check();

    if (!ensure_initialized(d))
    {
        return;
    }

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << " lock(\""
                  << e::strescape(m_state_key.table) << "\", \""
                  << e::strescape(m_state_key.key) << "\") nonce=" << nonce
                  << " id=" << id;
    }

    if (m_holder == tg)
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " lock already held; nonce=" << nonce << " id=" << id;
        send_response(id, nonce, tg, d);
        invariant_check();
        return;
    }

    bool found = false;

    // scan the enqueued transactions to see if this is already enqueued
    for (std::list<request>::iterator it = m_reqs.begin();
            it != m_reqs.end(); ++it)
    {
        // we found this transaction group vying for the lock
        if (it->tg == tg)
        {
            found = true;

            // if the previous requester has a higher nonce than the current
            // requester, tell prev to silently stop replicating
            if (it->nonce > nonce)
            {
                LOG_IF(INFO, s_debug_mode) << logid() << " drop-wounding "
                    << transaction_group::log(tg) << "; nonce=" << it->nonce << " id=" << it->id;
                send_wound_drop(it->id, it->nonce, it->tg, d);
                it->id = id;
                it->nonce = nonce;
            }
            // else, tell current to silently stop replicating
            else
            {
                LOG_IF(INFO, s_debug_mode) << logid() << " drop-wounding "
                                           << transaction_group::log(tg)
                                           << "; nonce=" << nonce << " id=" << id;
                send_wound_drop(id, nonce, tg, d);
            }
        }
    }

    if (!found)
    {
        ordered_enqueue(request(id, nonce, tg));
    }

    // if no one holds the lock, we take the lock
    if (m_holder == transaction_group())
    {
        assert(m_reqs.size() == 1);
        consus_returncode rc = d->m_data->write_lock(m_state_key.table,
                                                     m_state_key.key, tg);

        if (rc != CONSUS_SUCCESS)
        {
            LOG(ERROR) << "failed lock(\""
                       << e::strescape(m_state_key.table)
                       << "\", \""
                       << e::strescape(m_state_key.key)
                       << "\") nonce=" << nonce;
            assert(m_reqs.front().tg == tg);
            m_reqs.pop_front();
            invariant_check();
            return;
        }

        send_response(id, nonce, tg, d);
        m_holder = tg;
    }

    if (tg.txid.preempts(m_holder.txid))
    {
        send_wound_abort(id, nonce, m_holder, d);
        LOG_IF(INFO, s_debug_mode) << logid()
                                   << transaction_group::log(tg)
                                   << " abort-wounds "
                                   << transaction_group::log(m_holder);
    }

    invariant_check();
}

void
lock_state :: unlock(comm_id id, uint64_t nonce,
                     const transaction_group& tg,
                     daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    invariant_check();

    if (!ensure_initialized(d))
    {
        return;
    }

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << " unlock(\""
                  << e::strescape(m_state_key.table) << "\", \""
                  << e::strescape(m_state_key.key) << "\") nonce=" << nonce
                  << " id=" << id;
    }

    if (m_holder == tg)
    {
        assert(!m_reqs.empty());
        assert(m_reqs.front().tg == tg);
        request next;

        if (m_reqs.size() > 1)
        {
            std::list<request>::iterator it = m_reqs.begin();
            ++it;
            assert(it != m_reqs.end());
            next = *it;
        }

        consus_returncode rc = d->m_data->write_lock(m_state_key.table,
                                                     m_state_key.key,
                                                     next.tg);

        if (rc != CONSUS_SUCCESS)
        {
            LOG(ERROR) << logid() << " failed unlock(\""
                       << e::strescape(m_state_key.table)
                       << "\", \""
                       << e::strescape(m_state_key.key)
                       << "\") nonce=" << nonce;
            invariant_check();
            return;
        }

        m_reqs.pop_front();
        m_holder = next.tg;

        if (next.tg != transaction_group())
        {
            send_response(next.id, next.nonce, next.tg, d);
        }
    }
    else
    {
        for (std::list<request>::iterator it = m_reqs.begin(); it != m_reqs.end(); )
        {
            if (it->tg == tg)
            {
                it = m_reqs.erase(it);
                LOG_IF(INFO, s_debug_mode) << logid() << " drop-wounding "
                    << transaction_group::log(tg) << "; nonce=" << it->nonce << " id=" << it->id;
                send_wound_drop(it->id, it->nonce, it->tg, d);
            }
            else
            {
                ++it;
            }
        }
    }

    // see reasoning in lock_replicator.cc for why we unconditionally act as if
    // we unlocked the lock
    send_response(id, nonce, tg, d);
    invariant_check();
}

std::string
lock_state :: debug_dump()
{
    po6::threads::mutex::hold hold(&m_mtx);
    std::ostringstream ostr;
    ostr << "lock holder=" << transaction_group::log(m_holder) << "\n";
    size_t i = 0;

    for (std::list<request>::iterator it = m_reqs.begin();
            it != m_reqs.end(); ++it, ++i)
    {
        ostr << "lock queue[" << i << "]"
             << " tx=" << transaction_group::log(it->tg)
             << " id=" << it->id << " nonce=" << it->nonce << "\n";
    }

    return ostr.str();
}

std::string
lock_state :: logid()
{
    return daemon::logid(m_state_key.table, m_state_key.key) + "-LS";
}

void
lock_state :: invariant_check()
{
    if (!m_init || m_reqs.empty())
    {
        assert(m_holder == transaction_group());
        assert(m_reqs.empty());
    }
    else
    {
        assert(m_holder == m_reqs.front().tg);

        for (std::list<request>::iterator it1 = m_reqs.begin();
                it1 != m_reqs.end(); ++it1)
        {
            for (std::list<request>::iterator it2 = m_reqs.begin();
                    it2 != m_reqs.end(); ++it2)
            {
                assert(it1 == it2 || it1->tg != it2->tg);
            }
        }
    }
}

bool
lock_state :: ensure_initialized(daemon* d)
{
    invariant_check();

    if (m_init)
    {
        return true;
    }

    transaction_group tg;
    consus_returncode rc = d->m_data->read_lock(m_state_key.table,
                                                m_state_key.key, &tg);

    if (rc != CONSUS_SUCCESS && rc != CONSUS_NOT_FOUND)
    {
        LOG(ERROR) << "failed to initialize lock (\""
                   << e::strescape(m_state_key.table)
                   << "\", \""
                   << e::strescape(m_state_key.key)
                   << "\")";
        return false;
    }

    if (tg != transaction_group())
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " restoring " << transaction_group::log(tg) << " as durable lock holder";
        m_reqs.push_back(request(comm_id(), 0, tg));
        m_holder = tg;
    }

    m_init = true;
    invariant_check();
    return true;
}

void
lock_state :: ordered_enqueue(const request& r)
{
    std::list<request>::iterator it = m_reqs.begin();

    if (it != m_reqs.end())
    {
        ++it;
    }

    while (it != m_reqs.end() && r.tg.txid.preempts(m_holder.txid))
    {
        ++it;
    }

    m_reqs.insert(it, r);
}

void
lock_state :: send_wound(comm_id id, uint64_t nonce, uint8_t action,
                         const transaction_group& tg,
                         daemon* d)
{
    if (id == comm_id())
    {
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WOUND_XACT)
                    + sizeof(uint64_t)
                    + sizeof(uint8_t)
                    + pack_size(tg);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_WOUND_XACT << nonce << action << tg;
    d->send(id, msg);
}

void
lock_state :: send_wound_drop(comm_id id, uint64_t nonce,
                              const transaction_group& tg,
                              daemon* d)
{
    send_wound(id, nonce, WOUND_XACT_DROP_REQ, tg, d);
}

void
lock_state :: send_wound_abort(comm_id id, uint64_t nonce,
                               const transaction_group& tg,
                               daemon* d)
{
    send_wound(id, nonce, WOUND_XACT_ABORT, tg, d);
}

void
lock_state :: send_response(comm_id id, uint64_t nonce,
                            const transaction_group& tg, daemon* d)
{
    if (id == comm_id())
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " dropping response to null id";
        return;
    }

    configuration* c = d->get_config();
    replica_set rs;

    if (!c->hash(d->m_us.dc, m_state_key.table, m_state_key.key, &rs))
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " dropping response to=" << id << " because hashing failed";
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_LK_RESP)
                    + sizeof(uint64_t)
                    + pack_size(tg)
                    + pack_size(rs);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RAW_LK_RESP << nonce << tg << rs;
    d->send(id, msg);
}
