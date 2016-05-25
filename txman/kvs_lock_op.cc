// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/network_msgtype.h"
#include "txman/configuration.h"
#include "txman/daemon.h"
#include "txman/kvs_lock_op.h"

using consus::kvs_lock_op;

kvs_lock_op :: kvs_lock_op(const uint64_t& sk)
    : m_state_key(sk)
    , m_mtx()
    , m_init(false)
    , m_finished(false)
    , m_client()
    , m_client_nonce()
    , m_tx_group()
    , m_tx_seqno()
    , m_tx_func()
{
}

kvs_lock_op :: ~kvs_lock_op() throw ()
{
}

const uint64_t&
kvs_lock_op :: state_key() const
{
    return m_state_key;
}

bool
kvs_lock_op :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return !m_init || m_finished;
}

void
kvs_lock_op :: doit(lock_t type, lock_op op, const e::slice& table, const e::slice& key, const transaction_id& txid, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_LOCK_OP)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key)
                    + pack_size(txid)
                    + pack_size(type)
                    + pack_size(op);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_LOCK_OP << m_state_key << table << key << txid << type << op;
    configuration* c = d->get_config();
    comm_id kvs = c->choose_kvs(d->m_us.dc);
    d->send(kvs, msg);
    po6::threads::mutex::hold hold(&m_mtx);
    m_init = true;
}

void
kvs_lock_op :: response(consus_returncode rc, daemon* d)
{
    transaction_group tx_group;
    uint64_t tx_seqno;
    void (transaction::*tx_func)(consus_returncode, uint64_t, daemon*);

    {
        po6::threads::mutex::hold hold(&m_mtx);
        m_finished = true;

        if (m_client != comm_id())
        {
            const size_t sz = BUSYBEE_HEADER_SIZE
                            + pack_size(CLIENT_RESPONSE)
                            + sizeof(uint64_t)
                            + pack_size(rc);
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
            msg->pack_at(BUSYBEE_HEADER_SIZE)
                << CLIENT_RESPONSE << m_client_nonce << rc;
            d->send(m_client, msg);
        }

        tx_group = m_tx_group;
        tx_seqno = m_tx_seqno;
        tx_func = m_tx_func;
    }

    if (tx_group != transaction_group())
    {
        daemon::transaction_map_t::state_reference tsr;
        transaction* xact = d->m_transactions.get_state(tx_group, &tsr);

        if (xact)
        {
            (*xact.*tx_func)(rc, tx_seqno, d);
        }
    }
}

void
kvs_lock_op :: callback_client(comm_id client, uint64_t nonce)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_client = client;
    m_client_nonce = nonce;
}

void
kvs_lock_op :: callback_transaction(const transaction_group& tg, uint64_t seqno,
                                    void (transaction::*func)(consus_returncode, uint64_t, daemon*))
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_tx_group = tg;
    m_tx_seqno = seqno;
    m_tx_func = func;
}
