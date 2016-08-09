// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_txman_kvs_lock_op_h_
#define consus_txman_kvs_lock_op_h_

// STL
#include <memory>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/buffer.h>
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_id.h"

BEGIN_CONSUS_NAMESPACE
class daemon;
class transaction;

class kvs_lock_op
{
    public:
        kvs_lock_op(const uint64_t& sk);
        ~kvs_lock_op() throw ();

    public:
        const uint64_t& state_key() const;
        bool finished();

    public:
        void doit(lock_op op,
                  const e::slice& table, const e::slice& key,
                  const transaction_id& txid, daemon* d);
        void response(consus_returncode rc, daemon* d);
        void callback_client(comm_id client, uint64_t nonce);
        void callback_transaction(const transaction_group& tg, uint64_t seqno,
                                  void (transaction::*func)(consus_returncode, uint64_t, daemon*));

    private:
        const uint64_t m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        bool m_finished;
        // client callback
        comm_id m_client;
        uint64_t m_client_nonce;
        // transaction callback
        transaction_group m_tx_group;
        uint64_t m_tx_seqno;
        void (transaction::*m_tx_func)(consus_returncode, uint64_t, daemon*);

    private:
        kvs_lock_op(const kvs_lock_op&);
        kvs_lock_op& operator = (const kvs_lock_op&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_kvs_lock_op_h_
