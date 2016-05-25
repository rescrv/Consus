// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_txman_kvs_read_h_
#define consus_txman_kvs_read_h_

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

BEGIN_CONSUS_NAMESPACE
class daemon;

class kvs_read
{
    public:
        kvs_read(const uint64_t& sk);
        ~kvs_read() throw ();

    public:
        const uint64_t& state_key() const;
        bool finished();

    public:
        void read(const e::slice& table, const e::slice& key,
                  uint64_t timestamp, daemon* d);
        void response(consus_returncode rc,
                      uint64_t timestamp,
                      const e::slice& value,
                      daemon* d);
        void callback_client(comm_id client, uint64_t nonce);
        void callback_transaction(const transaction_group& tg, uint64_t seqno,
                                  void (transaction::*func)(consus_returncode,
                                                            uint64_t,
                                                            const e::slice&,
                                                            uint64_t, daemon*));

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
        void (transaction::*m_tx_func)(consus_returncode, uint64_t, const e::slice&, uint64_t, daemon*);

    private:
        kvs_read(const kvs_read&);
        kvs_read& operator = (const kvs_read&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_kvs_read_h_
