// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_lock_state_h_
#define consus_kvs_lock_state_h_

// STL
#include <list>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/compat.h>

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_group.h"
#include "kvs/table_key_pair.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class lock_state
{
    public:
        lock_state(const table_key_pair& tk);
        ~lock_state() throw ();

    public:
        table_key_pair state_key();
        bool finished();

    public:
        void enqueue_lock(comm_id id, uint64_t nonce,
                          const transaction_group& tg,
                          daemon* d);
        void unlock(comm_id id, uint64_t nonce,
                    const transaction_group& tg,
                    daemon* d);

    private:
        struct request;

    private:
        bool ensure_initialized(daemon* d);
        void send_wound(comm_id id, uint64_t nonce, uint8_t flags,
                        const transaction_group& tg,
                        daemon* d);
        void send_wound_drop(comm_id id, uint64_t nonce,
                             const transaction_group& tg,
                             daemon* d);
        void send_wound_abort(comm_id id, uint64_t nonce,
                              const transaction_group& tg,
                              daemon* d);
        void send_response(comm_id id, uint64_t nonce,
                           const transaction_group& tg,
                           daemon* d);

    private:
        const table_key_pair m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        transaction_group m_holder;
        std::list<request> m_reqs;

    private:
        lock_state(const lock_state&);
        lock_state& operator = (const lock_state&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_lock_state_h_
