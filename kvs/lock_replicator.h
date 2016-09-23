// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_lock_replicator_h_
#define consus_kvs_lock_replicator_h_

// po6
#include <po6/threads/mutex.h>

// e
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_group.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class lock_replicator
{
    public:
        lock_replicator(uint64_t key);
        virtual ~lock_replicator() throw ();

    public:
        uint64_t state_key();
        bool finished();

    public:
        void init(comm_id id, uint64_t nonce,
                  const e::slice& table, const e::slice& key,
                  const transaction_group& tg, lock_op op,
                  std::auto_ptr<e::buffer> backing);
        void response(comm_id id, const transaction_group& tg,
                      const replica_set& rs, daemon* d);
        void abort(const transaction_group& tg, daemon* d);
        void drop(const transaction_group& tg);
        void externally_work_state_machine(daemon* d);
        std::string debug_dump();

    private:
        struct lock_stub;

    private:
        std::string logid();
        lock_stub* get_stub(comm_id id);
        lock_stub* get_or_create_stub(comm_id id);
        void ensure_stub_exists(comm_id id) { get_or_create_stub(id); }
        void work_state_machine(daemon* d);
        void send_lock_request(lock_stub* stub, uint64_t now, daemon* d);

    private:
        const uint64_t m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        bool m_finished;
        comm_id m_id;
        uint64_t m_nonce;
        e::slice m_table;
        e::slice m_key;
        transaction_group m_tg;
        lock_op m_op;
        std::auto_ptr<e::buffer> m_backing;
        std::vector<lock_stub> m_requests;
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_lock_replicator_h_
