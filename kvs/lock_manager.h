// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_lock_manager_h_
#define consus_kvs_lock_manager_h_

// e
#include <e/compat.h>

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/lock.h"
#include "common/transaction_group.h"
#include "kvs/lock_state.h"
#include "kvs/table_key_pair.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class lock_manager
{
    public:
        lock_manager(e::garbage_collector* gc);
        ~lock_manager() throw ();

    public:
        void lock(comm_id id, uint64_t nonce,
                  const e::slice& table, const e::slice& key,
                  const transaction_group& tg, daemon* d);
        void unlock(comm_id id, uint64_t nonce,
                    const e::slice& table, const e::slice& key,
                    const transaction_group& tg, daemon* d);
        std::string debug_dump();

    private:
        typedef e::state_hash_table<table_key_pair, lock_state> lock_map_t;

    private:
        lock_map_t m_locks;

    private:
        lock_manager(const lock_manager&);
        lock_manager& operator = (const lock_manager&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_lock_manager_h_
