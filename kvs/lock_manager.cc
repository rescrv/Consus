// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// BusyBee
#include "busybee_constants.h"

// consus
#include "common/network_msgtype.h"
#include "kvs/configuration.h"
#include "kvs/daemon.h"
#include "kvs/lock_manager.h"
#include "kvs/replica_set.h"

using consus::lock_manager;

lock_manager :: lock_manager(e::garbage_collector* gc)
    : m_locks(gc)
{
}

lock_manager :: ~lock_manager() throw ()
{
}

void
lock_manager :: lock(comm_id id, uint64_t nonce,
                     const e::slice& table, const e::slice& key,
                     const transaction_id& txid, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->enqueue_lock(id, nonce, txid, d);
}

void
lock_manager :: unlock(comm_id id, uint64_t nonce,
                       const e::slice& table, const e::slice& key,
                       const transaction_id& txid, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->unlock(id, nonce, txid, d);
}
