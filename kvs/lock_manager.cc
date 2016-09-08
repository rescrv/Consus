// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// STL
#include <sstream>

// BusyBee
#include "busybee_constants.h"

// consus
#include "common/network_msgtype.h"
#include "kvs/configuration.h"
#include "kvs/daemon.h"
#include "kvs/lock_manager.h"
#include "kvs/replica_set.h"

using consus::lock_manager;

extern std::vector<std::string> split_by_newlines(std::string s);

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
                     const transaction_group& tg, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->enqueue_lock(id, nonce, tg, d);
}

void
lock_manager :: unlock(comm_id id, uint64_t nonce,
                       const e::slice& table, const e::slice& key,
                       const transaction_group& tg, daemon* d)
{
    lock_map_t::state_reference sr;
    lock_state* s = m_locks.get_or_create_state(table_key_pair(table, key), &sr);
    s->unlock(id, nonce, tg, d);
}

std::string
lock_manager :: debug_dump()
{
    std::ostringstream ostr;

    for (lock_map_t::iterator it(&m_locks); it.valid(); ++it)
    {
        lock_state* s = *it;
        std::string debug = s->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            ostr << s->logid() << ": " << lines[i] << "\n";
        }
    }

    return ostr.str();
}
