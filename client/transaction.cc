// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// treadstone
#include <treadstone.h>

// consus
#include "client/client.h"
#include "client/transaction.h"
#include "client/pending_transaction_read.h"
#include "client/pending_transaction_write.h"
#include "client/pending_transaction_commit.h"
#include "client/pending_transaction_abort.h"

#define ERROR(CODE) \
    *status = CONSUS_ ## CODE; \
    m_cl->set_error_message()->set_loc(__FILE__, __LINE__); \
    m_cl->set_error_message()->set_msg()

using consus::transaction;

transaction :: transaction(client* cl, const transaction_id& txid,
                           const comm_id* ids, size_t ids_sz)
    : m_cl(cl)
    , m_txid(txid)
    , m_ids(ids, ids + ids_sz)
    , m_next_slot(1)
{
}

transaction :: ~transaction() throw ()
{
}

int64_t
transaction :: get(const char* table,
                   const char* key, size_t key_sz,
                   consus_returncode* status,
                   char** value, size_t* value_sz)
{
    if (!m_cl->maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    uint64_t slot = m_next_slot;
    ++m_next_slot;
    int64_t client_id = m_cl->generate_new_client_id();
    pending* p = new pending_transaction_read(client_id, status, this, slot,
            table, binkey, binkey_sz, value, value_sz);
    free(binkey);
    p->kickstart_state_machine(m_cl);
    return client_id;
}

int64_t
transaction :: put(const char* table,
                   const char* key, size_t key_sz,
                   const char* value, size_t value_sz,
                   consus_returncode* status)
{
    if (!m_cl->maintain_coord_connection(status))
    {
        return -1;
    }

    unsigned char* binkey = NULL;
    size_t binkey_sz = 0;
    unsigned char* binval = NULL;
    size_t binval_sz = 0;

    if (treadstone_json_sz_to_binary(key, key_sz, &binkey, &binkey_sz) < 0)
    {
        ERROR(INVALID) << "key contains invalid JSON";
        return -1;
    }

    if (treadstone_json_sz_to_binary(value, value_sz, &binval, &binval_sz) < 0)
    {
        ERROR(INVALID) << "value contains invalid JSON";
        free(binkey);
        return -1;
    }

    uint64_t slot = m_next_slot;
    ++m_next_slot;
    int64_t client_id = m_cl->generate_new_client_id();
    pending* p = new pending_transaction_write(client_id, status, this, slot,
            table, binkey, binkey_sz, binval, binval_sz);
    free(binkey);
    free(binval);
    p->kickstart_state_machine(m_cl);
    return client_id;
}

int64_t
transaction :: commit(consus_returncode* status)
{
    if (!m_cl->maintain_coord_connection(status))
    {
        return -1;
    }

    uint64_t slot = m_next_slot;
    ++m_next_slot;
    int64_t client_id = m_cl->generate_new_client_id();
    pending* p = new pending_transaction_commit(client_id, status, this, slot);
    p->kickstart_state_machine(m_cl);
    return client_id;
}

int64_t
transaction :: abort(consus_returncode* status)
{
    if (!m_cl->maintain_coord_connection(status))
    {
        return -1;
    }

    uint64_t slot = m_next_slot;
    ++m_next_slot;
    int64_t client_id = m_cl->generate_new_client_id();
    pending* p = new pending_transaction_abort(client_id, status, this, slot);
    p->kickstart_state_machine(m_cl);
    return client_id;
}

void
transaction :: initialize(server_selector* ss)
{
    ss->set(&m_ids[0], m_ids.size());
}

void
transaction :: mark_aborted()
{
    ::abort(); // XXX
}
