// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_transaction_h_
#define consus_client_transaction_h_

// C
#include <stdint.h>

// e
#include <e/error.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/transaction_id.h"

BEGIN_CONSUS_NAMESPACE
class client;

class transaction
{
    public:
        transaction(client* cl, const transaction_id& txid,
                    const comm_id* ids, size_t ids_sz);
        ~transaction() throw ();

    public:
        transaction_id txid() { return m_txid; }
        client* parent() { return m_cl; }
        int64_t get(const char* table,
                    const char* key, size_t key_sz,
                    consus_returncode* status,
                    char** value, size_t* value_sz);
        int64_t put(const char* table,
                    const char* key, size_t key_sz,
                    const char* value, size_t value_sz,
                    consus_returncode* status);
        int64_t commit(consus_returncode* status);
        int64_t abort(consus_returncode* status);
        void initialize(server_selector* ss);
        void mark_aborted();

    private:
        client* const m_cl;
        const transaction_id m_txid;
        const std::vector<comm_id> m_ids;
        uint64_t m_next_slot;

    private:
        transaction(const transaction&);
        transaction& operator = (const transaction&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_transaction_h_
