// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_pending_transaction_write_h_
#define consus_client_pending_transaction_write_h_

// consus
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE
class transaction;

class pending_transaction_write : public pending
{
    public:
        pending_transaction_write(int64_t client_id,
                                  consus_returncode* status,
                                  transaction* xact,
                                  uint64_t slot,
                                  const char* table,
                                  const unsigned char* key, size_t key_sz,
                                  const unsigned char* value, size_t value_sz);
        virtual ~pending_transaction_write() throw ();

    public:
        virtual std::string describe();
        virtual void kickstart_state_machine(client* cl);
        virtual void handle_server_failure(client* cl, comm_id si);
        virtual void handle_server_disruption(client* cl, comm_id si);
        virtual void handle_busybee_op(client* cl,
                                       uint64_t nonce,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up);

    private:
        void send_request(client* cl);

    private:
        transaction* m_xact;
        server_selector m_ss;
        const uint64_t m_slot;
        std::string m_table;
        std::string m_key;
        std::string m_value;

    private:
        pending_transaction_write(const pending_transaction_write&);
        pending_transaction_write& operator = (const pending_transaction_write&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_transaction_write_h_
