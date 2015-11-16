// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_pending_transaction_commit_h_
#define consus_client_pending_transaction_commit_h_

// consus
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE
class transaction;

class pending_transaction_commit : public pending
{
    public:
        pending_transaction_commit(int64_t client_id,
                                   consus_returncode* status,
                                   transaction* xact,
                                   uint64_t slot);
        virtual ~pending_transaction_commit() throw ();

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

    private:
        pending_transaction_commit(const pending_transaction_commit&);
        pending_transaction_commit& operator = (const pending_transaction_commit&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_transaction_commit_h_
