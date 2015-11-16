// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_pending_begin_transaction_h_
#define consus_client_pending_begin_transaction_h_

// consus
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE

class pending_begin_transaction : public pending
{
    public:
        pending_begin_transaction(int64_t client_id,
                                  consus_returncode* status,
                                  consus_transaction** xact);
        virtual ~pending_begin_transaction() throw ();

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
        consus_transaction** m_xact;
        server_selector m_ss;

    private:
        pending_begin_transaction(const pending_begin_transaction&);
        pending_begin_transaction& operator = (const pending_begin_transaction&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_begin_transaction_h_
