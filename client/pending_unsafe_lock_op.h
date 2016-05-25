// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_pending_unsafe_lock_op_h_
#define consus_client_pending_unsafe_lock_op_h_

// consus
#include "common/lock.h"
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE
class transaction;

class pending_unsafe_lock_op : public pending
{
    public:
        pending_unsafe_lock_op(int64_t client_id,
                            consus_returncode* status,
                            const char* table,
                            const unsigned char* key, size_t key_sz,
                            lock_op op);
        virtual ~pending_unsafe_lock_op() throw ();

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
        server_selector m_ss;
        std::string m_table;
        std::string m_key;
        lock_op m_op;

    private:
        pending_unsafe_lock_op(const pending_unsafe_lock_op&);
        pending_unsafe_lock_op& operator = (const pending_unsafe_lock_op&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_unsafe_lock_op_h_
