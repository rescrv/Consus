// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_read_replicator_h_
#define consus_kvs_read_replicator_h_

// po6
#include <po6/threads/mutex.h>

// e
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class read_replicator
{
    public:
        read_replicator(uint64_t key);
        virtual ~read_replicator() throw ();

    public:
        uint64_t state_key();
        bool finished();

    public:
        void init(comm_id id, uint64_t nonce,
                  const e::slice& table, const e::slice& key,
                  std::auto_ptr<e::buffer> backing);
        void response(comm_id id, consus_returncode rc,
                      uint64_t timestamp, const e::slice& value,
                      const replica_set& rs,
                      std::auto_ptr<e::buffer> backing, daemon* d);
        void externally_work_state_machine(daemon* d);

    private:
        struct read_stub;

    private:
        std::string logid();
        read_stub* get_stub(comm_id id);
        void work_state_machine(daemon* d);
        bool returncode_is_final(consus_returncode rc);
        void send_read_request(read_stub* stub, uint64_t now, daemon* d);

    private:
        const uint64_t m_state_key;
        po6::threads::mutex m_mtx;
        bool m_init;
        bool m_finished;
        comm_id m_id;
        uint64_t m_nonce;
        e::slice m_table;
        e::slice m_key;
        std::auto_ptr<e::buffer> m_kbacking;
        consus_returncode m_status;
        e::slice m_value;
        std::auto_ptr<e::buffer> m_vbacking;
        uint64_t m_timestamp;
        std::vector<read_stub> m_requests;
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_read_replicator_h_
