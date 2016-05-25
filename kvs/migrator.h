// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_migrator_h_
#define consus_kvs_migrator_h_

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

class migrator
{
    public:
        migrator(partition_id key);
        virtual ~migrator() throw ();

    public:
        partition_id state_key();
        bool finished();

    public:
        void ack(version_id version, daemon* d);
        void externally_work_state_machine(daemon* d);
        void terminate();

    private:
        enum state_t
        {
            UNINITIALIZED,
            CHECK_CONFIG,
            TRANSFER_DATA,
            TERMINATED
        };

    private:
        void ensure_initialized(daemon* d);
        void work_state_machine(daemon* d);
        void work_state_machine_check_config(daemon* d);
        void work_state_machine_transfer_data(daemon* d);

    private:
        const partition_id m_state_key;
        po6::threads::mutex m_mtx;
        version_id m_version;
        state_t m_state;
        uint64_t m_last_handshake;
        uint64_t m_last_coord_call;
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_migrator_h_
