// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_daemon_h_
#define consus_kvs_daemon_h_

// STL
#include <string>

// LevelDB
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

// po6
#include <po6/net/location.h>
#include <po6/threads/thread.h>

// e
#include <e/compat.h>
#include <e/garbage_collector.h>

// BusyBee
#include <busybee_mta.h>

// consus
#include "namespace.h"
#include "common/coordinator_link.h"
#include "common/kvs.h"
#include "kvs/configuration.h"
#include "kvs/mapper.h"

BEGIN_CONSUS_NAMESPACE

class daemon
{
    public:
        daemon();
        ~daemon() throw ();

    public:
        int run(bool daemonize,
                std::string data,
                std::string log,
                std::string pidfile,
                bool has_pidfile,
                bool set_bind_to,
                po6::net::location bind_to,
                bool set_coordinator,
                const char* coordinator,
                unsigned threads);

    private:
        struct coordinator_callback;
        struct comparator;
        friend class mapper;

    private:
        void loop(size_t thread);
        void process_read_lock(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_read_unlock(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_write_begin(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_write_finish(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_write_cancel(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);

    private:
        configuration* get_config();
        void debug_dump();
        bool send(comm_id id, std::auto_ptr<e::buffer> msg);

    private:
        kvs m_us;
        e::garbage_collector m_gc;
        mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        std::auto_ptr<coordinator_callback> m_coord_cb;
        std::auto_ptr<coordinator_link> m_coord;
        configuration* m_config;
        std::vector<e::compat::shared_ptr<po6::threads::thread> > m_threads;
        std::auto_ptr<comparator> m_cmp;
        const leveldb::FilterPolicy* m_bf;
        leveldb::DB* m_db;

    private:
        daemon(const daemon&);
        daemon& operator = (const daemon&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_daemon_h_
