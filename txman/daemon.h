// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_txman_daemon_h_
#define consus_txman_daemon_h_

// STL
#include <algorithm>
#include <string>

// po6
#include <po6/net/location.h>
#include <po6/threads/thread.h>

// e
#include <e/compat.h>
#include <e/garbage_collector.h>
#include <e/serialization.h>
#include <e/state_hash_table.h>

// BusyBee
#include <busybee_mta.h>

// Replicant
#include <replicant.h>

// consus
#include "namespace.h"
#include "common/coordinator_link.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "common/transaction_id.h"
#include "common/transaction_group.h"
#include "common/txman.h"
#include "txman/configuration.h"
#include "txman/durable_log.h"
#include "txman/global_voter.h"
#include "txman/local_voter.h"
#include "txman/mapper.h"
#include "txman/transaction.h"

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
        struct durable_msg;
        struct durable_cb;
        typedef e::state_hash_table<transaction_group, transaction> transaction_map_t;
        typedef e::state_hash_table<transaction_group, local_voter> local_voter_map_t;
        typedef e::state_hash_table<transaction_group, global_voter> global_voter_map_t;
        typedef e::nwf_hash_map<transaction_group, uint64_t, transaction_group::hash> disposition_map_t;
        typedef std::vector<durable_msg> durable_msg_heap_t;
        typedef std::vector<durable_cb> durable_cb_heap_t;
        friend class mapper;
        friend class transaction;
        friend class local_voter;
        friend class global_voter;

    private:
        void loop(size_t thread);
        void process_begin(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_read(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_write(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_commit(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_abort(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_paxos_2a(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_paxos_2b(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_lv_vote_1a(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_lv_vote_1b(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_lv_vote_2a(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_lv_vote_2b(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_lv_vote_learn(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_commit_record(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_gv_propose(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_gv_vote_1a(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_gv_vote_1b(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_gv_vote_2a(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_gv_vote_2b(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_kvs_rd_locked(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_kvs_rd_unlocked(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_kvs_wr_begun(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_kvs_wr_finished(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up);

    private:
        configuration* get_config();
        void debug_dump();
        transaction_id generate_txid();
        uint64_t resend_interval() { return PO6_SECONDS; }
        bool send(comm_id id, std::auto_ptr<e::buffer> msg);
        unsigned send(paxos_group_id g, std::auto_ptr<e::buffer> msg);
        unsigned send(const paxos_group& g, std::auto_ptr<e::buffer> msg);
        void send_when_durable(const std::string& entry, comm_id id, std::auto_ptr<e::buffer> msg);
        void send_when_durable(const std::string& entry, comm_id* id, e::buffer** msg, size_t sz);
        void callback_when_durable(const std::string& entry, const transaction_group& tg, uint64_t seqno);
        void durable();

    private:
        txman m_us;
        e::garbage_collector m_gc;
        mapper m_busybee_mapper;
        std::auto_ptr<busybee_mta> m_busybee;
        std::auto_ptr<coordinator_callback> m_coord_cb;
        std::auto_ptr<coordinator_link> m_coord;
        configuration* m_config;
        std::vector<e::compat::shared_ptr<po6::threads::thread> > m_threads;
        transaction_map_t m_transactions;
        local_voter_map_t m_local_voters;
        global_voter_map_t m_global_voters;
        disposition_map_t m_dispositions;
        durable_log m_log;

        // awaiting durability
        po6::threads::thread m_durable_thread;
        po6::threads::mutex m_durable_mtx;
        int64_t m_durable_up_to;
        durable_msg_heap_t m_durable_msgs;
        durable_cb_heap_t m_durable_cbs;

    private:
        daemon(const daemon&);
        daemon& operator = (const daemon&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_daemon_h_
