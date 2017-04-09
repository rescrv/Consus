// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define __STDC_LIMIT_MACROS

// C
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

// POSIX
#include <signal.h>
#include <sys/stat.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// po6
#include <po6/errno.h>
#include <po6/io/fd.h>
#include <po6/path.h>

// e
#include <e/atomic.h>
#include <e/daemon.h>
#include <e/daemonize.h>
#include <e/identity.h>
#include <e/serialization.h>
#include <e/strescape.h>

// consus
#include "common/coordinator_returncode.h"
#include "common/generate_token.h"
#include "common/macros.h"
#include "common/util.h"
#include "txman/daemon.h"
#include "txman/log_entry_t.h"

using consus::daemon;

// XXX each and every BUSYBEE_DISRUPTED event must trigger associated retries or
// cleanups.  Most notably in the kvs_* functions

#define CHECK_UNPACK(MSGTYPE, UNPACKER) \
    do \
    { \
        if (UNPACKER.error()) \
        { \
            network_msgtype CONCAT(_anon, __LINE__)(MSGTYPE); \
            LOG(WARNING) << "received corrupt \"" \
                         << CONCAT(_anon, __LINE__) << "\" message"; \
            return; \
        } \
    } while (0)

uint32_t s_interrupts = 0;
bool s_debug_dump = false;
bool s_debug_mode = false;

static void
exit_on_signal(int /*signum*/)
{
    RAW_LOG(ERROR, "interrupted: exiting");
    e::atomic::increment_32_nobarrier(&s_interrupts, 1);
}

static void
handle_debug_dump(int /*signum*/)
{
    s_debug_dump = true;
}

static void
handle_debug_mode(int /*signum*/)
{
    s_debug_mode = !s_debug_mode;
}

struct daemon::coordinator_callback : public coordinator_link::callback
{
    coordinator_callback(daemon* d);
    virtual ~coordinator_callback() throw ();
    virtual std::string prefix() { return "txman"; }
    virtual bool new_config(const char* data, size_t data_sz);
    virtual bool has_id(comm_id id);
    virtual po6::net::location address(comm_id id);
    virtual bool is_steady_state(comm_id id);

    private:
        daemon* d;
        coordinator_callback(const coordinator_callback&);
        coordinator_callback& operator = (const coordinator_callback&);
};

daemon :: coordinator_callback :: coordinator_callback(daemon* _d)
    : d(_d)
{
}

daemon :: coordinator_callback :: ~coordinator_callback() throw ()
{
}

bool
daemon :: coordinator_callback :: new_config(const char* data, size_t data_sz)
{
    std::auto_ptr<configuration> c(new configuration());
    e::unpacker up(data, data_sz);
    up = up >> *c;

    if (up.error() || up.remain())
    {
        LOG(ERROR) << "received a bad configuration";
        return false;
    }

    configuration* old_config = d->get_config();
    d->m_us.dc = c->get_data_center(d->m_us.id);
    e::atomic::store_ptr_release(&d->m_config, c.release());
    d->m_gc.collect(old_config, e::garbage_collector::free_ptr<configuration>);
    LOG(INFO) << "updating to configuration " << d->get_config()->version();
    std::vector<paxos_group_id> gs = d->get_config()->groups_for(d->m_us.id);

#if 0
    if (s_debug_mode)
    {
        std::string debug = d->get_config()->dump();
        std::vector<std::string> lines = split_by_newlines(debug);
        LOG(INFO) << "=== begin debug dump of configuration ===";

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << lines[i];
        }

        LOG(INFO) << "active paxos groups for this transaction-manager:";

        for (size_t i = 0; i < gs.size(); ++i)
        {
            LOG(INFO) << gs[i];
        }

        LOG(INFO) << "===  end debug dump of configuration  ===";
    }
#endif

    return true;
}

bool
daemon :: coordinator_callback :: has_id(comm_id id)
{
    configuration* c = d->get_config();

    if (c)
    {
        return c->exists(id);
    }

    return false;
}

po6::net::location
daemon :: coordinator_callback :: address(comm_id id)
{
    configuration* c = d->get_config();

    if (c)
    {
        return c->get_address(id);
    }

    return po6::net::location();
}

bool
daemon :: coordinator_callback :: is_steady_state(comm_id id)
{
    configuration* c = d->get_config();

    if (c)
    {
        return c->get_state(id) == txman_state::ONLINE;
    }

    return false;
}

daemon :: daemon()
    : m_us()
    , m_gc()
    , m_busybee_controller(this)
    , m_busybee()
    , m_coord_cb()
    , m_coord()
    , m_config(NULL)
    , m_threads()
    , m_transactions(&m_gc)
    , m_local_voters(&m_gc)
    , m_global_voters(&m_gc)
    , m_dispositions(&m_gc)
    , m_readers(&m_gc)
    , m_writers(&m_gc)
    , m_lock_ops(&m_gc)
    , m_log()
    , m_durable_thread(po6::threads::make_obj_func(&daemon::durable, this))
    , m_durable_mtx()
    , m_durable_up_to(-1)
    , m_durable_msgs()
    , m_durable_cbs()
    , m_pumping_thread(po6::threads::make_obj_func(&daemon::pump, this))
{
}

daemon :: ~daemon() throw ()
{
    m_gc.collect(get_config(), e::garbage_collector::free_ptr<configuration>);
}

int
daemon :: run(bool background,
              std::string data,
              std::string log,
              std::string pidfile,
              bool has_pidfile,
              bool set_bind_to,
              po6::net::location bind_to,
              bool set_coordinator,
              const char* coordinator,
              const char* data_center,
              unsigned threads)
{
    if (!e::block_all_signals())
    {
        std::cerr << "could not block signals; exiting" << std::endl;
        return EXIT_FAILURE;
    }

    if (!e::daemonize(background, log, "consus-txman-", pidfile, has_pidfile))
    {
        return EXIT_FAILURE;
    }

    if (!e::install_signal_handler(SIGHUP, exit_on_signal) ||
        !e::install_signal_handler(SIGINT, exit_on_signal) ||
        !e::install_signal_handler(SIGTERM, exit_on_signal) ||
        !e::install_signal_handler(SIGQUIT, exit_on_signal) ||
        !e::install_signal_handler(SIGUSR1, handle_debug_dump) ||
        !e::install_signal_handler(SIGUSR2, handle_debug_mode))
    {
        PLOG(ERROR) << "could not install signal handlers";
        return EXIT_FAILURE;
    }

    if (!m_log.open(data))
    {
        LOG(ERROR) << "could not open log: " << po6::strerror(m_log.error());
        return EXIT_FAILURE;
    }

    bool saved;
    uint64_t id;
    std::string rendezvous(coordinator);

    if (!e::load_identity(po6::path::join(data, "TXMAN").c_str(), &saved, &id,
                          set_bind_to, &bind_to, set_coordinator, &rendezvous))
    {
        LOG(ERROR) << "could not load prior identity; exiting";
        return EXIT_FAILURE;
    }

    m_us.id = comm_id(id);
    m_us.bind_to = bind_to;
    bool (coordinator_link::*coordfunc)();

    if (saved)
    {
        coordfunc = &coordinator_link::establish;
    }
    else
    {
        if (!generate_token(&id))
        {
            PLOG(ERROR) << "could not read random token from /dev/urandom";
            return EXIT_FAILURE;
        }

        m_us.id = comm_id(id);
        coordfunc = &coordinator_link::initial_registration;
    }

    m_coord_cb.reset(new coordinator_callback(this));
    m_coord.reset(new coordinator_link(rendezvous, m_us.id, m_us.bind_to, data_center, m_coord_cb.get()));
    m_coord->allow_reregistration();
    LOG(INFO) << "starting consus transaction-manager " << m_us.id
              << " on address " << m_us.bind_to;
    LOG(INFO) << "connecting to " << rendezvous;

    if (!(((*m_coord).*coordfunc)()))
    {
        return EXIT_FAILURE;
    }

    assert(get_config());

    if (!e::save_identity(po6::path::join(data, "TXMAN").c_str(), id, bind_to, rendezvous))
    {
        LOG(ERROR) << "could not save identity; exiting";
        return EXIT_FAILURE;
    }

    m_busybee.reset(busybee_server::create(&m_busybee_controller, id, bind_to, &m_gc));
    m_durable_thread.start();
    m_pumping_thread.start();

    for (size_t i = 0; i < threads; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> t(new thread(make_obj_func(&daemon::loop, this, i)));
        m_threads.push_back(t);
        t->start();
    }

    while (e::atomic::increment_32_nobarrier(&s_interrupts, 0) == 0)
    {
        bool debug_mode = s_debug_mode;
        m_coord->maintain_connection();

        if (m_coord->error())
        {
            break;
        }

        if (m_coord->orphaned())
        {
            LOG(ERROR) << "server removed from cluster; exiting";
            break;
        }

        if (s_debug_mode != debug_mode)
        {
            if (s_debug_mode)
            {
                debug_dump();
                LOG(INFO) << "enabling debug mode; will log all state transitions";
                s_debug_dump = false;
            }
            else
            {
                LOG(INFO) << "disabling debug mode; will go back to normal operation";
            }
        }

        if (s_debug_dump)
        {
            debug_dump();
            s_debug_dump = false;
        }
    }

    e::atomic::increment_32_nobarrier(&s_interrupts, 1);
    m_busybee->shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    if (s_debug_mode)
    {
        debug_dump();
    }

    m_log.close();
    m_pumping_thread.join();
    m_durable_thread.join();
    LOG(ERROR) << "consus is gracefully shutting down";
    return EXIT_SUCCESS;
}

void
daemon :: loop(size_t thread)
{
    LOG(INFO) << "network thread " << thread << " started";

    sigset_t ss;

    if (sigfillset(&ss) < 0 ||
        pthread_sigmask(SIG_SETMASK, &ss, NULL) < 0)
    {
        std::cerr << "could not block signals" << std::endl;
        return;
    }

    e::garbage_collector::thread_state ts;
    m_gc.register_thread(&ts);
    bool done = false;

    while (!done)
    {
        uint64_t _id;
        std::auto_ptr<e::buffer> msg;
        busybee_returncode rc = m_busybee->recv(&ts, -1, &_id, &msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
                done = true;
                continue;
            case BUSYBEE_DISRUPTED:
            case BUSYBEE_INTERRUPTED:
                continue;
            case BUSYBEE_TIMEOUT:
                continue;
            case BUSYBEE_SEE_ERRNO:
                PLOG(ERROR) << "receive error";
                continue;
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "internal invariants broken; crashing";
                abort();
        }

        comm_id id(_id);
        network_msgtype mt;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        if (up.error())
        {
            LOG(WARNING) << "dropping message that has a malformed header";

            if (s_debug_mode)
            {
                LOG(WARNING) << "here's some hex: " << msg->hex();
            }

            continue;
        }

#ifdef CONSUS_LOG_ALL_MESSAGES
        if (s_debug_mode)
        {
            memset(msg->data(), 0, BUSYBEE_HEADER_SIZE);
            LOG(INFO) << "recv<-" << id << " " << mt << " " << msg->b64();
        }
#endif
        const uint64_t start = po6::monotonic_time();

        switch (mt)
        {
            case TXMAN_BEGIN:
                process_begin(id, msg, up);
                break;
            case TXMAN_READ:
                process_read(id, msg, up);
                break;
            case TXMAN_WRITE:
                process_write(id, msg, up);
                break;
            case TXMAN_COMMIT:
                process_commit(id, msg, up);
                break;
            case TXMAN_ABORT:
                process_abort(id, msg, up);
                break;
            case TXMAN_WOUND:
                process_wound(id, msg, up);
                break;
            case TXMAN_PAXOS_2A:
                process_paxos_2a(id, msg, up);
                break;
            case TXMAN_PAXOS_2B:
                process_paxos_2b(id, msg, up);
                break;
            case LV_VOTE_1A:
                process_lv_vote_1a(id, msg, up);
                break;
            case LV_VOTE_1B:
                process_lv_vote_1b(id, msg, up);
                break;
            case LV_VOTE_2A:
                process_lv_vote_2a(id, msg, up);
                break;
            case LV_VOTE_2B:
                process_lv_vote_2b(id, msg, up);
                break;
            case LV_VOTE_LEARN:
                process_lv_vote_learn(id, msg, up);
                break;
            case COMMIT_RECORD:
                process_commit_record(id, msg, up);
                break;
            case GV_OUTCOME:
                process_gv_outcome(id, msg, up);
                break;
            case GV_PROPOSE:
                process_gv_propose(id, msg, up);
                break;
            case GV_VOTE_1A:
                process_gv_vote_1a(id, msg, up);
                break;
            case GV_VOTE_1B:
                process_gv_vote_1b(id, msg, up);
                break;
            case GV_VOTE_2A:
                process_gv_vote_2a(id, msg, up);
                break;
            case GV_VOTE_2B:
                process_gv_vote_2b(id, msg, up);
                break;
            case KVS_REP_RD_RESP:
                process_kvs_rep_rd_resp(id, msg, up);
                break;
            case KVS_REP_WR_RESP:
                process_kvs_rep_wr_resp(id, msg, up);
                break;
            case KVS_LOCK_OP_RESP:
                process_kvs_lock_op_resp(id, msg, up);
                break;
            case CONSUS_NOP:
                break;
            case CLIENT_RESPONSE:
            case KVS_REP_RD:
            case KVS_REP_WR:
            case KVS_RAW_RD:
            case KVS_RAW_RD_RESP:
            case KVS_RAW_WR:
            case KVS_RAW_WR_RESP:
            case KVS_LOCK_OP:
            case KVS_RAW_LK:
            case KVS_RAW_LK_RESP:
            case KVS_WOUND_XACT:
            case KVS_MIGRATE_SYN:
            case KVS_MIGRATE_ACK:
            default:
                LOG(INFO) << "received " << mt << " message which transaction-managers do not process";
                break;
        }

        const uint64_t end = po6::monotonic_time();
        LOG_IF(INFO, s_debug_mode && (end - start) > 100 * PO6_MILLIS) << mt << " took " << ((end - start) / 100 * PO6_MILLIS) << "ms";
        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "network thread shutting down";
}

void
daemon :: process_begin(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    up = up >> e::unpack_varint(nonce);
    CHECK_UNPACK(TXMAN_BEGIN, up);
    configuration* c = get_config();

    while (true)
    {
        transaction_id txid = generate_txid();
        const paxos_group* group = c->get_group(txid.group);

        if (!group)
        {
            LOG(ERROR) << "generated txid with invalid paxos group";
            // XXX reply with an error
            return;
        }

        transaction_group tg(txid);
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.create_state(tg, &tsr);

        if (!xact)
        {
            continue;
        }

        std::vector<paxos_group_id> dcs;

        if (!c->choose_groups(txid.group, &dcs))
        {
            LOG(ERROR) << "not enough dcs online";
            // XXX reply with an error
            return;
        }

        uint64_t ts = po6::wallclock_time();
        xact->begin(id, nonce, ts, *group, dcs, this);
        break;
    }
}

void
daemon :: process_read(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_id txid;
    uint64_t nonce;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    up = up >> txid
            >> e::unpack_varint(nonce)
            >> e::unpack_varint(seqno)
            >> table >> key;
    CHECK_UNPACK(TXMAN_READ, up);

    configuration* c = get_config();

    if (!c->get_group(txid.group))
    {
        LOG_IF(INFO, s_debug_mode) << "dropping read for " << txid
                                   << " because the group is not in the configuration";
        return;
    }

    if (!c->is_member(txid.group, m_us.id))
    {
        LOG_IF(INFO, s_debug_mode) << "dropping read for " << txid
                                   << " this server is not part of the group";
        return;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(transaction_group(txid), &tsr);
    assert(xact);
    xact->read(id, nonce, seqno, table, key, msg, this);
}

void
daemon :: process_write(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_id txid;
    uint64_t nonce;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    e::slice value;
    up = up >> txid
            >> e::unpack_varint(nonce)
            >> e::unpack_varint(seqno)
            >> table >> key >> value;
    CHECK_UNPACK(TXMAN_WRITE, up);

    if (!get_config()->get_group(txid.group))
    {
        LOG_IF(INFO, s_debug_mode) << "dropping it because " << txid.group
                                   << " not in configuration";
        return;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(transaction_group(txid), &tsr);
    assert(xact);
    xact->write(id, nonce, seqno, table, key, value, msg, this);
}

void
daemon :: process_commit(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_id txid;
    uint64_t nonce;
    uint64_t seqno;
    up = up >> txid
            >> e::unpack_varint(nonce)
            >> e::unpack_varint(seqno);
    CHECK_UNPACK(TXMAN_COMMIT, up);

    if (!get_config()->get_group(txid.group))
    {
        LOG_IF(INFO, s_debug_mode) << "dropping it because " << txid.group
                                   << " not in configuration";
        return;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(transaction_group(txid), &tsr);
    assert(xact);
    xact->prepare(id, nonce, seqno, this);
}

void
daemon :: process_abort(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_id txid;
    uint64_t nonce;
    uint64_t seqno;
    up = up >> txid
            >> e::unpack_varint(nonce)
            >> e::unpack_varint(seqno);
    CHECK_UNPACK(TXMAN_ABORT, up);

    if (!get_config()->get_group(txid.group))
    {
        LOG_IF(INFO, s_debug_mode) << "dropping it because " << txid.group
                                   << " not in configuration";
        return;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(transaction_group(txid), &tsr);
    assert(xact);
    xact->abort(id, nonce, seqno, this);
}

void
daemon :: process_wound(comm_id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    up = up >> tg;
    CHECK_UNPACK(TXMAN_WOUND, up);

    if (get_config()->is_member(tg.group, m_us.id))
    {
        LOG_IF(INFO, s_debug_mode) << transaction_group::log(tg) << " wounding local transaction";
        // wound the local voter
        local_voter_map_t::state_reference lvsr;
        local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
        assert(lv);
        lv->wound(this);
        // wound the global voter
        global_voter_map_t::state_reference gvsr;
        global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
        assert(gv);
        // XXX gv->wound(this);
        // work the transaction
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
    else
    {
        LOG_IF(INFO, s_debug_mode) << transaction_group::log(tg) << " forwarding wound message to group";
        send(tg.group, msg);
    }
}

void
daemon :: process_paxos_2a(comm_id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    e::slice log_entry;
    up = up >> log_entry;
    CHECK_UNPACK(TXMAN_PAXOS_2A, up);
    log_entry_t t = LOG_ENTRY_NOP;
    transaction_group tg;
    uint64_t seqno = 0;
    up = e::unpacker(log_entry) >> t >> tg >> seqno;

    if (up.error() || !is_paxos_2a_log_entry(t))
    {
        LOG(ERROR) << "dropping corrupt paxos 2A log entry";

        if (s_debug_mode)
        {
            LOG(ERROR) << "here's some hex: " << msg->hex();
        }

        return;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(tg, &tsr);
    assert(xact);
    xact->paxos_2a(seqno, t, up, msg, this);
}

void
daemon :: process_paxos_2b(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    up = up >> tg >> seqno;
    CHECK_UNPACK(TXMAN_PAXOS_2B, up);
    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(tg, &tsr);
    assert(xact);
    xact->paxos_2b(id, seqno, this);
}

void
daemon :: process_lv_vote_1a(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    paxos_synod::ballot b;
    up = up >> tg >> idx >> b;
    CHECK_UNPACK(LV_VOTE_1A, up);
    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_1a(id, idx, b, this);
}

void
daemon :: process_lv_vote_1b(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    paxos_synod::ballot b;
    paxos_synod::pvalue p;
    up = up >> tg >> idx >> b >> p;
    CHECK_UNPACK(LV_VOTE_1B, up);
    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_1b(id, idx, b, p, this);
}

void
daemon :: process_lv_vote_2a(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    paxos_synod::pvalue p;
    up = up >> tg >> idx >> p;
    CHECK_UNPACK(LV_VOTE_2A, up);
    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_2a(id, idx, p, this);
}

void
daemon :: process_lv_vote_2b(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    paxos_synod::pvalue p;
    up = up >> tg >> idx >> p;
    CHECK_UNPACK(LV_VOTE_2B, up);
    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_2b(id, idx, p, this);
}

void
daemon :: process_lv_vote_learn(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    uint64_t v;
    up = up >> tg >> idx >> v;
    CHECK_UNPACK(LV_VOTE_LEARN, up);
    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_learn(idx, v, this);
    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_state(tg, &tsr);

    if (xact)
    {
        xact->externally_work_state_machine(this);
    }
    else
    {
        LOG(INFO) << transaction_group::log(tg) + " data center voter: dropped learn from=" << id;
    }
}

void
daemon :: process_commit_record(comm_id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    e::slice commit_record;
    up = up >> tg >> commit_record;
    CHECK_UNPACK(COMMIT_RECORD, up);
    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(tg, &tsr);
    assert(xact);
    xact->commit_record(commit_record, msg, this);
}

void
daemon :: process_gv_outcome(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint64_t index;
    uint64_t outcomes[CONSUS_MAX_REPLICATION_FACTOR];
    up = up >> tg >> e::unpack_varint(index) >> e::unpack_array<uint64_t>(outcomes, CONSUS_MAX_REPLICATION_FACTOR);
    CHECK_UNPACK(GV_OUTCOME, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->report(index, outcomes, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_gv_propose(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    generalized_paxos::command c;
    up = up >> tg >> c;
    CHECK_UNPACK(GV_PROPOSE, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->propose(c, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_gv_vote_1a(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    generalized_paxos::message_p1a m;
    up = up >> tg >> m;
    CHECK_UNPACK(GV_VOTE_1A, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->process_p1a(id, m, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_gv_vote_1b(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    generalized_paxos::message_p1b m;
    up = up >> tg >> m;
    CHECK_UNPACK(GV_VOTE_1B, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->process_p1b(m, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_gv_vote_2a(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    generalized_paxos::message_p2a m;
    up = up >> tg >> m;
    CHECK_UNPACK(GV_VOTE_2A, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->process_p2a(id, m, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_gv_vote_2b(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    generalized_paxos::message_p2b m;
    up = up >> tg >> m;
    CHECK_UNPACK(GV_VOTE_2B, up);

    global_voter_map_t::state_reference gvsr;
    global_voter* gv = m_global_voters.get_or_create_state(tg, &gvsr);
    assert(gv);

    if (gv->process_p2b(m, this))
    {
        transaction_map_t::state_reference tsr;
        transaction* xact = m_transactions.get_state(tg, &tsr);

        if (xact)
        {
            xact->externally_work_state_machine(this);
        }
    }
}

void
daemon :: process_kvs_rep_rd_resp(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    consus_returncode rc;
    uint64_t timestamp;
    e::slice value;
    up = up >> nonce >> rc >> timestamp >> value;
    CHECK_UNPACK(KVS_REP_RD_RESP, up);

    read_map_t::state_reference ksr;
    kvs_read* kv = m_readers.get_state(nonce, &ksr);

    if (kv)
    {
        kv->response(rc, timestamp, value, this);
    }
    else
    {
        LOG(INFO) << "dropped read response from=" << id << " nonce=" << nonce;
    }
}

void
daemon :: process_kvs_rep_wr_resp(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    consus_returncode rc;
    up = up >> nonce >> rc;
    CHECK_UNPACK(KVS_REP_WR_RESP, up);

    write_map_t::state_reference ksr;
    kvs_write* kv = m_writers.get_state(nonce, &ksr);

    if (kv)
    {
        kv->response(rc, this);
    }
    else
    {
        LOG(INFO) << "dropped write response from=" << id << " nonce=" << nonce;
    }
}

void
daemon :: process_kvs_lock_op_resp(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    consus_returncode rc;
    up = up >> nonce >> rc;
    CHECK_UNPACK(KVS_LOCK_OP_RESP, up);

    lock_op_map_t::state_reference ksr;
    kvs_lock_op* kv = m_lock_ops.get_state(nonce, &ksr);

    if (kv)
    {
        kv->response(rc, this);
    }
    else
    {
        LOG(INFO) << "dropped lock op response from=" << id << " nonce=" << nonce;
    }
}

consus::kvs_read*
daemon :: create_read(read_map_t::state_reference* sr)
{
    while (true)
    {
        uint64_t kv_nonce = generate_nonce();

        if (kv_nonce == 0)
        {
            continue;
        }

        kvs_read* kv = m_readers.create_state(kv_nonce, sr);

        if (kv)
        {
            return kv;
        }
    }
}

consus::kvs_write*
daemon :: create_write(write_map_t::state_reference* sr)
{
    while (true)
    {
        uint64_t kv_nonce = generate_nonce();

        if (kv_nonce == 0)
        {
            continue;
        }

        kvs_write* kv = m_writers.create_state(kv_nonce, sr);

        if (kv)
        {
            return kv;
        }
    }
}

consus::kvs_lock_op*
daemon :: create_lock_op(lock_op_map_t::state_reference* sr)
{
    while (true)
    {
        uint64_t kv_nonce = generate_nonce();

        if (kv_nonce == 0)
        {
            continue;
        }

        kvs_lock_op* kv = m_lock_ops.create_state(kv_nonce, sr);

        if (kv)
        {
            return kv;
        }
    }
}

consus::configuration*
daemon :: get_config()
{
    return e::atomic::load_ptr_acquire(&m_config);
}

void
daemon :: debug_dump()
{
    // Intentionally collect std::string from other components and then
    // split/etc here so that we can add a consistent prefix and so that it'll
    // show up in the log nicely indented.  Factoring it into a new function
    // would break this for relatively little savings.
    LOG(INFO) << "=============================== Begin Debug Dump ===============================";
    LOG(INFO) << "this host: " << m_us;
    LOG(INFO) << "configuration version: " << get_config()->version().get();
    LOG(INFO) << "note that entries can appear multiple times in the following tables";
    LOG(INFO) << "this is a natural consequence of not holding global locks during the dump";
    LOG(INFO) << "--------------------------------- Transactions ---------------------------------";

    for (transaction_map_t::iterator it(&m_transactions); it.valid(); ++it)
    {
        transaction* xact = *it;
        std::string debug = xact->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << xact->logid() << " " << lines[i];
        }
    }

    LOG(INFO) << "--------------------------------- Local Voters ---------------------------------";

    for (local_voter_map_t::iterator it(&m_local_voters); it.valid(); ++it)
    {
        local_voter* lv = *it;
        std::string debug = lv->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << lv->logid() << " " << lines[i];
        }
    }

    LOG(INFO) << "--------------------------------- Global Voters --------------------------------";

    for (global_voter_map_t::iterator it(&m_global_voters); it.valid(); ++it)
    {
        global_voter* gv = *it;
        std::string debug = gv->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << gv->logid() << " " << lines[i];
        }
    }

#if 0
    LOG(INFO) << "--------------------------------- Dispositions ---------------------------------";
    LOG(INFO) << "-------------------------------- Read Operations -------------------------------";
    LOG(INFO) << "------------------------------- Write Operations -------------------------------";
    LOG(INFO) << "-------------------------------- Lock Operations -------------------------------";
    LOG(INFO) << "---------------------------------- Durability ----------------------------------";
#endif
    LOG(INFO) << "================================ End Debug Dump ================================";
}

uint64_t
daemon :: generate_nonce()
{
    // XXX don't go out of process
    po6::io::fd fd(open("/dev/urandom", O_RDONLY));
    uint64_t x;
    int ret = fd.xread(&x, sizeof(x));
    assert(ret == 8);
    return x;
}

consus::transaction_id
daemon :: generate_txid()
{
    uint64_t x = generate_nonce();
    paxos_group_id id;
    std::vector<paxos_group_id> groups(get_config()->groups_for(m_us.id));
    // XXX groups.size() == 0?
    size_t idx = x % groups.size();
    id = groups[idx];
    return transaction_id(id, po6::wallclock_time(), x);
}

bool
daemon :: send(comm_id id, std::auto_ptr<e::buffer> msg)
{
#ifdef CONSUS_LOG_ALL_MESSAGES
    if (s_debug_mode)
    {
        network_msgtype mt;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        if (up.error())
        {
            LOG(ERROR) << "sending invalid message: " << msg->b64();
        }
        else
        {
            LOG(INFO) << "send->" << id << " " << mt << " " << msg->b64();
        }
    }
#endif

    if (id == comm_id())
    {
        LOG_IF(INFO, s_debug_mode) << "message not sent: dropped";
        return false;
    }

    if (id == m_us.id)
    {
        m_busybee->deliver(id.get(), msg);
        return true;
    }

    busybee_returncode rc = m_busybee->send(id.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            LOG_IF(INFO, s_debug_mode) << "message not sent: disrupted";
            return false;
        case BUSYBEE_SEE_ERRNO:
            PLOG(ERROR) << "receive error";
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_INTERRUPTED:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            LOG(ERROR) << "internal invariants broken; crashing";
            abort();
    }
}

unsigned
daemon :: send(paxos_group_id g, std::auto_ptr<e::buffer> msg)
{
    const paxos_group* group = get_config()->get_group(g);

    if (!group)
    {
        return 0;
    }

    return send(*group, msg);
}

unsigned
daemon :: send(const paxos_group& g, std::auto_ptr<e::buffer> msg)
{
    unsigned count = 0;

    for (unsigned i = 0; i < g.members_sz; ++i)
    {
        std::auto_ptr<e::buffer> m(msg->copy());

        if (g.members[i] == m_us.id)
        {
            m_busybee->deliver(g.members[i].get(), m);
            ++count;
            continue;
        }

        busybee_returncode rc = m_busybee->send(g.members[i].get(), m);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                ++count;
                break;
            case BUSYBEE_DISRUPTED:
                LOG_IF(INFO, s_debug_mode) << "message not sent to " << g.members[i];
                break;
            case BUSYBEE_SEE_ERRNO:
                PLOG(ERROR) << "receive error";
                break;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_INTERRUPTED:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "internal invariants broken; crashing";
                abort();
        }
    }

#ifdef CONSUS_LOG_ALL_MESSAGES
    if (s_debug_mode)
    {
        network_msgtype mt;
        e::unpacker up = msg->unpack_from(BUSYBEE_HEADER_SIZE);
        memset(msg->data(), 0, BUSYBEE_HEADER_SIZE);
        up = up >> mt;

        if (up.error())
        {
            LOG(ERROR) << "sending invalid message: " << msg->b64();
        }
        else
        {
            LOG(INFO) << "send->" << count << "/" << g.members_sz << " members of " <<  g.id << " " << mt << " " << msg->b64();
        }
    }
#endif

    return count;
}

struct daemon::durable_msg
{
    durable_msg() : recno(), client(), msg(NULL) {}
    durable_msg(int64_t r, comm_id c, e::buffer* m) : recno(r), client(c), msg(m) {}
    durable_msg(const durable_msg& other)
        : recno(other.recno), client(other.client), msg(other.msg) {}
    ~durable_msg() throw () {}
    durable_msg& operator = (const durable_msg& rhs)
    {
        // no self-assign check needed
        recno = rhs.recno;
        client = rhs.client;
        msg = rhs.msg;
        return *this;
    }
    bool operator < (const durable_msg& rhs) { return rhs.recno > recno; }
    int64_t recno;
    comm_id client;
    e::buffer* msg;
};

void
daemon :: send_when_durable(const std::string& entry, comm_id id, std::auto_ptr<e::buffer> msg)
{
    e::buffer* m = msg.release();
    send_when_durable(entry, &id, &m, 1);
}

void
daemon :: send_when_durable(const std::string& entry, const comm_id* ids, e::buffer** msgs, size_t sz)
{
    int64_t x = m_log.append(entry.data(), entry.size());
    send_when_durable(x, ids, msgs, sz);
}

void
daemon :: send_when_durable(int64_t idx, paxos_group_id g, std::auto_ptr<e::buffer> msg)
{
    const paxos_group* group = get_config()->get_group(g);

    if (!group || group->members_sz <= 0)
    {
        return;
    }

    e::buffer* msgs[CONSUS_MAX_REPLICATION_FACTOR];
    msgs[0] = msg.release();

    for (size_t i = 1; i < group->members_sz; ++i)
    {
        msgs[i] = msgs[0]->copy();
    }

    send_when_durable(idx, group->members, msgs, group->members_sz);
}

void
daemon :: send_when_durable(int64_t idx, comm_id id, std::auto_ptr<e::buffer> msg)
{
    e::buffer* m = msg.release();
    send_when_durable(idx, &id, &m, 1);
}

void
daemon :: send_when_durable(int64_t idx, const comm_id* ids, e::buffer** msgs, size_t sz)
{
    if (idx < 0)
    {
        for (size_t i = 0; i < sz; ++i)
        {
            delete msgs[i];
        }

        return;
    }

    bool send_now = false;

    {
        po6::threads::mutex::hold hold(&m_durable_mtx);
        send_now = idx <= m_durable_up_to;

        if (!send_now)
        {
            for (size_t i = 0; i < sz; ++i)
            {
                durable_msg d(idx, ids[i], msgs[i]);
                m_durable_msgs.push_back(d);
                std::push_heap(m_durable_msgs.begin(), m_durable_msgs.end());
            }
        }
    }

    if (send_now)
    {
        for (size_t i = 0; i < sz; ++i)
        {
            send(ids[i], std::auto_ptr<e::buffer>(msgs[i]));
        }
    }
}

void
daemon :: send_if_durable(int64_t idx, paxos_group_id g, std::auto_ptr<e::buffer> msg)
{
    const paxos_group* group = get_config()->get_group(g);

    if (!group || group->members_sz <= 0)
    {
        return;
    }

    e::buffer* msgs[CONSUS_MAX_REPLICATION_FACTOR];
    msgs[0] = msg.release();

    for (size_t i = 1; i < group->members_sz; ++i)
    {
        msgs[i] = msgs[0]->copy();
    }

    send_if_durable(idx, group->members, msgs, group->members_sz);
}

void
daemon :: send_if_durable(int64_t idx, comm_id id, std::auto_ptr<e::buffer> msg)
{
    e::buffer* m = msg.release();
    send_if_durable(idx, &id, &m, 1);
}

void
daemon :: send_if_durable(int64_t idx, const comm_id* ids, e::buffer** msgs, size_t sz)
{
    bool should_send = false;
    m_durable_mtx.lock();
    should_send = idx <= m_durable_up_to;
    m_durable_mtx.unlock();

    if (idx < 0 || !should_send)
    {
        for (size_t i = 0; i < sz; ++i)
        {
            delete msgs[i];
        }

        return;
    }

    for (size_t i = 0; i < sz; ++i)
    {
        send(ids[i], std::auto_ptr<e::buffer>(msgs[i]));
    }
}

struct daemon::durable_cb
{
    durable_cb() : recno(), tg(), seqno() {}
    durable_cb(int64_t r, transaction_group t, uint64_t s) : recno(r), tg(t), seqno(s) {}
    durable_cb(const durable_cb& other)
        : recno(other.recno), tg(other.tg), seqno(other.seqno) {}
    ~durable_cb() throw () {}
    durable_cb& operator = (const durable_cb& rhs)
    {
        // no self-assign check needed
        recno = rhs.recno;
        tg = rhs.tg;
        seqno = rhs.seqno;
        return *this;
    }
    bool operator < (const durable_cb& rhs) { return rhs.recno > recno; }
    int64_t recno;
    transaction_group tg;
    uint64_t seqno;
};

void
daemon :: callback_when_durable(const std::string& entry, const transaction_group& tg, uint64_t seqno)
{
    int64_t x = m_log.append(entry.data(), entry.size());

    if (x < 0)
    {
        return;
    }

    bool wake = false;

    {
        po6::threads::mutex::hold hold(&m_durable_mtx);
        durable_cb d(x, tg, seqno);
        m_durable_cbs.push_back(d);
        std::push_heap(m_durable_cbs.begin(), m_durable_cbs.end());
        wake = x <= m_durable_up_to;
    }

    if (wake)
    {
        m_log.wake();
    }
}

void
daemon :: durable()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0 ||
        pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    LOG(INFO) << "durability monitor started";
    int64_t x = -1;
    e::garbage_collector::thread_state ts;
    m_gc.register_thread(&ts);

    while (true)
    {
        m_gc.offline(&ts);
        x = m_log.wait(x);
        m_gc.online(&ts);

        if (m_log.error() != 0)
        {
            if (e::atomic::increment_32_nobarrier(&s_interrupts, 0) > 0)
            {
                break;
            }

            LOG(ERROR) << "durable log: " << po6::strerror(m_log.error());
            e::atomic::increment_32_nobarrier(&s_interrupts, 2);
            break;
        }

        std::vector<durable_msg> msgs;
        std::vector<durable_cb> cbs;

        {
            po6::threads::mutex::hold hold(&m_durable_mtx);
            m_durable_up_to = x;

            while (!m_durable_msgs.empty() &&
                   m_durable_msgs[0].recno < x)
            {
                msgs.push_back(m_durable_msgs[0]);
                std::pop_heap(m_durable_msgs.begin(), m_durable_msgs.end());
                m_durable_msgs.pop_back();
            }

            while (!m_durable_cbs.empty() &&
                   m_durable_cbs[0].recno < x)
            {
                cbs.push_back(m_durable_cbs[0]);
                std::pop_heap(m_durable_cbs.begin(), m_durable_cbs.end());
                m_durable_cbs.pop_back();
            }
        }

        for (size_t i = 0; i < msgs.size(); ++i)
        {
            std::auto_ptr<e::buffer> msg(msgs[i].msg);
            send(msgs[i].client, msg);
        }

        for (size_t i = 0; i < cbs.size(); ++i)
        {
            transaction_map_t::state_reference tsr;
            transaction* xact = m_transactions.get_state(cbs[i].tg, &tsr);

            if (xact)
            {
                xact->callback_durable(cbs[i].seqno, this);
            }
        }

        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "durability monitor shutting down";
}

void
daemon :: pump()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0 ||
        pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    LOG(INFO) << "pumping thread started";
    e::garbage_collector::thread_state ts;
    m_gc.register_thread(&ts);

    while (true)
    {
        m_gc.offline(&ts);
        po6::sleep(PO6_MILLIS * 250);
        m_gc.online(&ts);

        if (e::atomic::increment_32_nobarrier(&s_interrupts, 0) > 0)
        {
            break;
        }

        // a transaction implicitly pumps a local or global voter
        for (transaction_map_t::iterator it(&m_transactions); it.valid(); ++it)
        {
            transaction* xact = *it;
            xact->externally_work_state_machine(this);
        }

        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "pumping thread shutting down";
}
