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
#include <po6/io/fd.h>
#include <po6/path.h>
#include <po6/time.h>

// e
#include <e/atomic.h>
#include <e/base64.h>
#include <e/daemon.h>
#include <e/daemonize.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/identity.h>
#include <e/serialization.h>
#include <e/strescape.h>

// consus
#include <consus.h>
#include "common/background_thread.h"
#include "common/constants.h"
#include "common/consus.h"
#include "common/generate_token.h"
#include "common/lock.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/transaction_group.h"
#include "kvs/daemon.h"
#include "kvs/leveldb_datalayer.h"

using consus::daemon;

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
    virtual std::string prefix() { return "kvs"; }
    virtual bool new_config(const char* data, size_t data_sz);
    virtual bool has_id(comm_id id);
    virtual po6::net::location address(comm_id id);
    virtual bool is_steady_state(comm_id id);

    private:
        daemon* d;
        coordinator_callback(const coordinator_callback&);
        coordinator_callback& operator = (const coordinator_callback&);
};

class daemon::migration_bgthread : public consus::background_thread
{
    public:
        migration_bgthread(daemon* d);
        virtual ~migration_bgthread() throw ();

    public:
        void new_config();

    protected:
        virtual const char* thread_name();
        virtual bool have_work();
        virtual void do_work();

    private:
        migration_bgthread(const migration_bgthread&);
        migration_bgthread& operator = (const migration_bgthread&);

    private:
        daemon* m_d;
        bool m_have_new_config;
};

daemon :: coordinator_callback :: coordinator_callback(daemon* _d)
    : d(_d)
{
}

daemon :: coordinator_callback :: ~coordinator_callback() throw ()
{
}

std::vector<std::string>
split_by_newlines(std::string s)
{
    std::vector<std::string> v;

    while (!s.empty())
    {
        size_t idx = s.find_first_of('\n');

        if (idx == std::string::npos)
        {
            v.push_back(s);
            s = "";
        }
        else
        {
            v.push_back(s.substr(0, idx));
            s = s.substr(idx + 1, s.size());
        }
    }

    return v;
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
    d->m_migrate_thread->new_config();
    LOG(INFO) << "updating to configuration " << d->get_config()->version();

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
        return c->get_state(id) == kvs_state::ONLINE;
    }

    return false;
}

daemon :: migration_bgthread :: migration_bgthread(daemon* d)
    : background_thread(&d->m_gc)
    , m_d(d)
    , m_have_new_config(false)
{
}

daemon :: migration_bgthread :: ~migration_bgthread() throw ()
{
}

void
daemon :: migration_bgthread :: new_config()
{
    po6::threads::mutex::hold hold(mtx());
    m_have_new_config = true;
    wakeup();
}

const char*
daemon :: migration_bgthread :: thread_name()
{
    return "migration";
}

bool
daemon :: migration_bgthread :: have_work()
{
    return m_have_new_config;
}

void
daemon :: migration_bgthread :: do_work()
{
    {
        po6::threads::mutex::hold hold(mtx());
        m_have_new_config = false;
    }

    configuration* c = m_d->get_config();
    std::vector<partition_id> parts = c->migratable_partitions(m_d->m_us.id);

    for (size_t i = 0; i < parts.size(); ++i)
    {
        migrator_map_t::state_reference msr;
        migrator* m = m_d->m_migrations.get_or_create_state(parts[i], &msr);
        m->externally_work_state_machine(m_d);
    }

    // XXX add a true ticker to drive state machine (call ext_work_sm every X
    // seconds)
    po6::sleep(PO6_SECONDS);

    for (migrator_map_t::iterator it(&m_d->m_migrations); it.valid(); ++it)
    {
        migrator* m = *it;

        if (std::find(parts.begin(), parts.end(), m->state_key()) != parts.end())
        {
            m->externally_work_state_machine(m_d);
        }
        else
        {
            m->terminate();
        }
    }
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
    , m_data()
    , m_locks(&m_gc)
    , m_repl_lk(&m_gc)
    , m_repl_rd(&m_gc)
    , m_repl_wr(&m_gc)
    , m_migrations(&m_gc)
    , m_migrate_thread(new migration_bgthread(this))
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

    m_data.reset(new leveldb_datalayer());

    if (!m_data->init(data))
    {
        return EXIT_FAILURE;
    }

    bool saved;
    uint64_t id;
    std::string rendezvous(coordinator);

    if (!e::load_identity(po6::path::join(data, "KVS").c_str(), &saved, &id,
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
    LOG(INFO) << "starting consus kvs-daemon " << m_us.id
              << " on address " << m_us.bind_to;
    LOG(INFO) << "connecting to " << rendezvous;

    if (!(((*m_coord).*coordfunc)()))
    {
        return EXIT_FAILURE;
    }

    assert(get_config());

    if (!e::save_identity(po6::path::join(data, "KVS").c_str(), id, bind_to, rendezvous))
    {
        LOG(ERROR) << "could not save identity; exiting";
        return EXIT_FAILURE;
    }

    m_busybee.reset(busybee_server::create(&m_busybee_controller, id, bind_to, &m_gc));

    for (size_t i = 0; i < threads; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> t(new thread(make_obj_func(&daemon::loop, this, i)));
        m_threads.push_back(t);
        t->start();
    }

    m_migrate_thread->start();
    m_pumping_thread.start();

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
    m_pumping_thread.join();
    m_migrate_thread->shutdown();
    m_busybee->shutdown();

    for (size_t i = 0; i < m_threads.size(); ++i)
    {
        m_threads[i]->join();
    }

    LOG(INFO) << "consus is gracefully shutting down";
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
            case BUSYBEE_SEE_ERRNO:
                PLOG(ERROR) << "receive error";
                continue;
            case BUSYBEE_TIMEOUT:
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

        switch (mt)
        {
            case KVS_REP_RD:
                process_rep_rd(id, msg, up);
                break;
            case KVS_REP_WR:
                process_rep_wr(id, msg, up);
                break;
            case KVS_RAW_RD:
                process_raw_rd(id, msg, up);
                break;
            case KVS_RAW_RD_RESP:
                process_raw_rd_resp(id, msg, up);
                break;
            case KVS_RAW_WR:
                process_raw_wr(id, msg, up);
                break;
            case KVS_RAW_WR_RESP:
                process_raw_wr_resp(id, msg, up);
                break;
            case KVS_LOCK_OP:
                process_lock_op(id, msg, up);
                break;
            case KVS_RAW_LK:
                process_raw_lk(id, msg, up);
                break;
            case KVS_RAW_LK_RESP:
                process_raw_lk_resp(id, msg, up);
                break;
            case KVS_WOUND_XACT:
                process_wound_xact(id, msg, up);
                break;
            case KVS_MIGRATE_SYN:
                process_migrate_syn(id, msg, up);
                break;
            case KVS_MIGRATE_ACK:
                process_migrate_ack(id, msg, up);
                break;
            case CONSUS_NOP:
                break;
            case CLIENT_RESPONSE:
            case TXMAN_BEGIN:
            case TXMAN_READ:
            case TXMAN_WRITE:
            case TXMAN_COMMIT:
            case TXMAN_ABORT:
            case TXMAN_WOUND:
            case TXMAN_PAXOS_2A:
            case TXMAN_PAXOS_2B:
            case LV_VOTE_1A:
            case LV_VOTE_1B:
            case LV_VOTE_2A:
            case LV_VOTE_2B:
            case LV_VOTE_LEARN:
            case COMMIT_RECORD:
            case GV_OUTCOME:
            case GV_PROPOSE:
            case GV_VOTE_1A:
            case GV_VOTE_1B:
            case GV_VOTE_2A:
            case GV_VOTE_2B:
            case KVS_REP_RD_RESP:
            case KVS_REP_WR_RESP:
            case KVS_LOCK_OP_RESP:
            default:
                LOG(INFO) << "received " << mt << " message which key-value-stores do not process";
                break;
        }

        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "network thread shutting down";
}

void
daemon :: process_rep_rd(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    uint64_t nonce;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    up = up >> nonce >> table >> key >> timestamp;
    CHECK_UNPACK(KVS_REP_RD, up);
    // XXX check key meet spec

    while (true)
    {
        uint64_t x = generate_id();
        read_replicator_map_t::state_reference rsr;
        read_replicator* r = m_repl_rd.create_state(x, &rsr);

        if (!r)
        {
            continue;
        }

        r->init(id, nonce, table, key, msg);
        r->externally_work_state_machine(this);
        break;
    }
}

void
daemon :: process_rep_wr(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    uint64_t nonce;
    uint8_t flags;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    e::slice value;
    up = up >> nonce >> flags >> table >> key >> timestamp >> value;
    CHECK_UNPACK(KVS_REP_WR, up);
    // XXX check key/value meet spec

    while (true)
    {
        uint64_t x = generate_id();
        write_replicator_map_t::state_reference wsr;
        write_replicator* w = m_repl_wr.create_state(x, &wsr);

        if (!w)
        {
            continue;
        }

        w->init(id, nonce, flags, table, key, timestamp, value, msg);
        w->externally_work_state_machine(this);
        break;
    }
}

void
daemon :: process_raw_rd(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    up = up >> nonce >> table >> key >> timestamp;
    CHECK_UNPACK(KVS_RAW_RD, up);
    configuration* c = get_config();
    // XXX check table exists
    // XXX check key meet spec
    replica_set rs;

    if (!c->hash(m_us.dc, table, key, &rs))
    {
        if (s_debug_mode)
        {
            LOG(INFO) << logid(table, key) << "-R-RAW dropped because hashing failed";
        }

        return;
    }

    e::slice value;
    datalayer::reference* ref = NULL;
    consus_returncode rc = CONSUS_GARBAGE;
    rc = m_data->get(table, key, timestamp, &timestamp, &value, &ref);

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_RD_RESP)
                    + sizeof(uint64_t)
                    + pack_size(rc)
                    + sizeof(uint64_t)
                    + pack_size(value)
                    + pack_size(rs);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RAW_RD_RESP << nonce << rc << timestamp << value << rs;
    send(id, msg);

    if (s_debug_mode)
    {
        LOG(INFO) << logid(table, key) << "-R-RAW read; value=\""
                  << e::strescape(value.str()) << "\"@" << timestamp
                  << "\", nonce=" << nonce << " replicas=" << rs;
    }
}

void
daemon :: process_raw_rd_resp(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    uint64_t nonce;
    consus_returncode rc;
    uint64_t timestamp;
    e::slice value;
    replica_set rs;
    up = up >> nonce >> rc >> timestamp >> value >> rs;
    CHECK_UNPACK(KVS_RAW_RD_RESP, up);
    read_replicator_map_t::state_reference rsr;
    read_replicator* r = m_repl_rd.get_state(nonce, &rsr);

    if (r)
    {
        r->response(id, rc, timestamp, value, rs, msg, this);
    }
    else
    {
        LOG_IF(INFO, s_debug_mode) << "dropped raw read; nonce=" << nonce << " rc=" << rc << " from=" << id;
    }
}

void
daemon :: process_raw_wr(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    uint8_t flags;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    e::slice value;
    up = up >> nonce >> flags >> table >> key >> timestamp >> value;
    CHECK_UNPACK(KVS_RAW_WR, up);
    configuration* c = get_config();
    // XXX check table exists
    // XXX check key/value meet spec
    replica_set rs;

    if (!c->hash(m_us.dc, table, key, &rs))
    {
        if (s_debug_mode)
        {
            LOG(INFO) << logid(table, key) << "-W-RAW dropped because hashing failed";
        }

        return;
    }

    consus_returncode rc = CONSUS_GARBAGE;

    if ((CONSUS_WRITE_TOMBSTONE & flags))
    {
        rc = m_data->del(table, key, timestamp);
    }
    else
    {
        rc = m_data->put(table, key, timestamp, value);
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_WR_RESP)
                    + sizeof(uint64_t)
                    + pack_size(rc)
                    + pack_size(rs);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_RAW_WR_RESP << nonce << rc << rs;
    send(id, msg);

    if (s_debug_mode)
    {
        if ((CONSUS_WRITE_TOMBSTONE & flags))
        {
            LOG(INFO) << logid(table, key) << "-W-RAW deleted; nonce=" << nonce << " replicas=" << rs;
        }
        else
        {
            LOG(INFO) << logid(table, key) << "-W-RAW written; nonce=" << nonce << " replicas=" << rs;
        }
    }
}

void
daemon :: process_raw_wr_resp(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    consus_returncode rc;
    replica_set rs;
    up = up >> nonce >> rc >> rs;
    CHECK_UNPACK(KVS_RAW_WR_RESP, up);
    write_replicator_map_t::state_reference wsr;
    write_replicator* w = m_repl_wr.get_state(nonce, &wsr);

    if (w)
    {
        w->response(id, rc, rs, this);
    }
    else
    {
        LOG_IF(INFO, s_debug_mode) << "dropped raw write; nonce=" << nonce << " rc=" << rc << " from=" << id;
    }
}

void
daemon :: process_lock_op(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    uint64_t nonce;
    e::slice table;
    e::slice key;
    transaction_group tg;
    lock_op op;
    up = up >> nonce >> table >> key >> tg >> op;
    CHECK_UNPACK(KVS_LOCK_OP, up);
    // XXX check table exists
    // XXX check key meets spec

    while (true)
    {
        uint64_t x = generate_id();
        lock_replicator_map_t::state_reference lsr;
        lock_replicator* lr = m_repl_lk.create_state(x, &lsr);

        if (!lr)
        {
            continue;
        }

        lr->init(id, nonce, table, key, tg, op, msg);
        lr->externally_work_state_machine(this);
        break;
    }
}

void
daemon :: process_raw_lk(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    e::slice table;
    e::slice key;
    transaction_group tg;
    lock_op op;
    up = up >> nonce >> table >> key >> tg >> op;
    CHECK_UNPACK(KVS_RAW_LK, up);
    // XXX check table exists
    // XXX check key/value meet spec

    switch (op)
    {
        case LOCK_LOCK:
            return m_locks.lock(id, nonce, table, key, tg, this);
        case LOCK_UNLOCK:
            return m_locks.unlock(id, nonce, table, key, tg, this);
        default:
            LOG(ERROR) << "received invalid lock op " << (unsigned)op;
            return;
    }
}

void
daemon :: process_raw_lk_resp(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    transaction_group tg;
    replica_set rs;
    up = up >> nonce >> tg >> rs;
    CHECK_UNPACK(KVS_RAW_LK_RESP, up);
    lock_replicator_map_t::state_reference sr;
    lock_replicator* lk = m_repl_lk.get_state(nonce, &sr);

    if (lk)
    {
        lk->response(id, tg, rs, this);
    }
}

void
daemon :: process_wound_xact(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    uint64_t nonce;
    uint8_t flags;
    transaction_group tg;
    up = up >> nonce >> flags >> tg;
    CHECK_UNPACK(KVS_WOUND_XACT, up);
    lock_replicator_map_t::state_reference sr;
    lock_replicator* lk = m_repl_lk.get_state(nonce, &sr);

    if (lk)
    {
        LOG_IF(INFO, s_debug_mode) << "wounding transaction " << tg;

        if ((flags & WOUND_XACT_ABORT))
        {
            lk->abort(tg, this);
        }
        else if ((flags & WOUND_XACT_DROP_REQ))
        {
            lk->drop(tg);
        }
    }
}

void
daemon :: process_migrate_syn(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    partition_id key;
    version_id version;
    up = up >> key >> version;
    CHECK_UNPACK(KVS_MIGRATE_SYN, up);
    configuration* c = get_config();

    if (c->version() >= version)
    {
        LOG_IF(INFO, s_debug_mode) << "received migration SYN for " << key << "/" << version;
        assert(pack_size(KVS_MIGRATE_SYN) == pack_size(KVS_MIGRATE_ACK));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_MIGRATE_ACK << key << c->version();
        send(id, msg);
    }
}

void
daemon :: process_migrate_ack(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    partition_id key;
    version_id version;
    up = up >> key >> version;
    CHECK_UNPACK(KVS_MIGRATE_ACK, up);

    // XXX check source?

    migrator_map_t::state_reference msr;
    migrator* m = m_migrations.get_state(key, &msr);

    if (m)
    {
        m->ack(version, this);
    }
}

std::string
daemon :: logid(const e::slice& table, const e::slice& key)
{
    e::compat::hash<std::string> h;
    unsigned char buf[2 * sizeof(uint64_t)];
    unsigned char* ptr = buf;
    ptr = e::pack64be(h(table.str()), ptr);
    ptr = e::pack64be(h(key.str()), ptr);
    char b64[2 * sizeof(buf)];
    size_t sz = e::b64_ntop(buf, ptr - buf, b64, sizeof(b64));
    assert(sz <= sizeof(b64));
    return std::string(b64, sz);
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
    LOG(INFO) << "------------------------------- Replicating Locks ------------------------------";

    for (lock_replicator_map_t::iterator it(&m_repl_lk); it.valid(); ++it)
    {
        lock_replicator* lr = *it;
        std::string debug = lr->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << "request=" << lr->state_key() << " " << lines[i];
        }
    }

    LOG(INFO) << "------------------------------- Replicating Reads ------------------------------";

    for (read_replicator_map_t::iterator it(&m_repl_rd); it.valid(); ++it)
    {
        read_replicator* rr = *it;
        std::string debug = rr->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << "request=" << rr->state_key() << " " << lines[i];
        }
    }

    LOG(INFO) << "------------------------------ Replicating Writes ------------------------------";

    for (write_replicator_map_t::iterator it(&m_repl_wr); it.valid(); ++it)
    {
        write_replicator* wr = *it;
        std::string debug = wr->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << "request=" << wr->state_key() << " " << lines[i];
        }
    }

    LOG(INFO) << "---------------------------------- Lock State ----------------------------------";

    {
        std::string debug = m_locks.debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << lines[i];
        }
    }

    LOG(INFO) << "---------------------------------- Migrations ----------------------------------";

    for (migrator_map_t::iterator it(&m_migrations); it.valid(); ++it)
    {
        migrator* m = *it;
        std::string debug = m->debug_dump();
        std::vector<std::string> lines = split_by_newlines(debug);

        for (size_t i = 0; i < lines.size(); ++i)
        {
            LOG(INFO) << "partition=" << m->state_key().get() << " " << lines[i];
        }
    }

    LOG(INFO) << "================================ End Debug Dump ================================";
}

uint64_t
daemon :: generate_id()
{
    // XXX copy of txman impl
    // XXX don't go out of process
    po6::io::fd fd(open("/dev/urandom", O_RDONLY));
    uint64_t x;
    int ret = fd.xread(&x, sizeof(x));
    assert(ret == 8);
    return x;
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

    busybee_returncode rc = m_busybee->send(id.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            LOG_IF(INFO, s_debug_mode) << "message not sent: disrupted";
            return false;
        case BUSYBEE_SEE_ERRNO:
            PLOG(ERROR) << "send error";
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

        for (lock_replicator_map_t::iterator it(&m_repl_lk); it.valid(); ++it)
        {
            lock_replicator* lr = *it;
            lr->externally_work_state_machine(this);
        }

        for (read_replicator_map_t::iterator it(&m_repl_rd); it.valid(); ++it)
        {
            read_replicator* rr = *it;
            rr->externally_work_state_machine(this);
        }

        for (write_replicator_map_t::iterator it(&m_repl_wr); it.valid(); ++it)
        {
            write_replicator* wr = *it;
            wr->externally_work_state_machine(this);
        }

        for (migrator_map_t::iterator it(&m_migrations); it.valid(); ++it)
        {
            migrator* m = *it;
            m->externally_work_state_machine(this);
        }

        m_gc.quiescent_state(&ts);
    }

    m_gc.deregister_thread(&ts);
    LOG(INFO) << "pumping thread shutting down";
}
