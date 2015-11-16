// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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

// LevelDB
#include <leveldb/comparator.h>

// po6
#include <po6/io/fd.h>
#include <po6/path.h>

// e
#include <e/atomic.h>
#include <e/daemon.h>
#include <e/daemonize.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/identity.h>
#include <e/serialization.h>
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include <consus.h>
#include "common/consus.h"
#include "common/macros.h"
#include "common/network_msgtype.h"
#include "common/transaction_group.h"
#include "kvs/daemon.h"

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

daemon :: coordinator_callback :: coordinator_callback(daemon* _d)
    : d(_d)
{
}

daemon :: coordinator_callback :: ~coordinator_callback() throw ()
{
}

static std::vector<std::string>
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
    e::atomic::store_ptr_release(&d->m_config, c.release());
    d->m_gc.collect(old_config, e::garbage_collector::free_ptr<configuration>);
    LOG(INFO) << "updating to configuration " << d->get_config()->version();

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

struct daemon::comparator : public leveldb::Comparator
{
    comparator();
    virtual ~comparator() throw ();
    virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;
    virtual const char* Name() const { return "ConsusComparator"; }
    virtual void FindShortestSeparator(std::string*,
                                       const leveldb::Slice&) const {}
    virtual void FindShortSuccessor(std::string*) const {}
};

daemon :: comparator :: comparator()
{
}

daemon :: comparator :: ~comparator() throw ()
{
}

int
daemon :: comparator :: Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
{
    if (a.size() < 8 || b.size() < 8)
    {
        return -1;
    }

    e::slice x(a.data(), a.size() - 8);
    e::slice y(b.data(), b.size() - 8);
    size_t sz = std::min(x.size(), y.size());
    int cmp = memcmp(x.data(), y.data(), sz);

    if (cmp < 0)
    {
        return -1;
    }
    if (cmp > 0)
    {
        return 1;
    }

    uint64_t at;
    uint64_t bt;
    e::unpack64be(x.data() + x.size() - 8, &at);
    e::unpack64be(y.data() + y.size() - 8, &bt);

    if (at > bt)
    {
        return -1;
    }
    if (at < bt)
    {
        return 1;
    }

    return 0;
}

daemon :: daemon()
    : m_us()
    , m_gc()
    , m_busybee_mapper(this)
    , m_busybee()
    , m_coord_cb()
    , m_coord()
    , m_config(NULL)
    , m_threads()
    , m_cmp(new comparator())
    , m_bf()
    , m_db()
{
}

daemon :: ~daemon() throw ()
{
    m_gc.collect(get_config(), e::garbage_collector::free_ptr<configuration>);

    if (m_db)
    {
        delete m_db;
    }

    if (m_bf)
    {
        delete m_bf;
    }
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

    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.filter_policy = m_bf = leveldb::NewBloomFilterPolicy(10);
    opts.max_open_files = std::max(sysconf(_SC_OPEN_MAX) >> 1, 1024L);
    opts.comparator = m_cmp.get();
    leveldb::Status st = leveldb::DB::Open(opts, data, &m_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
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
        if (!e::generate_token(&id))
        {
            PLOG(ERROR) << "could not read random token from /dev/urandom";
            return EXIT_FAILURE;
        }

        m_us.id = comm_id(id);
        coordfunc = &coordinator_link::initial_registration;
    }

    m_coord_cb.reset(new coordinator_callback(this));
    m_coord.reset(new coordinator_link(rendezvous, m_us.id, m_us.bind_to, m_coord_cb.get()));
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

    m_busybee.reset(new busybee_mta(&m_gc, &m_busybee_mapper, bind_to, id, threads));

    for (size_t i = 0; i < threads; ++i)
    {
        using namespace po6::threads;
        e::compat::shared_ptr<thread> t(new thread(make_thread_wrapper(&daemon::loop, this, i)));
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

    LOG(INFO) << "consus is gracefully shutting down";
    return EXIT_SUCCESS;
}

void
daemon :: loop(size_t thread)
{
    size_t core = thread % sysconf(_SC_NPROCESSORS_ONLN);
#ifdef __LINUX__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_t cur = pthread_self();
    int x = pthread_setaffinity_np(cur, sizeof(cpu_set_t), &cpuset);
    assert(x == 0);
#elif defined(__APPLE__)
    thread_affinity_policy_data_t policy;
    policy.affinity_tag = 0;
    thread_policy_set(mach_thread_self(),
                      THREAD_AFFINITY_POLICY,
                      (thread_policy_t)&policy,
                      THREAD_AFFINITY_POLICY_COUNT);
#endif

    LOG(INFO) << "network thread " << thread << " started on core " << core;

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
        busybee_returncode rc = m_busybee->recv(&ts, &_id, &msg);

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
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
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

        switch (mt)
        {
            case KVS_RD_LOCK:
                process_read_lock(id, msg, up);
                break;
            case KVS_RD_UNLOCK:
                process_read_unlock(id, msg, up);
                break;
            case KVS_WR_BEGIN:
                process_write_begin(id, msg, up);
                break;
            case KVS_WR_FINISH:
                process_write_finish(id, msg, up);
                break;
            case KVS_WR_CANCEL:
                process_write_cancel(id, msg, up);
                break;
            case CONSUS_NOP:
                break;
            case CLIENT_RESPONSE:
            case TXMAN_BEGIN:
            case TXMAN_READ:
            case TXMAN_WRITE:
            case TXMAN_COMMIT:
            case TXMAN_ABORT:
            case TXMAN_PAXOS_2A:
            case TXMAN_PAXOS_2B:
            case LV_VOTE_1A:
            case LV_VOTE_1B:
            case LV_VOTE_2A:
            case LV_VOTE_2B:
            case LV_VOTE_LEARN:
            case KVS_RD_LOCKED:
            case KVS_RD_UNLOCKED:
            case KVS_WR_BEGUN:
            case KVS_WR_FINISHED:
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
daemon :: process_read_lock(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    up = up >> tg >> seqno >> table >> key;
    CHECK_UNPACK(KVS_RD_LOCK, up);

    if (s_debug_mode)
    {
        LOG(INFO) << "get(\"" << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\") by "
                  << tg << "[" << seqno << "]";
    }

    LOG(INFO) << tg << " request read lock(table=\"" << e::strescape(table.str())
              << "\", key=\"" << e::strescape(key.str()) << "\"); not implemented (yet)";

    std::string tmp;
    e::packer(&tmp) << table << key << uint64_t(UINT64_MAX);
    leveldb::ReadOptions opts;
    leveldb::Iterator* it = m_db->NewIterator(opts);

    if (!it)
    {
        LOG(ERROR) << "fatal LevelDB failure: " << it->status().ToString();
        e::atomic::increment_32_nobarrier(&s_interrupts, 1);
        return;
    }

    e::guard g_it = e::makeguard(e::garbage_collector::free_ptr<leveldb::Iterator>, it);
    it->Seek(tmp);
    consus_returncode rc;
    uint64_t timestamp = 0;
    e::slice value;

    if (!it->status().ok())
    {
        LOG(ERROR) << "fatal LevelDB failure: " << it->status().ToString();
        e::atomic::increment_32_nobarrier(&s_interrupts, 1);
        return;
    }

    leveldb::Slice k;

    if (!it->Valid() || (k = it->key()).size() < 8 ||
        memcmp(k.data(), tmp.data(), k.size() - 8) != 0)
    {
        rc = CONSUS_NOT_FOUND;
    }
    else
    {
        rc = CONSUS_SUCCESS;
        e::unpack64be(k.data() + k.size() - 8, &timestamp);
        leveldb::Slice v = it->value();
        value = e::slice(v.data(), v.size());
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RD_LOCKED)
                    + pack_size(tg)
                    + sizeof(uint64_t)
                    + pack_size(rc)
                    + sizeof(uint64_t)
                    + pack_size(value);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RD_LOCKED << tg << seqno << rc << timestamp << value;
    send(id, msg);

    if (s_debug_mode)
    {
        if (rc == CONSUS_SUCCESS)
        {
            LOG(INFO) << "timestamp=" << timestamp
                      << " value=\"" << e::strescape(value.str()) << "\"";
        }
        else
        {
            LOG(INFO) << "value not found";
        }
    }
}

void
daemon :: process_read_unlock(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    up = up >> tg >> seqno >> table >> key;
    CHECK_UNPACK(KVS_RD_UNLOCK, up);

    //if (s_debug_mode)
    //{
        LOG(ERROR) << tg << " request read unlock(table=\"" << e::strescape(table.str())
                  << "\", key=\"" << e::strescape(key.str()) << "\"); not implemented (yet)";
    //}

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RD_UNLOCKED)
                    + pack_size(tg)
                    + sizeof(uint64_t);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_RD_UNLOCKED << tg << seqno;
    send(id, msg);
}

void
daemon :: process_write_begin(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    up = up >> tg >> seqno >> table >> key;
    CHECK_UNPACK(KVS_WR_BEGIN, up);

    //if (s_debug_mode)
    //{
        LOG(ERROR) << tg << " request write lock(table=\"" << e::strescape(table.str())
                  << "\", key=\"" << e::strescape(key.str()) << "\"); not implemented (yet)";
    //}

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_BEGUN)
                    + pack_size(tg)
                    + sizeof(uint64_t);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_WR_BEGUN << tg << seqno;
    send(id, msg);
}

void
daemon :: process_write_finish(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    e::slice value;
    up = up >> tg >> seqno >> table >> key >> timestamp >> value;
    CHECK_UNPACK(KVS_WR_BEGIN, up);

    if (s_debug_mode)
    {
        LOG(INFO) << "put(\"" << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\"@" << timestamp << ", \""
                  << e::strescape(value.str()) << "\") by "
                  << tg << "[" << seqno << "]";
    }

    LOG(INFO) << tg << " request write unlock(table=\"" << e::strescape(table.str())
              << "\", key=\"" << e::strescape(key.str()) << "\"); not implemented (yet)";

    std::string tmp;
    e::packer(&tmp) << table << key << timestamp;
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = m_db->Put(opts, tmp, leveldb::Slice(value.cdata(), value.size()));
    consus_returncode rc;

    if (st.ok())
    {
        rc = CONSUS_SUCCESS;
    }
    else
    {
        rc = CONSUS_SERVER_ERROR;
    }


    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_FINISHED)
                    + pack_size(tg)
                    + sizeof(uint64_t)
                    + pack_size(rc)
                    + sizeof(uint64_t)
                    + pack_size(e::slice());
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_WR_FINISHED << tg << seqno << rc;
    send(id, msg);

    if (!st.ok())
    {
        LOG(ERROR) << "fatal LevelDB failure: " << st.ToString();
    }
}

void
daemon :: process_write_cancel(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    e::slice table;
    e::slice key;
    up = up >> tg >> seqno >> table >> key;
    CHECK_UNPACK(KVS_WR_CANCEL, up);

    //if (s_debug_mode)
    //{
        LOG(ERROR) << tg << " request write unlock(table=\"" << e::strescape(table.str())
                  << "\", key=\"" << e::strescape(key.str()) << "\"); not implemented (yet)";
    //}

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_FINISHED)
                    + pack_size(tg)
                    + sizeof(uint64_t);
    msg.reset(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_WR_FINISHED << tg << seqno << CONSUS_SUCCESS;
    send(id, msg);
}

consus::configuration*
daemon :: get_config()
{
    return e::atomic::load_ptr_acquire(&m_config);
}

void
daemon :: debug_dump()
{
    LOG(ERROR) << "DEBUG DUMP"; // XXX
}

bool
daemon :: send(comm_id id, std::auto_ptr<e::buffer> msg)
{
    busybee_returncode rc = m_busybee->send(id.get(), msg);

    switch (rc)
    {
        case BUSYBEE_SUCCESS:
            return true;
        case BUSYBEE_DISRUPTED:
            return false;
        case BUSYBEE_SHUTDOWN:
        case BUSYBEE_INTERRUPTED:
        case BUSYBEE_POLLFAILED:
        case BUSYBEE_ADDFDFAIL:
        case BUSYBEE_TIMEOUT:
        case BUSYBEE_EXTERNAL:
        default:
            LOG(ERROR) << "internal invariants broken; crashing";
            abort();
    }
}
