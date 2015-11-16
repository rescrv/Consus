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

// po6
#include <po6/errno.h>
#include <po6/io/fd.h>
#include <po6/path.h>

// e
#include <e/atomic.h>
#include <e/daemon.h>
#include <e/daemonize.h>
#include <e/identity.h>
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/coordinator_returncode.h"
#include "common/macros.h"
#include "txman/daemon.h"
#include "txman/log_entry_t.h"

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
    std::vector<paxos_group_id> gs = d->get_config()->groups_for(d->m_us.id);

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
    , m_busybee_mapper(this)
    , m_busybee()
    , m_coord_cb()
    , m_coord()
    , m_config(NULL)
    , m_threads()
    , m_transactions(&m_gc)
    , m_local_voters(&m_gc)
    , m_dispositions(&m_gc)
    , m_log()
    , m_durable_thread(po6::threads::make_thread_wrapper(&daemon::durable, this))
    , m_durable_mtx()
    , m_durable_up_to(-1)
    , m_durable_msgs()
    , m_durable_cbs()
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

    m_busybee.reset(new busybee_mta(&m_gc, &m_busybee_mapper, bind_to, id, threads));
    m_durable_thread.start();

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

    m_log.close();
    m_durable_thread.join();
    LOG(ERROR) << "consus is gracefully shutting down";
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
            case KVS_RD_LOCKED:
                process_kvs_rd_locked(id, msg, up);
                break;
            case KVS_RD_UNLOCKED:
                process_kvs_rd_unlocked(id, msg, up);
                break;
            case KVS_WR_BEGUN:
                process_kvs_wr_begun(id, msg, up);
                break;
            case KVS_WR_FINISHED:
                process_kvs_wr_finished(id, msg, up);
                break;
            case CONSUS_NOP:
                break;
            case KVS_RD_LOCK:
            case KVS_RD_UNLOCK:
            case KVS_WR_BEGIN:
            case KVS_WR_FINISH:
            case KVS_WR_CANCEL:
            case CLIENT_RESPONSE:
            default:
                LOG(INFO) << "received " << mt << " message which transaction-managers do not process";
                break;
        }

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
    xact->commit(id, nonce, seqno, this);
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

    if (s_debug_mode)
    {
        LOG(INFO) << tg << ".synod_commit[" << unsigned(idx) << "] vote 1A " << b;
    }

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

    if (s_debug_mode)
    {
        LOG(INFO) << tg << ".synod_commit[" << unsigned(idx) << "] vote 1B " << b << " " << p;
    }

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

    if (s_debug_mode)
    {
        LOG(INFO) << tg << ".synod_commit[" << unsigned(idx) << "] vote 2A " << p;
    }

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

    if (s_debug_mode)
    {
        LOG(INFO) << tg << ".synod_commit[" << unsigned(idx) << "] vote 2B " << p << " from " << id.get();
    }

    local_voter_map_t::state_reference lvsr;
    local_voter* lv = m_local_voters.get_or_create_state(tg, &lvsr);
    assert(lv);
    lv->vote_2b(id, idx, p, this);
}

void
daemon :: process_lv_vote_learn(comm_id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint8_t idx;
    uint64_t v;
    up = up >> tg >> idx >> v;
    CHECK_UNPACK(LV_VOTE_LEARN, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << ".synod_commit[" << unsigned(idx) << "] vote learn " << v;
    }

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
}

void
daemon :: process_commit_record(comm_id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    e::slice commit_record;
    up = up >> tg >> commit_record;
    CHECK_UNPACK(COMMIT_RECORD, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << " commit record";
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_or_create_state(tg, &tsr);
    assert(xact);
    xact->commit_record(commit_record, msg, this);
}

void
daemon :: process_kvs_rd_locked(comm_id id, std::auto_ptr<e::buffer> msg, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    consus_returncode rc;
    uint64_t timestamp;
    e::slice value;
    up = up >> tg >> seqno >> rc >> timestamp >> value;
    CHECK_UNPACK(KVS_RD_LOCKED, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << "[" << seqno << "] kvs returned " << rc << " from " << id;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_state(tg, &tsr);

    if (xact)
    {
        xact->kvs_rd_locked(seqno, rc, timestamp, value, msg, this);
    }
}

void
daemon :: process_kvs_rd_unlocked(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    up = up >> tg >> seqno;
    CHECK_UNPACK(KVS_RD_UNLOCKED, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << "[" << seqno << "] lock released from " << id;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_state(tg, &tsr);

    if (xact)
    {
        xact->kvs_rd_unlocked(seqno, this);
    }
}

void
daemon :: process_kvs_wr_begun(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    up = up >> tg >> seqno;
    CHECK_UNPACK(KVS_WR_BEGUN, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << "[" << seqno << "] write started by " << id;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_state(tg, &tsr);

    if (xact)
    {
        xact->kvs_wr_begun(seqno, this);
    }
}

void
daemon :: process_kvs_wr_finished(comm_id id, std::auto_ptr<e::buffer>, e::unpacker up)
{
    transaction_group tg;
    uint64_t seqno;
    up = up >> tg >> seqno;
    CHECK_UNPACK(KVS_RD_UNLOCKED, up);

    if (s_debug_mode)
    {
        LOG(INFO) << tg << "[" << seqno << "] write finished by " << id;
    }

    transaction_map_t::state_reference tsr;
    transaction* xact = m_transactions.get_state(tg, &tsr);

    if (xact)
    {
        xact->kvs_wr_finished(seqno, this);
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
    LOG(ERROR) << "DEBUG DUMP"; // XXX
}

consus::transaction_id
daemon :: generate_txid()
{
    // XXX don't go out of process
    po6::io::fd fd(open("/dev/urandom", O_RDONLY));
    uint64_t x;
    int ret = fd.xread(&x, sizeof(x));
    assert(ret == 8);
    paxos_group_id id;
    std::vector<paxos_group_id> groups(get_config()->groups_for(m_us.id));
    // XXX groups.size() == 0?
    size_t idx = x % groups.size();
    id = groups[idx];
    return transaction_id(id, x);
}

bool
daemon :: send(comm_id id, std::auto_ptr<e::buffer> msg)
{
    if (id == comm_id())
    {
        return false;
    }

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

unsigned
daemon :: send(const paxos_group& g, std::auto_ptr<e::buffer> msg)
{
    unsigned count = 0;

    for (unsigned i = 0; i < g.members_sz; ++i)
    {
        std::auto_ptr<e::buffer> m(msg->copy());
        busybee_returncode rc = m_busybee->send(g.members[i].get(), m);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                ++count;
                break;
            case BUSYBEE_DISRUPTED:
                break;
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
daemon :: send_when_durable(const std::string& entry, comm_id* ids, e::buffer** msgs, size_t sz)
{
    int64_t x = m_log.append(entry.data(), entry.size());

    if (x < 0)
    {
        return;
    }

    bool wake = false;

    {
        po6::threads::mutex::hold hold(&m_durable_mtx);
        wake = x <= m_durable_up_to;

        for (size_t i = 0; i < sz; ++i)
        {
            durable_msg d(x, ids[i], msgs[i]);
            m_durable_msgs.push_back(d);
            std::push_heap(m_durable_msgs.begin(), m_durable_msgs.end());
        }
    }

    if (wake)
    {
        m_log.wake();
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

    while (true)
    {
        x = m_log.wait(x);

        if (m_log.error() != 0)
        {
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
                xact->callback(cbs[i].seqno, this);
            }
        }
    }

    LOG(INFO) << "durability monitor shutting down";
}
