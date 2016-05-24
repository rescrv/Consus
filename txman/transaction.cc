// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// STL
#include <string>

// Google Log
#include <glog/logging.h>

// e
#include <e/serialization.h>
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "common/ids.h"
#include "txman/daemon.h"
#include "txman/log_entry_t.h"
#include "txman/transaction.h"

#define UNPACK_ERROR(X) \
    LOG(ERROR) << logid() << " failed while unpacking " << (X);

#define INVARIANT_VIOLATION(X) \
    LOG(ERROR) << logid() << " invariant violation: " << (X) << " failed";

#define RETURN_IF_EXECUTED(I, S, A) \
    do { \
    if (m_state > EXECUTING) \
    { \
        LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << (I) << "]: " << (S) << " initiated " << (A) << " dropped because transaction has finished execution"; \
        return; \
    } \
    } while (0)

#pragma GCC diagnostic ignored "-Wlarger-than="

using consus::transaction;

extern bool s_debug_mode;

struct transaction :: comparison
{
    comparison() : type(false), table(false), key(false), value(false) {}
    ~comparison() throw () {}

    bool type;
    bool table;
    bool key;
    bool value;

    private:
        comparison(const comparison& other);
        comparison& operator = (const comparison& rhs);
};

struct transaction :: operation
{
    operation();
    ~operation() throw ();

    // utils
    void set_client(comm_id client, uint64_t nonce);
    bool merge(const operation& op, const comparison& cmp);

    // log entry
    log_entry_t type;
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    e::slice value;
    consus_returncode rc;
    e::compat::shared_ptr<e::buffer> backing;

    // reading
    bool require_read_lock;
    bool read_lock_acquired;
    bool read_lock_released;
    e::compat::shared_ptr<e::buffer> read_backing;
    uint64_t read_timestamp;

    // writing
    bool require_write;
    bool write_started;
    bool write_finished;
    uint64_t write_timestamp;

    // durability
    bool log_write_issued;
    bool log_write_durable;
    bool durable[CONSUS_MAX_REPLICATION_FACTOR];
    uint64_t paxos_timestamps[CONSUS_MAX_REPLICATION_FACTOR];
    uint64_t paxos_2b_timestamps[CONSUS_MAX_REPLICATION_FACTOR];

    // client response
    comm_id client;
    uint64_t nonce;
};

transaction :: operation :: operation()
    : type(LOG_ENTRY_NOP)
    , table()
    , key()
    , timestamp(0)
    , value()
    , rc(CONSUS_GARBAGE)
    , backing()
    , require_read_lock(false)
    , read_lock_acquired(false)
    , read_lock_released(false)
    , read_backing()
    , read_timestamp(0)
    , require_write(false)
    , write_started(false)
    , write_finished(false)
    , write_timestamp(0)
    , log_write_issued(false)
    , log_write_durable(false)
    , client()
    , nonce()
{
    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        durable[i] = false;
        paxos_timestamps[i] = 0;
        paxos_2b_timestamps[i] = 0;
    }
}

transaction :: operation :: ~operation() throw ()
{
}

void
transaction :: operation :: set_client(comm_id c, uint64_t n)
{
    if (client == comm_id() || c != comm_id())
    {
        client = c;
        nonce = n;
    }
}

bool
transaction :: operation :: merge(const operation& op, const comparison& cmp)
{
    if (type == LOG_ENTRY_NOP)
    {
        type = op.type;
        table = op.table;
        key = op.key;
        value = op.value;
        backing = op.backing;
    }
    else
    {
        if ((cmp.type && type != op.type) ||
            (cmp.table && table != op.table) ||
            (cmp.key && key != op.key) ||
            (cmp.value && value != op.value))
        {
            return false;
        }
    }

    return true;
}

transaction :: transaction(const transaction_group& tg)
    : m_tg(tg)
    , m_mtx()
    , m_init_timestamp()
    , m_group()
    , m_dcs()
    , m_dcs_sz()
    , m_state(INITIALIZED)
    , m_timestamp(0)
    , m_prefer_to_commit(true)
    , m_ops()
    , m_deferred_2b()
{
    po6::threads::mutex::hold hold(&m_mtx);

    for (unsigned i = 0; i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
    {
        m_dcs_timestamps[i] = 0;
    }
}

transaction :: ~transaction() throw ()
{
}

const consus::transaction_group&
transaction :: state_key() const
{
    return m_tg;
}

bool
transaction :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_state == INITIALIZED || m_state == COLLECTED;
}

void
transaction :: begin(comm_id id, uint64_t nonce, uint64_t timestamp,
                     const paxos_group& group,
                     const std::vector<paxos_group_id>& dcs,
                     daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    internal_begin("client", timestamp, group, dcs, d);
    m_ops[0].set_client(id, nonce);
    work_state_machine(d);
}

void
transaction :: paxos_2a_begin(uint64_t seqno,
                              e::unpacker up,
                              std::auto_ptr<e::buffer>,
                              daemon* d)
{
    uint64_t timestamp;
    std::vector<paxos_group_id> dcs;
    up = up >> timestamp >> dcs;
    const paxos_group* group = d->get_config()->get_group(m_tg.group);

    if (seqno != 0 || up.error() || up.remain() || !group)
    {
        UNPACK_ERROR("paxos 2a::begin");
        po6::threads::mutex::hold hold(&m_mtx);
        avoid_commit_if_possible(d);
        return;
    }

    po6::threads::mutex::hold hold(&m_mtx);
    internal_begin("paxos 2a", timestamp, *group, dcs, d);
    work_state_machine(d);
}

void
transaction :: commit_record_begin(uint64_t seqno,
                                   e::unpacker up,
                                   e::compat::shared_ptr<e::buffer>,
                                   daemon* d)
{
    uint64_t timestamp;
    std::vector<paxos_group_id> dcs(m_dcs, m_dcs + m_dcs_sz);
    up = up >> timestamp >> dcs;
    const paxos_group* group = d->get_config()->get_group(m_tg.group);

    if (seqno != 0 || up.error() || up.remain() || !group)
    {
        UNPACK_ERROR("commit record::begin");
        avoid_commit_if_possible(d);
        return;
    }

    internal_begin("commit record", timestamp, *group, dcs, d);
}

void
transaction :: internal_begin(const char* source, uint64_t timestamp,
                              const paxos_group& group,
                              const std::vector<paxos_group_id>& dcs,
                              daemon* d)
{
    ensure_initialized();
    RETURN_IF_EXECUTED(0, source, "begin");

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << ".ops[0]: " << source << " initiated begin() in " << dcs.size() << " datacenters @ time " << timestamp;
    }

    operation op;
    comparison cmp;
    op.type = LOG_ENTRY_TX_BEGIN;
    cmp.type = true;

    if (!resize_to_hold(0) ||
        !m_ops[0].merge(op, cmp) ||
        (m_init_timestamp != 0 && m_init_timestamp != timestamp))
    {
        INVARIANT_VIOLATION("begin");
        avoid_commit_if_possible(d);
        return;
    }

    if (m_init_timestamp == 0)
    {
        LOG(INFO) << logid() << ".transaction_group: " << m_tg;

        for (size_t i = 0; i < dcs.size(); ++i)
        {
            LOG(INFO) << logid() << ".dc[" << i << "]: " << dcs[i]; // XXX prettify
        }

        LOG(INFO) << logid() << ".group " << group; // XXX prettify
        m_init_timestamp = timestamp;
        m_timestamp = std::max(m_timestamp, timestamp); // XXX replay
        m_group = group;

        for (unsigned i = 0; i < dcs.size() && i < CONSUS_MAX_REPLICATION_FACTOR; ++i)
        {
            m_dcs[i] = dcs[i];
        }

        m_dcs_sz = dcs.size();

        for (size_t i = 0; i < m_deferred_2b.size(); ++i)
        {
            LOG_IF(INFO, s_debug_mode) << logid() << " processing delayed durable notifaction " << m_deferred_2b[i].first << "/" << m_deferred_2b[i].second;
            internal_paxos_2b(m_deferred_2b[i].first, m_deferred_2b[i].second, d);
        }
    }
}

void
transaction :: read(comm_id id, uint64_t nonce, uint64_t seqno,
                    const e::slice& table,
                    const e::slice& key,
                    std::auto_ptr<e::buffer> _backing,
                    daemon* d)
{
    e::compat::shared_ptr<e::buffer> backing(_backing.release());
    po6::threads::mutex::hold hold(&m_mtx);
    internal_read("client", seqno, table, key, backing, d);
    m_ops[seqno].require_read_lock = true;
    m_ops[seqno].set_client(id, nonce);
    work_state_machine(d);
}

void
transaction :: paxos_2a_read(uint64_t seqno,
                             e::unpacker up,
                             std::auto_ptr<e::buffer> _backing,
                             daemon* d)
{
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    up = up >> table >> key >> timestamp;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("paxos 2a::read");
        po6::threads::mutex::hold hold(&m_mtx);
        avoid_commit_if_possible(d);
        return;
    }

    e::compat::shared_ptr<e::buffer> backing(_backing.release());
    po6::threads::mutex::hold hold(&m_mtx);
    internal_read("paxos 2a", seqno, table, key, backing, d);
    m_ops[seqno].require_read_lock = true;
    m_ops[seqno].read_lock_acquired = true;
    // XXX copy value
    work_state_machine(d);
}

#if 0
void
transaction :: commit_record_read(uint64_t seqno,
                                  e::unpacker up,
                                  std::auto_ptr<e::buffer> backing)
{
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    up = up >> table >> key >> timestamp;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("commit record::read");
        LOG_IF(INFO, s_debug_mode) << m_tg << " commit record read failed; invariants violated";
        m_avoid_commit_if_possible = true;
        return;
    }

    internal_read("commit record", seqno, table, key, backing);
    m_ops[seqno].require_lock = true;
    m_ops[seqno].require_fetch = true;
}
#endif

void
transaction :: internal_read(const char* source, uint64_t seqno,
                             const e::slice& table,
                             const e::slice& key,
                             e::compat::shared_ptr<e::buffer> backing,
                             daemon* d)
{
    ensure_initialized();

    if (s_debug_mode)
    {
        LOG(INFO) << m_tg << "[" << seqno << "] = " << source << " initiated read(\""
                  << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\")";
    }

    if (m_state > EXECUTING)
    {
        LOG_IF(INFO, s_debug_mode) << "NOP:  state > EXECUTING";
        return;
    }

    operation op;
    comparison cmp;
    op.type = LOG_ENTRY_TX_READ;
    cmp.type = true;
    op.table = table;
    cmp.table = true;
    op.key = key;
    cmp.key = true;
    op.backing = backing;

    if (!resize_to_hold(seqno) ||
        !m_ops[seqno].merge(op, cmp))
    {
        LOG_IF(INFO, s_debug_mode) << m_tg << " read failed; invariants violated";
        avoid_commit_if_possible(d);
        return;
    }
}

void
transaction :: write(comm_id id, uint64_t nonce, uint64_t seqno,
                     const e::slice& table,
                     const e::slice& key,
                     const e::slice& value,
                     std::auto_ptr<e::buffer> _backing,
                     daemon* d)
{
    e::compat::shared_ptr<e::buffer> backing(_backing.release());
    po6::threads::mutex::hold hold(&m_mtx);
    internal_write("client", seqno, table, key, value, backing, d);
    m_ops[seqno].require_write = true;
    m_ops[seqno].set_client(id, nonce);
    work_state_machine(d);
}

void
transaction :: paxos_2a_write(uint64_t seqno,
                              e::unpacker up,
                              std::auto_ptr<e::buffer> _backing,
                              daemon* d)
{
    e::slice table;
    e::slice key;
    e::slice value;
    up = up >> table >> key >> value;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("paxos 2a::write");
        po6::threads::mutex::hold hold(&m_mtx);
        avoid_commit_if_possible(d);
        return;
    }

    e::compat::shared_ptr<e::buffer> backing(_backing.release());
    po6::threads::mutex::hold hold(&m_mtx);
    internal_write("paxos 2a", seqno, table, key, value, backing, d);
    m_ops[seqno].require_write = true;
    m_ops[seqno].write_started = true;
    // XXX copy timestamp
    work_state_machine(d);
}

#if 0
void
transaction :: commit_record_write(uint64_t seqno,
                                   e::unpacker up,
                                   std::auto_ptr<e::buffer> backing)
{
    e::slice table;
    e::slice key;
    e::slice value;
    up = up >> table >> key >> value;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("commit record::write");
        LOG_IF(INFO, s_debug_mode) << m_tg << " commit record write failed; invariants violated";
        m_avoid_commit_if_possible = true;
        return;
    }

    internal_write("commit record", seqno, table, key, value, backing);
    m_ops[seqno].require_lock = true;
    m_ops[seqno].require_flush = true;
}
#endif

void
transaction :: internal_write(const char* source, uint64_t seqno,
                              const e::slice& table,
                              const e::slice& key,
                              const e::slice& value,
                              e::compat::shared_ptr<e::buffer> backing,
                              daemon* d)
{
    ensure_initialized();

    if (s_debug_mode)
    {
        LOG(INFO) << m_tg << "[" << seqno << "] = " << source << " initiated write(\""
                  << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\", \""
                  << e::strescape(value.str()) << "\")";
    }

    if (m_state > EXECUTING)
    {
        LOG_IF(INFO, s_debug_mode) << "NOP:  state > EXECUTING";
        return;
    }

    operation op;
    comparison cmp;
    op.type = LOG_ENTRY_TX_WRITE;
    cmp.type = true;
    op.table = table;
    cmp.table = true;
    op.key = key;
    cmp.key = true;
    op.value = value;
    cmp.value = true;
    op.backing = backing;

    if (!resize_to_hold(seqno) ||
        !m_ops[seqno].merge(op, cmp))
    {
        LOG_IF(INFO, s_debug_mode) << m_tg << " write failed; invariants violated";
        avoid_commit_if_possible(d);
        return;
    }
}

void
transaction :: prepare(comm_id id, uint64_t nonce, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    internal_end_of_transaction("client", "prepare", LOG_ENTRY_TX_PREPARE, seqno, d);
    m_ops[seqno].set_client(id, nonce);
    work_state_machine(d);
}

void
transaction :: paxos_2a_prepare(uint64_t seqno,
                                e::unpacker up,
                                std::auto_ptr<e::buffer>,
                                daemon* d)
{
    if (up.error() || up.remain())
    {
        UNPACK_ERROR("paxos 2a::prepare");
        po6::threads::mutex::hold hold(&m_mtx);
        avoid_commit_if_possible(d);
        return;
    }

    po6::threads::mutex::hold hold(&m_mtx);
    internal_end_of_transaction("paxos 2a", "prepare", LOG_ENTRY_TX_PREPARE, seqno, d);
    work_state_machine(d);
}

void
transaction :: commit_record_prepare(uint64_t seqno,
                                     e::unpacker up,
                                     e::compat::shared_ptr<e::buffer>,
                                     daemon* d)
{
    if (up.error() || up.remain())
    {
        UNPACK_ERROR("commit record::prepare");
        avoid_commit_if_possible(d);
        return;
    }

    internal_end_of_transaction("commit record", "prepare", LOG_ENTRY_TX_PREPARE, seqno, d);
    work_state_machine(d);
}

void
transaction :: abort(comm_id id, uint64_t nonce, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    internal_end_of_transaction("client", "abort", LOG_ENTRY_TX_ABORT, seqno, d);
    m_ops[seqno].set_client(id, nonce);
    work_state_machine(d);
}

void
transaction :: paxos_2a_abort(uint64_t seqno,
                              e::unpacker up,
                              std::auto_ptr<e::buffer>,
                              daemon* d)
{
    if (up.error() || up.remain())
    {
        UNPACK_ERROR("paxos 2a::abort");
        po6::threads::mutex::hold hold(&m_mtx);
        avoid_commit_if_possible(d);
        return;
    }

    po6::threads::mutex::hold hold(&m_mtx);
    internal_end_of_transaction("paxos 2a", "abort", LOG_ENTRY_TX_ABORT, seqno, d);
    work_state_machine(d);
}

void
transaction :: internal_end_of_transaction(const char* source, const char* func, log_entry_t let, uint64_t seqno, daemon* d)
{
    ensure_initialized();
    RETURN_IF_EXECUTED(seqno, source, func);

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << ".ops[" << seqno << "]: " << source << " initiated " << func << "()";
    }

    operation op;
    comparison cmp;
    op.type = let;
    cmp.type = true;

    if (!resize_to_hold(seqno) ||
        !m_ops[seqno].merge(op, cmp))
    {
        INVARIANT_VIOLATION(func);
        avoid_commit_if_possible(d);
        return;
    }
}

void
transaction :: paxos_2a(uint64_t seqno,
                        log_entry_t t,
                        e::unpacker up,
                        std::auto_ptr<e::buffer> backing,
                        daemon* d)
{
    assert(is_paxos_2a_log_entry(t));
    assert(!up.error());

    switch (t)
    {
        case LOG_ENTRY_TX_BEGIN:
            return paxos_2a_begin(seqno, up, backing, d);
        case LOG_ENTRY_TX_READ:
            return paxos_2a_read(seqno, up, backing, d);
        case LOG_ENTRY_TX_WRITE:
            return paxos_2a_write(seqno, up, backing, d);
        case LOG_ENTRY_TX_PREPARE:
            return paxos_2a_prepare(seqno, up, backing, d);
        case LOG_ENTRY_TX_ABORT:
            return paxos_2a_abort(seqno, up, backing, d);
        case LOG_ENTRY_LOCAL_VOTE_1A:
        case LOG_ENTRY_LOCAL_VOTE_2A:
        case LOG_ENTRY_LOCAL_LEARN:
        case LOG_ENTRY_GLOBAL_PROPOSE:
        case LOG_ENTRY_GLOBAL_VOTE_1A:
        case LOG_ENTRY_GLOBAL_VOTE_2A:
        case LOG_ENTRY_GLOBAL_VOTE_2B:
        case LOG_ENTRY_CONFIG:
        case LOG_ENTRY_NOP:
        default:
            ::abort();
    }
}

void
transaction :: paxos_2b(comm_id id, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    internal_paxos_2b(id, seqno, d);
    work_state_machine(d);
}

void
transaction :: internal_paxos_2b(comm_id id, uint64_t seqno, daemon* d)
{
    ensure_initialized();

    if (m_init_timestamp == 0)
    {
        m_deferred_2b.push_back(std::make_pair(id, seqno));
        LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << seqno << "]: durable notification deferred until begin() action received";
        return;
    }

    unsigned idx = m_group.index(id);

    if (idx >= m_group.members_sz)
    {
        LOG(ERROR) << logid() << ".ops[" << seqno << "]: " << id << " is misrepresenting itself as a member of " << m_group.id;
        return;
    }

    if (!resize_to_hold(seqno))
    {
        INVARIANT_VIOLATION("durable notification");
        avoid_commit_if_possible(d);
        return;
    }

    bool already_durable = m_ops[seqno].durable[idx];
    m_ops[seqno].durable[idx] = true;

    if (!already_durable && s_debug_mode)
    {
        if (d->m_us.id == id)
        {
            LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << seqno << "]: durable notification from this server";
        }
        else
        {
            LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << seqno << "]: durable notification from " << id;
        }

        std::ostringstream ostr;

        for (size_t i = 0; i < m_group.members_sz; ++i)
        {
            ostr << (m_ops[seqno].durable[i] ? "1" : "0");
        }

        LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << seqno << "]: durability across group: " << ostr.str();
    }
}

void
transaction :: commit_record(e::slice commit_record, std::auto_ptr<e::buffer> _backing, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (!m_ops.empty())
    {
        work_state_machine(d);
        return;
    }

    e::unpacker up(commit_record);
    e::compat::shared_ptr<e::buffer> backing(_backing.release());

    while (!up.error() && up.remain())
    {
        e::slice entry;
        up = up >> entry;

        if (up.error())
        {
            break;
        }

        log_entry_t type;
        transaction_group tg;
        uint64_t seqno;
        e::unpacker eup(entry);
        eup = eup >> type >> tg >> seqno;

        if (eup.error())
        {
            up = up.error_out();
            break;
        }

        switch (type)
        {
            case LOG_ENTRY_TX_BEGIN:
                commit_record_begin(seqno, eup, backing, d);
                break;
#if 0
            case LOG_ENTRY_TX_READ:
                commit_record_read(seqno, eup, backing);
                break;
            case LOG_ENTRY_TX_WRITE:
                commit_record_write(seqno, eup, backing);
                break;
#endif
            case LOG_ENTRY_TX_PREPARE:
                commit_record_prepare(seqno, eup, backing, d);
                break;
            case LOG_ENTRY_TX_ABORT:
            case LOG_ENTRY_CONFIG:
            case LOG_ENTRY_LOCAL_VOTE_1A:
            case LOG_ENTRY_LOCAL_VOTE_2A:
            case LOG_ENTRY_LOCAL_LEARN:
            case LOG_ENTRY_GLOBAL_PROPOSE:
            case LOG_ENTRY_GLOBAL_VOTE_1A:
            case LOG_ENTRY_GLOBAL_VOTE_2A:
            case LOG_ENTRY_GLOBAL_VOTE_2B:
            case LOG_ENTRY_NOP:
            default:
                LOG(ERROR) << "commit record has " << type << " entry";
                up = up.error_out();
                continue;
        }
    }

    if (up.error())
    {
        ::abort(); // XXX
    }

    if (m_ops.empty() || m_ops.back().type != LOG_ENTRY_TX_PREPARE)
    {
        ::abort(); // XXX
    }

    work_state_machine(d);
}

void
transaction :: callback(uint64_t seqno, daemon* d)
{
    {
        po6::threads::mutex::hold hold(&m_mtx);

        if (seqno >= m_ops.size())
        {
            return;
        }

        m_ops[seqno].log_write_durable = true;
    }

    paxos_2b(d->m_us.id, seqno, d);
}

void
transaction :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

void
transaction :: kvs_rd_locked(uint64_t seqno,
                             consus_returncode rc,
                             uint64_t timestamp,
                             const e::slice& value,
                             std::auto_ptr<e::buffer> backing,
                             daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    if (m_ops[seqno].require_read_lock)
    {
        m_ops[seqno].read_lock_acquired = true;
        m_ops[seqno].rc = rc;
        m_ops[seqno].timestamp = timestamp;
        m_ops[seqno].value = value;
        m_ops[seqno].read_backing = e::compat::shared_ptr<e::buffer>(backing.release());
        m_ops[seqno].read_timestamp = 0;

        if (m_state == EXECUTING)
        {
            m_timestamp = std::max(m_timestamp, timestamp + 1); // XXX replay
        }
    }

    work_state_machine(d);
}

void
transaction :: kvs_rd_unlocked(uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].read_lock_released = true;
    work_state_machine(d);
}

void
transaction :: kvs_wr_begun(uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].write_started = true;
    m_ops[seqno].write_timestamp = 0;
    work_state_machine(d);
}

void
transaction :: kvs_wr_finished(uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].write_finished = true;
    work_state_machine(d);
}

std::string
transaction :: logid()
{
    return transaction_group::log(m_tg);
}

void
transaction :: ensure_initialized()
{
    if (m_state == INITIALIZED)
    {
        m_state = EXECUTING;
    }
}

void
transaction :: work_state_machine(daemon* d)
{
    switch (m_state)
    {
        case INITIALIZED:
            return;
        case EXECUTING:
            return work_state_machine_executing(d);
        case LOCAL_COMMIT_VOTE:
            return work_state_machine_local_commit_vote(d);
        case GLOBAL_COMMIT_VOTE:
            return work_state_machine_global_commit_vote(d);
        case COMMITTED:
            return work_state_machine_committed(d);
        case ABORTED:
            return work_state_machine_aborted(d);
        case TERMINATED:
        case COLLECTED:
            break;
        default:
            ::abort();
    }
}

void
transaction :: work_state_machine_executing(daemon* d)
{
    size_t done = 0;

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type == LOG_ENTRY_NOP)
        {
            continue;
        }

        if (m_ops[i].require_read_lock && !m_ops[i].read_lock_acquired)
        {
            acquire_read_lock(i, d);
            continue;
        }

        if (m_ops[i].require_write && !m_ops[i].write_started)
        {
            begin_write(i, d);
            continue;
        }

        if (m_ops[i].log_write_durable)
        {
            send_paxos_2b(i, d);
        }

        if (!is_durable(i))
        {
            send_paxos_2a(i, d);

            if (!m_ops[i].log_write_issued)
            {
                std::string le = generate_log_entry(i);
                d->callback_when_durable(le, m_tg, i);
                m_ops[i].log_write_issued = true;
            }

            continue;
        }

        if (m_ops[i].client != comm_id())
        {
            send_response(&m_ops[i], d);
        }

        ++done;
    }

    if (done == m_ops.size() && !m_ops.empty() &&
        (m_ops.back().type == LOG_ENTRY_TX_PREPARE ||
         m_ops.back().type == LOG_ENTRY_TX_ABORT))
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " finished execuing all operations; transitioning to DATA CENTER VOTE state";
        m_state = LOCAL_COMMIT_VOTE;
        return work_state_machine(d);
    }
}

void
transaction :: work_state_machine_local_commit_vote(daemon* d)
{
    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        send_paxos_2a(i, d);
        send_paxos_2b(i, d);
    }

    daemon::local_voter_map_t::state_reference lvsr;
    local_voter* lv = d->m_local_voters.get_or_create_state(m_tg, &lvsr);
    assert(lv);
    lv->set_preferred_vote(m_prefer_to_commit && m_ops.back().type == LOG_ENTRY_TX_PREPARE
                           ? CONSUS_VOTE_COMMIT : CONSUS_VOTE_ABORT);
    uint64_t outcome;

    if (!lv->outcome(&outcome))
    {
        lv->externally_work_state_machine(d);
        return;
    }

    lv = NULL;
    lvsr.release();
    assert(m_dcs_sz >= 1);
    bool single_dc = m_dcs_sz == 1;

    if (outcome == CONSUS_VOTE_COMMIT)
    {
        if (single_dc)
        {
            LOG_IF(INFO, s_debug_mode) << logid() << " data center vote chose COMMIT; transitioning to COMMITTED state";
            m_state = COMMITTED;
            record_commit(d);
        }
        else
        {
            LOG_IF(INFO, s_debug_mode) << logid() << " data center vote chose COMMIT; transitioning to GLOBAL VOTE state";
            m_state = GLOBAL_COMMIT_VOTE;
        }
    }
    else if (outcome == CONSUS_VOTE_ABORT)
    {
        // if single dc or only aborting in originating data center
        if (single_dc || m_tg.group == m_tg.txid.group)
        {
            LOG_IF(INFO, s_debug_mode) << logid() << " data center vote chose ABORT; transitioning to ABORTED state";
            m_state = ABORTED;
            record_abort(d);
        }
        else
        {
            LOG_IF(INFO, s_debug_mode) << logid() << " data center vote chose ABORT; transitioning to GLOBAL VOTE state";
            m_state = GLOBAL_COMMIT_VOTE;
        }
    }
    else
    {
        LOG(ERROR) << logid() << " data center commit failed; invariants violated";
        return;
    }

    return work_state_machine(d);
}

void
transaction :: work_state_machine_global_commit_vote(daemon* d)
{
    std::string commit_record;
    e::packer pa(&commit_record);

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        std::string log_entry = generate_log_entry(i);
        pa = pa << e::slice(log_entry);
    }

    const configuration* c = d->get_config();
    const uint64_t now = po6::monotonic_time();

    for (unsigned i = 0; i < m_dcs_sz; ++i)
    {
        if (m_dcs[i] != m_group.id)
        {
            const paxos_group* g = c->get_group(m_dcs[i]);

            if (!g)
            {
                // XXX what to do here?
                ::abort();
            }

            for (unsigned j = 0; j < g->members_sz; ++j)
            {
                // XXX coordinator failure sensitive
                if (c->get_state(g->members[j]) == txman_state::ONLINE &&
                    m_dcs_timestamps[i] + d->resend_interval() < now)
                {
                    transaction_group tg(g->id, m_tg.txid);
                    const size_t sz = BUSYBEE_HEADER_SIZE
                                    + pack_size(COMMIT_RECORD)
                                    + pack_size(tg)
                                    + pack_size(e::slice(commit_record));
                    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                    msg->pack_at(BUSYBEE_HEADER_SIZE)
                        << COMMIT_RECORD << tg << e::slice(commit_record);
                    d->send(g->members[j], msg);
                    m_dcs_timestamps[i] = now;
                    break;
                }
            }
        }
    }

    daemon::global_voter_map_t::state_reference gvsr;
    global_voter* gv = d->m_global_voters.get_or_create_state(m_tg, &gvsr);

    if (!gv->initialized())
    {
        daemon::local_voter_map_t::state_reference lvsr;
        local_voter* lv = d->m_local_voters.get_or_create_state(m_tg, &lvsr);
        assert(lv);
        uint64_t v = lv->outcome();
        gv->init(v, m_dcs, m_dcs_sz, d);
    }

    gv->externally_work_state_machine(d);
    uint64_t outcome;

    if (!gv->outcome(&outcome))
    {
        return;
    }

    if (outcome == CONSUS_VOTE_COMMIT)
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " global vote chose COMMIT; transitioning to COMMITTED state";
        m_state = COMMITTED;
        record_commit(d);
    }
    else if (outcome == CONSUS_VOTE_ABORT)
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " global vote chose ABORT; transitioning to ABORTED state";
        m_state = ABORTED;
        record_abort(d);
    }
    else
    {
        LOG(ERROR) << m_tg << " global commit failed; invariants violated";
        return;
    }

    return work_state_machine(d);
}

void
transaction :: work_state_machine_committed(daemon* d)
{
    size_t non_nop = 0;
    size_t done = 0;

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type == LOG_ENTRY_NOP)
        {
            continue;
        }

        ++non_nop;

        if (m_ops[i].require_read_lock && !m_ops[i].read_lock_released)
        {
            release_read_lock(i, d);
            continue;
        }

        if (m_ops[i].require_write && !m_ops[i].write_finished)
        {
            finish_write(i, d);
            continue;
        }

        ++done;
    }

    if (done == non_nop)
    {
        send_tx_commit(d);
        LOG_IF(INFO, s_debug_mode) << logid() << " transitioning to TERMINATED state";
        m_state = TERMINATED;
        return work_state_machine(d);
    }
}

void
transaction :: work_state_machine_aborted(daemon* d)
{
    size_t non_nop = 0;
    size_t done = 0;

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type == LOG_ENTRY_NOP)
        {
            continue;
        }

        ++non_nop;

        if (m_ops[i].require_read_lock && !m_ops[i].read_lock_released)
        {
            release_read_lock(i, d);
            continue;
        }

        if (m_ops[i].require_write && !m_ops[i].write_finished)
        {
            cancel_write(i, d);
            continue;
        }

        ++done;
    }

    if (done == non_nop)
    {
        send_tx_abort(d);
        LOG_IF(INFO, s_debug_mode) << logid() << " transitioning to TERMINATED state";
        m_state = TERMINATED;
        return work_state_machine(d);
    }
}

void
transaction :: avoid_commit_if_possible(daemon* d)
{
    m_prefer_to_commit = false;
    daemon::local_voter_map_t::state_reference lvsr;
    local_voter* lv = d->m_local_voters.get_or_create_state(m_tg, &lvsr);
    assert(lv);
    lv->set_preferred_vote(CONSUS_VOTE_ABORT);
}

bool
transaction :: is_durable(uint64_t seqno)
{
    if (seqno >= m_ops.size())
    {
        return false;
    }

    unsigned c = 0;

    for (unsigned i = 0; i < m_group.members_sz; ++i)
    {
        if (m_ops[seqno].durable[i])
        {
            ++c;
        }
    }

    return c >= m_group.quorum();
}

bool
transaction :: resize_to_hold(uint64_t seqno)
{
    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type != LOG_ENTRY_NOP &&
            m_ops[i].type >= LOG_ENTRY_TX_PREPARE &&
            i < seqno)
        {
            return false;
        }
    }

    if (m_ops.size() <= seqno && m_state == EXECUTING)
    {
        m_ops.resize(seqno + 1);
    }
    else if (m_ops.size() <= seqno && m_state > EXECUTING)
    {
        return false;
    }

    return true;
}

void
transaction :: acquire_read_lock(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].read_timestamp + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].read_timestamp = now;
    const e::slice& table(m_ops[seqno].table);
    const e::slice& key(m_ops[seqno].key);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RD_LOCK)
                    + pack_size(m_tg)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RD_LOCK << m_tg << seqno << table << key;
    d->send(d->get_config()->choose_kvs(m_group.dc), msg);
}

void
transaction :: release_read_lock(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].read_timestamp + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].read_timestamp = now;
    const e::slice& table(m_ops[seqno].table);
    const e::slice& key(m_ops[seqno].key);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RD_UNLOCK)
                    + pack_size(m_tg)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RD_UNLOCK << m_tg << seqno << table << key;
    d->send(d->get_config()->choose_kvs(m_group.dc), msg);
}

void
transaction :: begin_write(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].write_timestamp + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].write_timestamp = now;
    const e::slice& table(m_ops[seqno].table);
    const e::slice& key(m_ops[seqno].key);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_BEGIN)
                    + pack_size(m_tg)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_WR_BEGIN << m_tg << seqno << table << key;
    d->send(d->get_config()->choose_kvs(m_group.dc), msg);
}

void
transaction :: finish_write(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].write_timestamp + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].write_timestamp = now;
    const e::slice& table(m_ops[seqno].table);
    const e::slice& key(m_ops[seqno].key);
    const e::slice& value(m_ops[seqno].value);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_FINISH)
                    + pack_size(m_tg)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key)
                    + sizeof(uint64_t)
                    + pack_size(value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_WR_FINISH << m_tg << seqno << table << key << m_timestamp << value;
    d->send(d->get_config()->choose_kvs(m_group.dc), msg);
}

void
transaction :: cancel_write(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].write_timestamp + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].write_timestamp = now;
    const e::slice& table(m_ops[seqno].table);
    const e::slice& key(m_ops[seqno].key);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_WR_CANCEL)
                    + pack_size(m_tg)
                    + sizeof(uint64_t)
                    + pack_size(table)
                    + pack_size(key);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_WR_CANCEL << m_tg << seqno << table << key;
    d->send(d->get_config()->choose_kvs(m_group.dc), msg);
}

std::string
transaction :: generate_log_entry(uint64_t seqno)
{
    assert(seqno < m_ops.size());
    std::string entry;
    e::packer pa(&entry);
    std::vector<paxos_group_id> dcs(m_dcs, m_dcs + m_dcs_sz);
    operation* op = &m_ops[seqno];

    switch (m_ops[seqno].type)
    {
        case LOG_ENTRY_TX_BEGIN:
            pa << LOG_ENTRY_TX_BEGIN << m_tg << seqno << m_init_timestamp << dcs;
            break;
        case LOG_ENTRY_TX_READ:
            pa << LOG_ENTRY_TX_READ << m_tg << seqno << op->table << op->key << op->timestamp;
            break;
        case LOG_ENTRY_TX_WRITE:
            pa << LOG_ENTRY_TX_WRITE << m_tg << seqno << op->table << op->key << op->value;
            break;
        case LOG_ENTRY_TX_PREPARE:
            pa << LOG_ENTRY_TX_PREPARE << m_tg << seqno;
            break;
        case LOG_ENTRY_TX_ABORT:
            pa << LOG_ENTRY_TX_ABORT << m_tg << seqno;
            break;
        case LOG_ENTRY_LOCAL_VOTE_1A:
        case LOG_ENTRY_LOCAL_VOTE_2A:
        case LOG_ENTRY_LOCAL_LEARN:
        case LOG_ENTRY_GLOBAL_PROPOSE:
        case LOG_ENTRY_GLOBAL_VOTE_1A:
        case LOG_ENTRY_GLOBAL_VOTE_2A:
        case LOG_ENTRY_GLOBAL_VOTE_2B:
        case LOG_ENTRY_CONFIG:
        case LOG_ENTRY_NOP:
        default:
            ::abort();
    }

    return entry;
}

void
transaction :: record_commit(daemon* d)
{
    d->m_dispositions.put(m_tg, CONSUS_VOTE_COMMIT);
}

void
transaction :: record_abort(daemon* d)
{
    d->m_dispositions.put(m_tg, CONSUS_VOTE_ABORT);
}

void
transaction :: send_paxos_2a(uint64_t i, daemon* d)
{
    std::string le = generate_log_entry(i);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(TXMAN_PAXOS_2A)
                    + pack_size(e::slice(le));
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << TXMAN_PAXOS_2A << e::slice(le);
    send_to_nondurable(i, msg, m_ops[i].paxos_timestamps, d);
}

void
transaction :: send_paxos_2b(uint64_t i, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(TXMAN_PAXOS_2B)
                    + pack_size(m_tg)
                    + sizeof(uint64_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE) << TXMAN_PAXOS_2B << m_tg << i;
    send_to_group(msg, m_ops[i].paxos_2b_timestamps, d);
}

void
transaction :: send_response(operation* op, daemon* d)
{
    assert(op->client != comm_id());

    switch (op->type)
    {
        case LOG_ENTRY_TX_BEGIN:
            return send_tx_begin(op, d);
        case LOG_ENTRY_TX_READ:
            return send_tx_read(op, d);
        case LOG_ENTRY_TX_WRITE:
            return send_tx_write(op, d);
        case LOG_ENTRY_TX_PREPARE:
        case LOG_ENTRY_TX_ABORT:
			// only sent after a vote
            return;
        case LOG_ENTRY_LOCAL_VOTE_1A:
        case LOG_ENTRY_LOCAL_VOTE_2A:
        case LOG_ENTRY_LOCAL_LEARN:
        case LOG_ENTRY_GLOBAL_PROPOSE:
        case LOG_ENTRY_GLOBAL_VOTE_1A:
        case LOG_ENTRY_GLOBAL_VOTE_2A:
        case LOG_ENTRY_GLOBAL_VOTE_2B:
        case LOG_ENTRY_CONFIG:
        case LOG_ENTRY_NOP:
        default:
            LOG(ERROR) << "trying to send client response for " << op->type;
            return;
    }
}

void
transaction :: send_tx_begin(operation* op, daemon* d)
{
    std::vector<comm_id> ids(m_group.members, m_group.members + m_group.members_sz);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(m_tg)
                    + ::pack_size(ids);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << op->nonce << m_tg.txid << ids;
    d->send(op->client, msg);
    op->client = comm_id();
}

void
transaction :: send_tx_read(operation* op, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(op->rc)
                    + sizeof(uint64_t)
                    + pack_size(op->value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE
        << op->nonce
        << op->rc
        << op->timestamp
        << op->value;
    d->send(op->client, msg);
    op->client = comm_id();
}

void
transaction :: send_tx_write(operation* op, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_SUCCESS);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << op->nonce << CONSUS_SUCCESS;
    d->send(op->client, msg);
    op->client = comm_id();
}

void
transaction :: send_tx_commit(daemon* d)
{
    if (m_ops.back().client == comm_id())
    {
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_SUCCESS);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << m_ops.back().nonce << CONSUS_SUCCESS;
    d->send(m_ops.back().client, msg);
}

void
transaction :: send_tx_abort(daemon* d)
{
    if (m_ops.back().client == comm_id())
    {
        return;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_ABORTED);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << m_ops.back().nonce << CONSUS_ABORTED;
    d->send(m_ops.back().client, msg);
}

void
transaction :: send_to_group(std::auto_ptr<e::buffer> msg, uint64_t timestamps[CONSUS_MAX_REPLICATION_FACTOR], daemon* d)
{
    const uint64_t now = po6::monotonic_time();

    for (unsigned i = 0; i < m_group.members_sz; ++i)
    {
        if (m_group.members[i] == d->m_us.id ||
            timestamps[i] + d->resend_interval() > now)
        {
            continue;
        }

        std::auto_ptr<e::buffer> m(msg->copy());
        d->send(m_group.members[i], m);
        timestamps[i] = now;
    }
}

void
transaction :: send_to_nondurable(uint64_t seqno, std::auto_ptr<e::buffer> msg, uint64_t timestamps[CONSUS_MAX_REPLICATION_FACTOR], daemon* d)
{
    if (seqno >= m_ops.size())
    {
        return;
    }

    const uint64_t now = po6::monotonic_time();

    for (unsigned i = 0; i < m_group.members_sz; ++i)
    {
        if (m_group.members[i] == d->m_us.id ||
            m_ops[seqno].durable[i] ||
            timestamps[i] + d->resend_interval() > now)
        {
            continue;
        }

        std::auto_ptr<e::buffer> m(msg->copy());
        d->send(m_group.members[i], m);
        timestamps[i] = now;
    }
}
