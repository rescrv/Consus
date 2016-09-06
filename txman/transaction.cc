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

#define CLIENT_RETURN_IF_EXECUTED(I, F, N, A) \
    do { \
    if (m_state == ABORTED || m_decision == ABORTED) \
    { \
        LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << (I) << "]: responding with \"aborted\""; \
        send_aborted_response((F), (N), d); \
        return; \
    } \
    else if (m_state == COMMITTED || m_decision == COMMITTED) \
    { \
        LOG_IF(INFO, s_debug_mode) << logid() << ".ops[" << (I) << "]: responding with \"committed\""; \
        send_committed_response((F), (N), d); \
        return; \
    } \
    else \
    { \
        INTERNAL_RETURN_IF_EXECUTED((I), "client", (A)); \
    } \
    } while (0)

#define INTERNAL_RETURN_IF_EXECUTED(I, S, A) \
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

    // locking
    bool require_lock;
    bool lock_acquired;
    bool lock_released;
    uint64_t lock_nonce;

    // reading
    bool require_read;
    bool read_done;
    std::string read_backing;
    uint64_t read_nonce;

    // writing
    bool require_write;
    bool write_done;
    uint64_t write_nonce;

    // verify read
    bool require_verify_read;
    bool verify_read_done;
    uint64_t verify_read_nonce;

    // verify write
    bool require_verify_write;
    bool verify_write_done;
    uint64_t verify_write_nonce;

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
    , require_lock(false)
    , lock_acquired(false)
    , lock_released(false)
    , lock_nonce(0)
    , require_read(false)
    , read_done(false)
    , read_backing()
    , read_nonce()
    , require_write(false)
    , write_done(false)
    , write_nonce(0)
    , require_verify_read(false)
    , verify_read_done(false)
    , verify_read_nonce()
    , require_verify_write(false)
    , verify_write_done(false)
    , verify_write_nonce()
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
    , m_decision(INITIALIZED)
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
    CLIENT_RETURN_IF_EXECUTED(0, id, nonce, "begin");
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
    INTERNAL_RETURN_IF_EXECUTED(0, source, "begin");

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
        LOG_IF(INFO, s_debug_mode) << logid() << ".transaction_group: " << m_tg;

        for (size_t i = 0; i < dcs.size(); ++i)
        {
            LOG_IF(INFO, s_debug_mode) << logid() << ".dc[" << i << "]: " << dcs[i]; // XXX prettify
        }

        LOG_IF(INFO, s_debug_mode) << logid() << ".group " << group; // XXX prettify
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
    CLIENT_RETURN_IF_EXECUTED(seqno, id, nonce, "read");
    internal_read("client", seqno, table, key, backing, d);
    m_ops[seqno].require_lock = true;
    m_ops[seqno].require_read = true;
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
    m_ops[seqno].require_lock = true;
    m_ops[seqno].lock_acquired = true;
    m_ops[seqno].timestamp = timestamp;
    work_state_machine(d);
}

void
transaction :: commit_record_read(uint64_t seqno,
                                  e::unpacker up,
                                  e::compat::shared_ptr<e::buffer> backing,
                                  daemon* d)
{
    e::slice table;
    e::slice key;
    uint64_t timestamp;
    up = up >> table >> key >> timestamp;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("commit record::read");
        LOG_IF(INFO, s_debug_mode) << logid() << " commit record read failed; invariants violated";
        avoid_commit_if_possible(d);
        return;
    }

    internal_read("commit record", seqno, table, key, backing, d);
    m_ops[seqno].require_lock = true;
    m_ops[seqno].timestamp = timestamp;
    m_ops[seqno].require_verify_read = true;
}

void
transaction :: internal_read(const char* source, uint64_t seqno,
                             const e::slice& table,
                             const e::slice& key,
                             e::compat::shared_ptr<e::buffer> backing,
                             daemon* d)
{
    ensure_initialized();
    INTERNAL_RETURN_IF_EXECUTED(seqno, source, "read");

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << "[" << seqno << "] = " << source << " initiated read(\""
                  << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\")";
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
        LOG_IF(INFO, s_debug_mode) << logid() << " read failed; invariants violated";
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
    CLIENT_RETURN_IF_EXECUTED(seqno, id, nonce, "write");
    internal_write("client", seqno, table, key, value, backing, d);
    m_ops[seqno].require_lock = true;
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
    m_ops[seqno].require_lock = true;
    m_ops[seqno].lock_acquired = true;
    m_ops[seqno].require_write = true;
    work_state_machine(d);
}

void
transaction :: commit_record_write(uint64_t seqno,
                                   e::unpacker up,
                                   e::compat::shared_ptr<e::buffer> backing,
                                   daemon* d)
{
    e::slice table;
    e::slice key;
    e::slice value;
    up = up >> table >> key >> value;

    if (up.error() || up.remain())
    {
        UNPACK_ERROR("commit record::write");
        LOG_IF(INFO, s_debug_mode) << logid() << " commit record write failed; invariants violated";
        avoid_commit_if_possible(d);
        return;
    }

    internal_write("commit record", seqno, table, key, value, backing, d);
    m_ops[seqno].require_lock = true;
    m_ops[seqno].require_verify_write = true;
    m_ops[seqno].require_write = true;
}

void
transaction :: internal_write(const char* source, uint64_t seqno,
                              const e::slice& table,
                              const e::slice& key,
                              const e::slice& value,
                              e::compat::shared_ptr<e::buffer> backing,
                              daemon* d)
{
    ensure_initialized();
    INTERNAL_RETURN_IF_EXECUTED(seqno, source, "write");

    if (s_debug_mode)
    {
        LOG(INFO) << logid() << "[" << seqno << "] = " << source << " initiated write(\""
                  << e::strescape(table.str()) << "\", \""
                  << e::strescape(key.str()) << "\", \""
                  << e::strescape(value.str()) << "\")";
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
        LOG_IF(INFO, s_debug_mode) << logid() << " write failed; invariants violated";
        avoid_commit_if_possible(d);
        return;
    }
}

void
transaction :: prepare(comm_id id, uint64_t nonce, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    CLIENT_RETURN_IF_EXECUTED(seqno, id, nonce, "prepare");
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
    CLIENT_RETURN_IF_EXECUTED(seqno, id, nonce, "abort");
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
    INTERNAL_RETURN_IF_EXECUTED(seqno, source, func);

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
            case LOG_ENTRY_TX_READ:
                commit_record_read(seqno, eup, backing, d);
                break;
            case LOG_ENTRY_TX_WRITE:
                commit_record_write(seqno, eup, backing, d);
                break;
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
transaction :: callback_durable(uint64_t seqno, daemon* d)
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
transaction :: callback_locked(consus_returncode rc, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_LESS_DURABLE);// XXX unsafe

    if (m_ops[seqno].require_lock && !m_ops[seqno].lock_acquired)
    {
        m_ops[seqno].lock_nonce = 0;
        m_ops[seqno].lock_acquired = true;
        work_state_machine(d);
    }
}

void
transaction :: callback_unlocked(consus_returncode rc, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_LESS_DURABLE);// XXX unsafe

    if (m_ops[seqno].require_lock && !m_ops[seqno].lock_released)
    {
        m_ops[seqno].lock_nonce = 0;
        m_ops[seqno].lock_released = true;
        work_state_machine(d);
    }
}

void
transaction :: callback_read(consus_returncode rc, uint64_t timestamp, const e::slice& value,
                             uint64_t seqno, daemon*d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_NOT_FOUND);// XXX unsafe

    if (m_ops[seqno].require_read && !m_ops[seqno].read_done)
    {
        m_ops[seqno].read_nonce = 0;
        m_ops[seqno].read_done = true;
        m_ops[seqno].read_backing.assign(value.cdata(), value.size());
        m_ops[seqno].timestamp = timestamp;
        m_ops[seqno].value = e::slice(m_ops[seqno].read_backing);
        m_ops[seqno].rc = rc;
        work_state_machine(d);
    }
}

void
transaction :: callback_write(consus_returncode rc, uint64_t seqno, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_LESS_DURABLE);// XXX unsafe

    if (m_ops[seqno].require_write && !m_ops[seqno].write_done)
    {
        m_ops[seqno].write_nonce = 0;
        m_ops[seqno].write_done = true;
        work_state_machine(d);
    }
}

void
transaction :: callback_verify_read(consus_returncode rc, uint64_t timestamp, const e::slice&,
                                    uint64_t seqno, daemon*d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_NOT_FOUND);// XXX unsafe

    if (m_ops[seqno].require_verify_read && !m_ops[seqno].verify_read_done)
    {
        m_ops[seqno].verify_read_nonce = 0;
        m_ops[seqno].verify_read_done = true;

        if (timestamp > m_ops[seqno].timestamp)
        {
            avoid_commit_if_possible(d);
        }

        work_state_machine(d);
    }
}

void
transaction :: callback_verify_write(consus_returncode rc, uint64_t timestamp, const e::slice&,
                                     uint64_t seqno, daemon*d)
{
    po6::threads::mutex::hold hold(&m_mtx);

    if (seqno >= m_ops.size())
    {
        return;
    }

    assert(rc == CONSUS_SUCCESS || rc == CONSUS_NOT_FOUND);// XXX unsafe

    if (m_ops[seqno].require_verify_write && !m_ops[seqno].verify_write_done)
    {
        m_ops[seqno].verify_write_nonce = 0;
        m_ops[seqno].verify_write_done = true;

        if (timestamp >= m_timestamp)
        {
            avoid_commit_if_possible(d);
        }

        work_state_machine(d);
    }
}

void
transaction :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
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

        if (m_ops[i].require_lock && !m_ops[i].lock_acquired)
        {
            acquire_lock(i, d);
            continue;
        }

        if (m_ops[i].require_read && !m_ops[i].read_done)
        {
            start_read(i, d);
            continue;
        }

        if (m_ops[i].require_verify_read && !m_ops[i].verify_read_done)
        {
            start_verify_read(i, d);
            continue;
        }

        if (m_ops[i].require_verify_write && !m_ops[i].verify_write_done)
        {
            start_verify_write(i, d);
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

    daemon::local_voter_map_t::state_reference lvsr;
    local_voter* lv = d->m_local_voters.get_or_create_state(m_tg, &lvsr);
    assert(lv);
    uint64_t outcome;

    if (lv->outcome(&outcome))
    {
        LOG_IF(INFO, s_debug_mode) << logid() << " short-circuiting operations to abort (possible deadlock prevention)";
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
        LOG(ERROR) << logid() << " global commit failed; invariants violated";
        return;
    }

    return work_state_machine(d);
}

void
transaction :: work_state_machine_committed(daemon* d)
{
    size_t non_nop = 0;
    size_t done = 0;
    m_decision = COMMITTED;

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type == LOG_ENTRY_NOP)
        {
            continue;
        }

        ++non_nop;

        if (m_ops[i].require_write && !m_ops[i].write_done)
        {
            start_write(i, d);
            continue;
        }

        if (m_ops[i].require_lock && !m_ops[i].lock_released)
        {
            release_lock(i, d);
            continue;
        }

        if (m_ops[i].client != comm_id())
        {
            send_committed_response(&m_ops[i], d);
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
    m_decision = ABORTED;

    for (size_t i = 0; i < m_ops.size(); ++i)
    {
        if (m_ops[i].type == LOG_ENTRY_NOP)
        {
            continue;
        }

        ++non_nop;

        if (m_ops[i].require_lock && !m_ops[i].lock_released)
        {
            release_lock(i, d);
            continue;
        }

        if (m_ops[i].client != comm_id())
        {
            send_aborted_response(&m_ops[i], d);
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
transaction :: acquire_lock(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.lock_nonce == 0)
    {
        daemon::lock_op_map_t::state_reference sr;
        kvs_lock_op* kv = d->create_lock_op(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_locked);
        kv->doit(LOCK_LOCK, op.table, op.key, m_tg, d);
        op.lock_nonce = kv->state_key();
    }
}

void
transaction :: release_lock(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.lock_nonce == 0)
    {
        daemon::lock_op_map_t::state_reference sr;
        kvs_lock_op* kv = d->create_lock_op(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_unlocked);
        kv->doit(LOCK_UNLOCK, op.table, op.key, m_tg, d);
        op.lock_nonce = kv->state_key();
    }
}

void
transaction :: start_read(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.read_nonce == 0)
    {
        daemon::read_map_t::state_reference sr;
        kvs_read* kv = d->create_read(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_read);
        kv->read(op.table, op.key, UINT64_MAX, d);
        op.read_nonce = kv->state_key();
    }
}

void
transaction :: start_write(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.write_nonce == 0)
    {
        daemon::write_map_t::state_reference sr;
        kvs_write* kv = d->create_write(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_write);
        kv->write(0/*XXX*/, op.table, op.key, m_timestamp, op.value, d);
        op.write_nonce = kv->state_key();
    }
}

void
transaction :: start_verify_read(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.verify_read_nonce == 0)
    {
        daemon::read_map_t::state_reference sr;
        kvs_read* kv = d->create_read(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_verify_read);
        kv->read(op.table, op.key, UINT64_MAX, d);
        op.verify_read_nonce = kv->state_key();
    }
}

void
transaction :: start_verify_write(uint64_t seqno, daemon* d)
{
    assert(seqno < m_ops.size());
    operation& op(m_ops[seqno]);

    if (op.verify_write_nonce == 0)
    {
        daemon::read_map_t::state_reference sr;
        kvs_read* kv = d->create_read(&sr);
        kv->callback_transaction(m_tg, seqno, &transaction::callback_verify_write);
        kv->read(op.table, op.key, UINT64_MAX, d);
        op.verify_write_nonce = kv->state_key();
    }
}

#if 0
void
transaction :: begin_write(uint64_t seqno, daemon* d)
{
#if 0
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].time_of_last_write_request + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].time_of_last_write_request = now;
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
#endif
    // XXX delete below

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].write_started = true;
    m_ops[seqno].time_of_last_write_request = 0;
}

void
transaction :: finish_write(uint64_t seqno, daemon* d)
{
#if 0
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].time_of_last_write_request + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].time_of_last_write_request = now;
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
#endif
    // XXX delete below

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].write_finished = true;
}

void
transaction :: cancel_write(uint64_t seqno, daemon* d)
{
#if 0
    assert(seqno < m_ops.size());
    const uint64_t now = po6::monotonic_time();

    if (m_ops[seqno].time_of_last_write_request + d->resend_interval() > now)
    {
        return;
    }

    m_ops[seqno].time_of_last_write_request = now;
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
#endif
    // XXX delete below

    if (seqno >= m_ops.size())
    {
        return;
    }

    m_ops[seqno].write_finished = true;
    work_state_machine(d);
}
#endif

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
transaction :: send_committed_response(operation* op, daemon* d)
{
    if (op->client == comm_id())
    {
        return;
    }

    send_committed_response(op->client, op->nonce, d);
    op->client = comm_id();
}

void
transaction :: send_committed_response(comm_id id, uint64_t nonce, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_COMMITTED);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << nonce << CONSUS_COMMITTED;
    d->send(id, msg);
}

void
transaction :: send_aborted_response(operation* op, daemon* d)
{
    if (op->client == comm_id())
    {
        return;
    }

    send_aborted_response(op->client, op->nonce, d);
    op->client = comm_id();
}

void
transaction :: send_aborted_response(comm_id id, uint64_t nonce, daemon* d)
{
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_ABORTED);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << nonce << CONSUS_ABORTED;
    d->send(id, msg);
}

void
transaction :: send_tx_begin(operation* op, daemon* d)
{
    std::vector<comm_id> ids(m_group.members, m_group.members + m_group.members_sz);
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(CLIENT_RESPONSE)
                    + sizeof(uint64_t)
                    + pack_size(CONSUS_SUCCESS)
                    + pack_size(m_tg)
                    + ::pack_size(ids);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << CLIENT_RESPONSE << op->nonce << CONSUS_SUCCESS << m_tg.txid << ids;
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
