// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/constants.h"
#include "common/consus.h"
#include "common/network_msgtype.h"
#include "kvs/daemon.h"
#include "kvs/read_replicator.h"

using consus::read_replicator;

extern bool s_debug_mode;

struct read_replicator :: read_stub
{
    read_stub(comm_id t);
    ~read_stub() throw () {}

    comm_id target;
    replica_set rs;
    uint64_t last_request_time;
};

read_replicator :: read_stub :: read_stub(comm_id t)
    : target(t)
    , rs()
    , last_request_time(0)
{
}

read_replicator :: read_replicator(uint64_t key)
    : m_state_key(key)
    , m_mtx()
    , m_init(false)
    , m_finished(false)
    , m_id()
    , m_nonce()
    , m_table()
    , m_key()
    , m_kbacking()
    , m_status(CONSUS_NOT_FOUND)
    , m_value()
    , m_vbacking()
    , m_timestamp(0)
    , m_requests()
{
}

read_replicator :: ~read_replicator() throw ()
{
}

uint64_t
read_replicator :: state_key()
{
    return m_state_key;
}

bool
read_replicator :: finished()
{
    return !m_init || m_finished;
}

void
read_replicator :: init(comm_id id, uint64_t nonce,
                        const e::slice& table, const e::slice& key,
                        std::auto_ptr<e::buffer> backing)
{
    po6::threads::mutex::hold hold(&m_mtx);
    assert(!m_init);
    m_id = id;
    m_nonce = nonce;
    m_table = table;
    m_key = key;
    m_kbacking = backing;
    m_init = true;
}

void
read_replicator :: response(comm_id id, consus_returncode rc,
                            uint64_t timestamp, const e::slice& value,
                            const replica_set& rs,
                            std::auto_ptr<e::buffer> backing, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    read_stub* stub = get_stub(id);

    if (!stub)
    {
        return;
    }

    if (returncode_is_final(rc))
    {
        stub->rs = rs;

        if (m_timestamp == 0 || timestamp > m_timestamp)
        {
            m_status = rc;
            m_value = value;
            m_vbacking = backing;
            m_timestamp = timestamp;
        }
        else if (timestamp == m_timestamp && s_debug_mode && value != m_value)
        {
            LOG(WARNING) << "two different values with the same timestamp";
        }
    }

    work_state_machine(d);
}

void
read_replicator :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

read_replicator::read_stub*
read_replicator :: get_stub(comm_id id)
{
    for (size_t j = 0; j < m_requests.size(); ++j)
    {
        if (m_requests[j].target == id)
        {
            return &m_requests[j];
        }
    }

    return NULL;
}

void
read_replicator :: work_state_machine(daemon* d)
{
    configuration* c = d->get_config();
    replica_set rs;

    if (!c->hash(d->m_us.dc, m_table, m_key, &rs))
    {
        // XXX
    }

    const uint64_t now = po6::monotonic_time();
    unsigned complete = 0;

    for (unsigned i = 0; i < rs.num_replicas; ++i)
    {
        read_stub* stub = get_stub(rs.replicas[i]);

        if (!stub)
        {
            m_requests.push_back(read_stub(rs.replicas[i]));
            stub = &m_requests.back();
        }

        if (replica_sets_agree(rs.replicas[i], rs, stub->rs))
        {
            ++complete;
        }
        else if (stub->last_request_time + d->resend_interval() < now)
        {
            send_read_request(stub, now, d);
        }
    }

    if (rs.desired_replication > rs.num_replicas)
    {
        LOG_EVERY_N(WARNING, 1000) << "too few kvs daemons to achieve desired replication factor: "
                                   << rs.desired_replication - rs.num_replicas
                                   << " more daemons needed";
        rs.desired_replication = rs.num_replicas;
    }

    const unsigned quorum = rs.desired_replication / 2 + 1;

    if (complete >= quorum)
    {
        m_finished = true;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(KVS_REP_RD_RESP)
                        + sizeof(uint64_t)
                        + pack_size(m_status)
                        + sizeof(uint64_t)
                        + pack_size(m_value);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << KVS_REP_RD_RESP << m_nonce << m_status << m_timestamp << m_value;
        d->send(m_id, msg);
        LOG_IF(INFO, s_debug_mode) << "sending read response " << m_status
                                   << " nonce=" << m_nonce << " to " << m_id;
    }
}

// It's tempting to dedupe this with {write,lock}-replicator.  Reads and writes
// may have different sets of "terminal" returncodes in the future that
// represent non-transient errors; keeping them as different functions reminds
// us to make this decision in the future.
bool
read_replicator :: returncode_is_final(consus_returncode rc)
{
    switch (rc)
    {
        case CONSUS_SUCCESS:
        case CONSUS_NOT_FOUND:
        case CONSUS_UNKNOWN_TABLE:
            return true;
        case CONSUS_LESS_DURABLE:
        case CONSUS_ABORTED:
        case CONSUS_NONE_PENDING:
        case CONSUS_INVALID:
        case CONSUS_TIMEOUT:
        case CONSUS_INTERRUPTED:
        case CONSUS_SEE_ERRNO:
        case CONSUS_COORD_FAIL:
        case CONSUS_UNAVAILABLE:
        case CONSUS_SERVER_ERROR:
        case CONSUS_INTERNAL:
        case CONSUS_GARBAGE:
        default:
            return false;
    }
}

void
read_replicator :: send_read_request(read_stub* stub, uint64_t now, daemon* d)
{
    if (s_debug_mode)
    {
        LOG(INFO) << "sending raw read(\"" << e::strescape(m_table.str()) << "\", \""
                  << e::strescape(m_key.str()) << "\"@" << m_timestamp
                  << "\") nonce=" << m_nonce << " to "
                  << stub->target;
    }

    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_RD)
                    + sizeof(uint64_t)
                    + sizeof(uint8_t)
                    + pack_size(m_table)
                    + pack_size(m_key)
                    + sizeof(uint64_t)
                    + pack_size(m_value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RAW_RD << m_state_key << m_table << m_key << uint64_t(UINT64_MAX);
    d->send(stub->target, msg);
    stub->last_request_time = now;
}
