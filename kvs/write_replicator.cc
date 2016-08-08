// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// e
#include <e/strescape.h>

// BusyBee
#include <busybee_constants.h>

// consus
#include "common/consus.h"
#include "common/network_msgtype.h"
#include "kvs/daemon.h"
#include "kvs/write_replicator.h"

using consus::write_replicator;

extern bool s_debug_mode;

struct write_replicator :: write_stub
{
    write_stub(comm_id t);
    ~write_stub() throw () {}

    comm_id target;
    uint64_t last_request_time;
    consus_returncode status;
    replica_set rs;
};

write_replicator :: write_stub :: write_stub(comm_id t)
    : target(t)
    , last_request_time(0)
    , status(CONSUS_GARBAGE)
    , rs()
{
}

write_replicator :: write_replicator(uint64_t key)
    : m_state_key(key)
    , m_mtx()
    , m_init(false)
    , m_finished(false)
    , m_id()
    , m_nonce()
    , m_flags()
    , m_table()
    , m_key()
    , m_timestamp()
    , m_value()
    , m_backing()
    , m_requests()
{
}

write_replicator :: ~write_replicator() throw ()
{
}

uint64_t
write_replicator :: state_key()
{
    return m_state_key;
}

bool
write_replicator :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return !m_init || m_finished;
}

void
write_replicator :: init(comm_id id, uint64_t nonce, unsigned flags,
                         const e::slice& table, const e::slice& key,
                         uint64_t timestamp, const e::slice& value,
                         std::auto_ptr<e::buffer> msg)
{
    po6::threads::mutex::hold hold(&m_mtx);
    assert(!m_init);
    m_id = id;
    m_nonce = nonce;
    m_flags = flags;
    m_table = table;
    m_key = key;
    m_timestamp = timestamp;
    m_value = value;
    m_backing = msg;
    m_init = true;
}

void
write_replicator :: response(comm_id id,
                             consus_returncode rc,
                             const replica_set& rs,
                             daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    write_stub* stub = get_stub(id);

    if (!stub)
    {
        return;
    }

    if (stub->status == CONSUS_GARBAGE)
    {
        stub->status = rc;
        stub->rs = rs;
    }

    work_state_machine(d);
}

void
write_replicator :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    work_state_machine(d);
}

write_replicator::write_stub*
write_replicator :: get_stub(comm_id id)
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

write_replicator::write_stub*
write_replicator :: get_or_create_stub(comm_id id)
{
    write_stub* ws = get_stub(id);

    if (!ws && id != comm_id())
    {
        m_requests.push_back(write_stub(id));
        ws = &m_requests.back();
    }

    return ws;
}

void
write_replicator :: work_state_machine(daemon* d)
{
    configuration* c = d->get_config();
    replica_set rs;

    if (!c->hash(d->m_us.dc, m_table, m_key, &rs))
    {
        // XXX
    }

    const uint64_t now = po6::monotonic_time();
    unsigned complete_success = 0;
    unsigned complete_unknown = 0;
    unsigned complete_invalid = 0;

    for (unsigned i = 0; i < rs.num_replicas; ++i)
    {
        write_stub* owner1 = get_or_create_stub(rs.replicas[i]);
        write_stub* owner2 = get_or_create_stub(rs.transitioning[i]);
        // need to do it again in case anything was created
        owner1 = get_stub(rs.replicas[i]);
        owner2 = get_stub(rs.transitioning[i]);
        assert(owner1);

        consus_returncode rc = owner1->status;

        if (owner2)
        {
            if (owner2->status == CONSUS_GARBAGE)
            {
                rc = CONSUS_GARBAGE;
            }
            else if (owner1->status != owner2->status ||
                     !replica_sets_agree(rs.replicas[i], owner1->rs, owner2->rs))
            {
                rc = owner1->status = owner2->status = CONSUS_GARBAGE;
            }
            else
            {
                assert(owner1->status != CONSUS_GARBAGE);
                assert(owner1->status == owner2->status);
                assert(replica_sets_agree(rs.replicas[i], owner1->rs, owner2->rs));
            }
        }

        if (rc == CONSUS_SUCCESS)
        {
            ++complete_success;
        }
        else if (rc == CONSUS_UNKNOWN_TABLE)
        {
            ++complete_unknown;
        }
        else if (rc == CONSUS_INVALID)
        {
            ++complete_invalid;
        }
        else if (owner1->last_request_time + d->resend_interval() < now)
        {
            if (owner1 && !returncode_is_final(owner1->status))
            {
                send_write_request(owner1, now, d);
            }

            if (owner2 && !returncode_is_final(owner2->status))
            {
                send_write_request(owner2, now, d);
            }
        }
    }

    bool short_write = false;

    if (rs.desired_replication > rs.num_replicas)
    {
        LOG_EVERY_N(WARNING, 1000) << "too few kvs daemons to achieve desired replication factor: "
                                   << rs.desired_replication - rs.num_replicas
                                   << " more daemons needed";
        rs.desired_replication = rs.num_replicas;
        short_write = true;
    }

    consus_returncode status = CONSUS_GARBAGE;
    const unsigned quorum = rs.desired_replication / 2 + 1;
    const unsigned sum = complete_success + complete_unknown + complete_invalid;
    assert(quorum > 0);

    // we're very draconian here and require complete agreement among the live
    // quroum
    // if this proves problematic, we should revisit
    //
    // also, this only writes a quorum, and {c,sh}ould be modified to write the
    // remaining nodes after returning to the client.
    if (sum > 0 && sum == complete_success && complete_success >= quorum)
    {
        status = !short_write ? CONSUS_SUCCESS : CONSUS_LESS_DURABLE;
    }
    else if (sum > 0 && sum == complete_unknown && complete_unknown >= quorum)
    {
        status = CONSUS_UNKNOWN_TABLE;
    }
    else if (sum > 0 && sum == complete_invalid && complete_invalid >= quorum)
    {
        status = CONSUS_INVALID;
    }
    else if (sum > 0 &&
             sum != complete_success &&
             sum != complete_unknown &&
             sum != complete_invalid)
    {
        // We have mixed responses; try again
        m_requests.clear();
        work_state_machine(d);
    }

    if (status != CONSUS_GARBAGE)
    {
        m_finished = true;
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(KVS_REP_WR_RESP)
                        + sizeof(uint64_t)
                        + pack_size(status);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE) << KVS_REP_WR_RESP << m_nonce << status;
        d->send(m_id, msg);
        LOG_IF(INFO, s_debug_mode) << "sending write response " << status
                                   << " nonce=" << m_nonce << " to " << m_id;
    }
}

bool
write_replicator :: returncode_is_final(consus_returncode rc)
{
    switch (rc)
    {
        case CONSUS_SUCCESS:
        case CONSUS_UNKNOWN_TABLE:
        case CONSUS_INVALID:
            return true;
        case CONSUS_LESS_DURABLE:
        case CONSUS_NOT_FOUND:
        case CONSUS_ABORTED:
        case CONSUS_NONE_PENDING:
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
write_replicator :: send_write_request(write_stub* stub, uint64_t now, daemon* d)
{
    if (s_debug_mode)
    {
        std::string tmp;
        const char* v = NULL;

        if ((CONSUS_WRITE_TOMBSTONE & m_flags))
        {
            v = "TOMBSTONE";
        }
        else
        {
            tmp = e::strescape(m_value.str());
            v = tmp.c_str();
        }

        LOG(INFO) << "sending raw write(\"" << e::strescape(m_table.str()) << "\", \""
                  << e::strescape(m_key.str()) << "\"@" << m_timestamp
                  << ", \"" << v << "\") nonce=" << m_nonce << " to "
                  << stub->target;
    }

    assert(!returncode_is_final(stub->status));
    const size_t sz = BUSYBEE_HEADER_SIZE
                    + pack_size(KVS_RAW_WR)
                    + sizeof(uint64_t)
                    + sizeof(uint8_t)
                    + pack_size(m_table)
                    + pack_size(m_key)
                    + sizeof(uint64_t)
                    + pack_size(m_value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << KVS_RAW_WR << m_state_key << uint8_t(m_flags) << m_table << m_key << m_timestamp << m_value;
    d->send(stub->target, msg);
    stub->last_request_time = now;
}
