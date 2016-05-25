// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// BusyBee
#include <busybee_constants.h>

// Google Log
#include <glog/logging.h>

// consus
#include "common/network_msgtype.h"
#include "kvs/daemon.h"
#include "kvs/migrator.h"

using consus::migrator;

extern bool s_debug_mode;

migrator :: migrator(partition_id key)
    : m_state_key(key)
    , m_mtx()
    , m_version()
    , m_state(UNINITIALIZED)
    , m_last_handshake(0)
    , m_last_coord_call(0)
{
}

migrator :: ~migrator() throw ()
{
}

consus::partition_id
migrator :: state_key()
{
    return m_state_key;
}

bool
migrator :: finished()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_state == UNINITIALIZED ||
           m_state == TERMINATED;
}

void
migrator :: ack(version_id version, daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    ensure_initialized(d);

    if (m_state == CHECK_CONFIG && m_version <= version)
    {
        LOG_IF(INFO, s_debug_mode) << "received migration ACK for " << m_state_key << "/" << m_version;
        m_state = TRANSFER_DATA;
        work_state_machine(d);
    }
}

void
migrator :: externally_work_state_machine(daemon* d)
{
    po6::threads::mutex::hold hold(&m_mtx);
    ensure_initialized(d);
    work_state_machine(d);
}

void
migrator :: terminate()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_state = TERMINATED;
}

void
migrator :: ensure_initialized(daemon* d)
{
    if (m_state == UNINITIALIZED)
    {
        m_version = d->get_config()->version();
        m_state = CHECK_CONFIG;
    }
}

void
migrator :: work_state_machine(daemon* d)
{
    switch (m_state)
    {
        case CHECK_CONFIG:
            return work_state_machine_check_config(d);
        case TRANSFER_DATA:
            return work_state_machine_transfer_data(d);
        case TERMINATED:
            return;
        case UNINITIALIZED:
        default:
            abort();
    }
}

void
migrator :: work_state_machine_check_config(daemon* d)
{
    configuration* c = d->get_config();
    const uint64_t now = po6::monotonic_time();

    if (m_last_handshake + d->resend_interval() < now)
    {
        const size_t sz = BUSYBEE_HEADER_SIZE
                        + pack_size(KVS_MIGRATE_SYN)
                        + pack_size(m_state_key)
                        + sizeof(uint64_t);
        std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
        msg->pack_at(BUSYBEE_HEADER_SIZE)
            << KVS_MIGRATE_SYN << m_state_key << m_version;
        d->send(c->owner_from_next_id(m_state_key), msg);
        m_last_handshake = now;
        LOG_IF(INFO, s_debug_mode) << "sending migration SYN for " << m_state_key << "/" << m_version;
    }
}

void
migrator :: work_state_machine_transfer_data(daemon* d)
{
    bool done = true;
    // XXX

    const uint64_t now = po6::monotonic_time();

    if (done &&
        m_last_coord_call + d->resend_interval() < now)
    {
        std::string msg;
        e::packer(&msg) << m_state_key;
        d->m_coord->fire_and_forget("kvs_migrated", msg.data(), msg.size());
        m_last_coord_call = now;
    }
}
