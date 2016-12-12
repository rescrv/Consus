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

std::string
migrator :: debug_dump()
{
    return "XXX"; // XXX
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
