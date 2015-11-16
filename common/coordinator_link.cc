// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// C
#include <stdio.h>

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/io/fd.h>

// e
#include <e/daemon.h>
#include <e/endian.h>

// consus
#include "common/coordinator_returncode.h"
#include "common/coordinator_link.h"

using consus::coordinator_link;

coordinator_link :: coordinator_link(const std::string& rendezvous,
                                     comm_id id, po6::net::location bind_to,
                                     callback* c/*ownership not transferred*/)
    : m_mtx()
    , m_repl(replicant_client_create_conn_str(rendezvous.c_str()))
    , m_id(id)
    , m_bind_to(bind_to)
    , m_cb(c)
    , m_config_id(-1)
    , m_config_status(REPLICANT_SUCCESS)
    , m_config_state(0)
    , m_config_data(NULL)
    , m_config_data_sz(0)
    , m_last_config_state(0)
    , m_last_config_valid(false)
    , m_allow_rereg(false)
    , m_error(false)
    , m_orphaned(false)
    , m_online_once(false)
{
    if (!m_repl)
    {
        throw std::bad_alloc();
    }

    po6::threads::mutex::hold hold(&m_mtx);
    invariant_check();
}

coordinator_link :: ~coordinator_link() throw ()
{
    replicant_client_destroy(m_repl);
}

bool
coordinator_link :: initial_registration()
{
    {
        po6::threads::mutex::hold hold(&m_mtx);
        invariant_check();

        if (m_error)
        {
            LOG(ERROR) << "coordinator link failed";
            return false;
        }

        if (!registration())
        {
            return false;
        }
    }

    return establish();
}

bool
coordinator_link :: establish()
{
    {
        po6::threads::mutex::hold hold(&m_mtx);
        invariant_check();

        if (m_error)
        {
            LOG(ERROR) << "coordinator link failed";
            return false;
        }
    }

    maintain_connection();
    po6::threads::mutex::hold hold(&m_mtx);
    return m_last_config_valid;
}

void
coordinator_link :: allow_reregistration()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_allow_rereg = true;
}

void
coordinator_link :: maintain_connection()
{
    po6::threads::mutex::hold hold(&m_mtx);
    invariant_check();

    if (m_error)
    {
        LOG(ERROR) << "coordinator link failed";
        return;
    }

    std::string cond = m_cb->prefix() + "conf";

    if (m_config_status != REPLICANT_SUCCESS)
    {
        replicant_client_kill(m_repl, m_config_id);
        m_config_id = -1;
    }

    int timeout = 1000;

    if (m_config_id < 0)
    {
        m_config_id = replicant_client_cond_follow(m_repl, "consus", cond.c_str(),
                                                   &m_config_status, &m_config_state,
                                                   &m_config_data, &m_config_data_sz);
        replicant_returncode rc;

        if (replicant_client_wait(m_repl, m_config_id, -1, &rc) < 0 ||
            m_config_status != REPLICANT_SUCCESS)
        {
            LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
            return;
        }

        timeout = 0;
    }

    if (!m_online_once)
    {
        online();
        m_online_once = true;
    }

    replicant_returncode rc;
    m_mtx.unlock();
    replicant_client_block(m_repl, timeout);
    m_mtx.lock();

    if (replicant_client_loop(m_repl, 0, &rc) < 0)
    {
        if (rc == REPLICANT_TIMEOUT ||
            rc == REPLICANT_INTERRUPTED ||
            rc == REPLICANT_NONE_PENDING)
        {
        }
        else
        {
            LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
            return;
        }
    }

    if (m_last_config_state < m_config_state)
    {
        m_last_config_valid = m_cb->new_config(m_config_data, m_config_data_sz);
        m_last_config_state = m_config_state;

        if (!m_cb->has_id(m_id) && m_allow_rereg)
        {
            registration();
        }
        else if (!m_cb->has_id(m_id))
        {
            m_orphaned = true;
        }
    }

    if (m_cb->has_id(m_id) &&
        (!m_cb->is_steady_state(m_id) || m_cb->address(m_id) != m_bind_to))
    {
        online();
    }
}

bool
coordinator_link :: error()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_error;
}

bool
coordinator_link :: orphaned()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_orphaned;
}

bool
coordinator_link :: call(const char* func,
                         const char* input, size_t input_sz,
                         coordinator_returncode* rc)
{
    po6::threads::mutex::hold hold(&m_mtx);
    return call_no_lock(func, input, input_sz, rc);
}

void
coordinator_link :: invariant_check()
{
    if (!m_repl)
    {
        m_error = true;
    }
    else if (!m_cb)
    {
        m_error = true;
    }
    else if (m_id == comm_id())
    {
        m_error = true;
    }
}

bool
coordinator_link :: call_no_lock(const char* func,
                                 const char* input, size_t input_sz,
                                 coordinator_returncode* rc)
{
    replicant_returncode status;
    char* output = NULL;
    size_t output_sz = 0;
    int64_t req = replicant_client_call(m_repl, "consus", func, input, input_sz,
                                        REPLICANT_CALL_ROBUST,
                                        &status, &output, &output_sz);

    if (req < 0)
    {
        LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
        return false;
    }

    int64_t loop = replicant_client_wait(m_repl, req, 10000, &status);

    if (loop < 0)
    {
        LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
        return false;
    }

    assert(loop == req);

    if (status == REPLICANT_TIMEOUT || status == REPLICANT_INTERRUPTED)
    {
        return false;
    }
    else if (status != REPLICANT_SUCCESS)
    {
        LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
        return false;
    }

    if (output_sz != 2 || !output)
    {
        LOG(ERROR) << "coordinator failure: bad response to " << func << " request";
        return false;
    }

    uint16_t x;
    e::unpack16be(output, &x);
    *rc = static_cast<coordinator_returncode>(x);
    free(output);
    return true;
}

bool
coordinator_link :: registration()
{
    std::string func = m_cb->prefix() + "_register";
    std::string input;
    e::packer(&input) << m_id << m_bind_to;
    coordinator_returncode rc;

    if (!call_no_lock(func.c_str(), input.data(), input.size(), &rc))
    {
        return false;
    }

    switch (rc)
    {
        case COORD_SUCCESS:
            return true;
        case COORD_DUPLICATE:
            LOG(ERROR) << "cannot register: another server already registered this identity";
            return false;
        case COORD_MALFORMED:
        case COORD_NOT_FOUND:
        case COORD_UNINITIALIZED:
        case COORD_NO_CAN_DO:
        default:
            LOG(ERROR) << "coordinator failure: bad response to registration request";
            return false;
    }
}

bool
coordinator_link :: online()
{
    uint64_t x;

    if (!e::generate_token(&x))
    {
        return false;
    }

    std::string func1 = m_cb->prefix() + "_online";
    std::string func2 = m_cb->prefix() + "_offline";
    std::string input;
    e::packer(&input) << m_id << m_bind_to << x;
    replicant_returncode status = REPLICANT_GARBAGE;
    replicant_returncode lstatus = REPLICANT_GARBAGE;
    int64_t req = replicant_client_defended_call(m_repl,
            "consus", func1.c_str(), input.data(), input.size(),
            func2.c_str(), input.data(), input.size(), &status);

    if (req < 0 ||
        replicant_client_wait(m_repl, req, 10000, &lstatus) != req ||
        status != REPLICANT_SUCCESS)
    {
        LOG(ERROR) << "coordinator failure: " << replicant_client_error_message(m_repl);
        return false;
    }

    return true;
}

coordinator_link :: callback :: callback()
{
}

coordinator_link :: callback :: ~callback() throw ()
{
}
