// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "client/pending.h"

using consus::pending;

pending :: pending(int64_t client_id, consus_returncode* status)
    : m_ref(0)
    , m_error()
    , m_client_id(client_id)
    , m_status(status)
{
}

pending :: ~pending() throw ()
{
}

void
pending :: returning()
{
}

void
pending :: handle_server_failure(client*, comm_id)
{
}

void
pending :: handle_server_disruption(client*, comm_id)
{
}

void
pending :: handle_busybee_op(client*,
                             uint64_t,
                             std::auto_ptr<e::buffer>,
                             e::unpacker)
{
}

std::ostream&
pending :: error(const char* file, size_t line)
{
    m_error.set_loc(file, line);
    return m_error.set_msg();
}

void
pending :: set_error(const e::error& err)
{
    m_error = err;
}

void
pending :: success()
{
    set_status(CONSUS_SUCCESS);
    m_error = e::error();
}
