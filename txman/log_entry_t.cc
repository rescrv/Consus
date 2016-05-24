// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/macros.h"
#include "txman/log_entry_t.h"

using consus::log_entry_t;

bool
consus :: is_paxos_2a_log_entry(log_entry_t t)
{
    switch (t)
    {
        case LOG_ENTRY_TX_BEGIN:
        case LOG_ENTRY_TX_READ:
        case LOG_ENTRY_TX_WRITE:
        case LOG_ENTRY_TX_PREPARE:
        case LOG_ENTRY_TX_ABORT:
            return true;
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
            return false;
    }
}

std::ostream&
consus :: operator << (std::ostream& lhs, const log_entry_t& rhs)
{
    switch (rhs)
    {
        STRINGIFY(LOG_ENTRY_CONFIG);
        STRINGIFY(LOG_ENTRY_TX_BEGIN);
        STRINGIFY(LOG_ENTRY_TX_READ);
        STRINGIFY(LOG_ENTRY_TX_WRITE);
        STRINGIFY(LOG_ENTRY_TX_PREPARE);
        STRINGIFY(LOG_ENTRY_TX_ABORT);
        STRINGIFY(LOG_ENTRY_LOCAL_VOTE_1A);
        STRINGIFY(LOG_ENTRY_LOCAL_VOTE_2A);
        STRINGIFY(LOG_ENTRY_LOCAL_LEARN);
        STRINGIFY(LOG_ENTRY_GLOBAL_PROPOSE);
        STRINGIFY(LOG_ENTRY_GLOBAL_VOTE_1A);
        STRINGIFY(LOG_ENTRY_GLOBAL_VOTE_2A);
        STRINGIFY(LOG_ENTRY_GLOBAL_VOTE_2B);
        STRINGIFY(LOG_ENTRY_NOP);
        default:
            lhs << "unknown LOG_ENTRY";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const log_entry_t& rhs)
{
    uint16_t mt = static_cast<uint16_t>(rhs);
    return lhs << mt;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, log_entry_t& rhs)
{
    uint16_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<log_entry_t>(mt);
    return lhs;
}

size_t
consus :: pack_size(const log_entry_t&)
{
    return sizeof(uint16_t);
}
