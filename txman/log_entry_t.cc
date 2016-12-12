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
