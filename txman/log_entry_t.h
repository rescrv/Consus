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

#ifndef consus_txman_log_entry_t_h_
#define consus_txman_log_entry_t_h_

// C++
#include <iostream>

// e
#include <e/buffer.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

/* log_entry_t occupies [7936, 8192) */
enum log_entry_t
{
    LOG_ENTRY_CONFIG        = 7936,
    LOG_ENTRY_TX_BEGIN      = 7937,
    LOG_ENTRY_TX_READ       = 7938,
    LOG_ENTRY_TX_WRITE      = 7939,
    LOG_ENTRY_TX_PREPARE    = 7940,
    LOG_ENTRY_TX_ABORT      = 7943,
    LOG_ENTRY_LOCAL_VOTE_1A = 7944,
    LOG_ENTRY_LOCAL_VOTE_2A = 7946,
    LOG_ENTRY_LOCAL_LEARN   = 7947,
    LOG_ENTRY_GLOBAL_PROPOSE = 8000,
    LOG_ENTRY_GLOBAL_VOTE_1A = 8001,
    LOG_ENTRY_GLOBAL_VOTE_2A = 8002,
    LOG_ENTRY_GLOBAL_VOTE_2B = 8003,
    LOG_ENTRY_NOP           = 8191
};

bool
is_paxos_2a_log_entry(log_entry_t t);

std::ostream&
operator << (std::ostream& lhs, const log_entry_t& rhs);

e::packer
operator << (e::packer pa, const log_entry_t& rhs);
e::unpacker
operator >> (e::unpacker up, log_entry_t& rhs);
size_t
pack_size(const log_entry_t& tid);

END_CONSUS_NAMESPACE

#endif // consus_txman_log_entry_t_h_
