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

#ifndef consus_common_network_msgtype_h_
#define consus_common_network_msgtype_h_

// C++
#include <iostream>

// e
#include <e/serialization.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

/* network_returncode occupies [7168, 7936) */
enum network_msgtype
{
    CLIENT_RESPONSE = 7423,

    TXMAN_BEGIN     = 7424,
    TXMAN_READ      = 7425,
    TXMAN_WRITE     = 7426,
    TXMAN_COMMIT    = 7427,
    TXMAN_ABORT     = 7428,
    TXMAN_WOUND     = 7429,

    TXMAN_PAXOS_2A  = 7439,
    TXMAN_PAXOS_2B  = 7433,

    LV_VOTE_1A      = 7500,
    LV_VOTE_1B      = 7501,
    LV_VOTE_2A      = 7502,
    LV_VOTE_2B      = 7503,
    LV_VOTE_LEARN   = 7504,

    COMMIT_RECORD   = 7505,

    GV_OUTCOME      = 7611,
    GV_PROPOSE      = 7606,
    GV_VOTE_1A      = 7607,
    GV_VOTE_1B      = 7608,
    GV_VOTE_2A      = 7609,
    GV_VOTE_2B      = 7610,

    KVS_REP_RD      = 7740,
    KVS_REP_RD_RESP = 7741,
    KVS_REP_WR      = 7742,
    KVS_REP_WR_RESP = 7743,

    KVS_RAW_RD      = 7750,
    KVS_RAW_RD_RESP = 7751,
    KVS_RAW_WR      = 7752,
    KVS_RAW_WR_RESP = 7753,

    KVS_LOCK_OP      = 7754,
    KVS_LOCK_OP_RESP = 7755,

    KVS_RAW_LK      = 7756,
    KVS_RAW_LK_RESP = 7757,

    KVS_WOUND_XACT  = 7758,

    KVS_MIGRATE_SYN = 7800,
    KVS_MIGRATE_ACK = 7801,

    CONSUS_NOP      = 7835
};

std::ostream&
operator << (std::ostream& lhs, network_msgtype rhs);

e::packer
operator << (e::packer lhs, const network_msgtype& rhs);
e::unpacker
operator >> (e::unpacker lhs, network_msgtype& rhs);
size_t
pack_size(const network_msgtype& rhs);

END_CONSUS_NAMESPACE

#endif // consus_common_network_msgtype_h_
