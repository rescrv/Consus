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

// C++
#include <iostream>

// e
#include <e/serialization.h>

// consus
#include "common/macros.h"
#include "common/network_msgtype.h"

std::ostream&
consus :: operator << (std::ostream& lhs, network_msgtype rhs)
{
    switch (rhs)
    {
        STRINGIFY(CLIENT_RESPONSE);
        STRINGIFY(TXMAN_BEGIN);
        STRINGIFY(TXMAN_READ);
        STRINGIFY(TXMAN_WRITE);
        STRINGIFY(TXMAN_COMMIT);
        STRINGIFY(TXMAN_ABORT);
        STRINGIFY(TXMAN_WOUND);
        STRINGIFY(TXMAN_PAXOS_2A);
        STRINGIFY(TXMAN_PAXOS_2B);
        STRINGIFY(LV_VOTE_1A);
        STRINGIFY(LV_VOTE_1B);
        STRINGIFY(LV_VOTE_2A);
        STRINGIFY(LV_VOTE_2B);
        STRINGIFY(LV_VOTE_LEARN);
        STRINGIFY(COMMIT_RECORD);
        STRINGIFY(GV_PROPOSE);
        STRINGIFY(GV_VOTE_1A);
        STRINGIFY(GV_VOTE_1B);
        STRINGIFY(GV_VOTE_2A);
        STRINGIFY(GV_VOTE_2B);
        STRINGIFY(KVS_REP_RD);
        STRINGIFY(KVS_REP_RD_RESP);
        STRINGIFY(KVS_REP_WR);
        STRINGIFY(KVS_REP_WR_RESP);
        STRINGIFY(KVS_RAW_RD);
        STRINGIFY(KVS_RAW_RD_RESP);
        STRINGIFY(KVS_RAW_WR);
        STRINGIFY(KVS_RAW_WR_RESP);
        STRINGIFY(KVS_LOCK_OP);
        STRINGIFY(KVS_LOCK_OP_RESP);
        STRINGIFY(KVS_RAW_LK);
        STRINGIFY(KVS_RAW_LK_RESP);
        STRINGIFY(KVS_WOUND_XACT);
        STRINGIFY(KVS_MIGRATE_SYN);
        STRINGIFY(KVS_MIGRATE_ACK);
        STRINGIFY(CONSUS_NOP);
        default:
            lhs << "unknown msgtype";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const network_msgtype& rhs)
{
    return lhs << e::pack_uint16<network_msgtype>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, network_msgtype& rhs)
{
    return lhs >> e::unpack_uint16<network_msgtype>(rhs);
}

size_t
consus :: pack_size(const network_msgtype&)
{
    return sizeof(uint16_t);
}
