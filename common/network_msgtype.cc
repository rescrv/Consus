// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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
        STRINGIFY(UNSAFE_READ);
        STRINGIFY(UNSAFE_WRITE);
        STRINGIFY(UNSAFE_LOCK_OP);
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
