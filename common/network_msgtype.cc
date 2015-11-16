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
        STRINGIFY(TXMAN_BEGIN);
        STRINGIFY(TXMAN_READ);
        STRINGIFY(TXMAN_WRITE);
        STRINGIFY(TXMAN_COMMIT);
        STRINGIFY(TXMAN_ABORT);
        STRINGIFY(TXMAN_PAXOS_2A);
        STRINGIFY(TXMAN_PAXOS_2B);
        STRINGIFY(LV_VOTE_1A);
        STRINGIFY(LV_VOTE_1B);
        STRINGIFY(LV_VOTE_2A);
        STRINGIFY(LV_VOTE_2B);
        STRINGIFY(LV_VOTE_LEARN);
        STRINGIFY(COMMIT_RECORD);
        STRINGIFY(KVS_RD_LOCK);
        STRINGIFY(KVS_RD_LOCKED);
        STRINGIFY(KVS_RD_UNLOCK);
        STRINGIFY(KVS_RD_UNLOCKED);
        STRINGIFY(KVS_WR_BEGIN);
        STRINGIFY(KVS_WR_BEGUN);
        STRINGIFY(KVS_WR_FINISH);
        STRINGIFY(KVS_WR_CANCEL);
        STRINGIFY(KVS_WR_FINISHED);
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
