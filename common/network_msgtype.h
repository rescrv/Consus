// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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

    UNSAFE_READ     = 7422,
    UNSAFE_WRITE    = 7421,
    UNSAFE_LOCK_OP  = 7420,

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
