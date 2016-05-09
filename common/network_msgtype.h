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

    TXMAN_BEGIN     = 7424,
    TXMAN_READ      = 7425,
    TXMAN_WRITE     = 7426,
    TXMAN_COMMIT    = 7427,
    TXMAN_ABORT     = 7428,

    TXMAN_PAXOS_2A  = 7439,
    TXMAN_PAXOS_2B  = 7433,

    LV_VOTE_1A      = 7900,
    LV_VOTE_1B      = 7901,
    LV_VOTE_2A      = 7902,
    LV_VOTE_2B      = 7903,
    LV_VOTE_LEARN   = 7904,

    COMMIT_RECORD   = 7905,

    GV_PROPOSE      = 7906,
    GV_VOTE_1A      = 7907,
    GV_VOTE_1B      = 7908,
    GV_VOTE_2A      = 7909,
    GV_VOTE_2B      = 7910,

    KVS_RD_LOCK     = 7800,
    KVS_RD_LOCKED   = 7802,
    KVS_RD_UNLOCK   = 7803,
    KVS_RD_UNLOCKED = 7804,

    KVS_WR_BEGIN    = 7805,
    KVS_WR_BEGUN    = 7807,
    KVS_WR_FINISH   = 7808,
    KVS_WR_CANCEL   = 7809,
    KVS_WR_FINISHED = 7810,

    CONSUS_NOP      = 7935
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
