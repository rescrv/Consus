// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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
