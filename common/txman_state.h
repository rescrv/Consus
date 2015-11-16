// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_txman_state_h_
#define consus_common_txman_state_h_

// po6
#include <po6/net/location.h>

// consus
#include "namespace.h"
#include "common/txman.h"

BEGIN_CONSUS_NAMESPACE

class txman_state
{
    public:
        enum state_t
        {
            REGISTERED  = 1,
            ONLINE      = 2,
            OFFLINE     = 3
        };
        static const char* to_string(state_t state);

    public:
        txman_state();
        txman_state(const txman& tx);
        txman_state(const txman_state& other);
        ~txman_state() throw ();

    public:
        comm_id id() const { return tx.id; }
        po6::net::location bind_to() const { return tx.bind_to; }

    public:
        txman_state& operator = (const txman_state& rhs);

    public:
        txman tx;
        state_t state;
        uint64_t nonce;
};

std::ostream&
operator << (std::ostream& lhs, const txman_state& rhs);
std::ostream&
operator << (std::ostream& lhs, const txman_state::state_t& rhs);

e::packer
operator << (e::packer lhs, const txman_state& rhs);
e::unpacker
operator >> (e::unpacker lhs, txman_state& rhs);
size_t
pack_size(const txman_state& p);

e::packer
operator << (e::packer lhs, const txman_state::state_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, txman_state::state_t& rhs);
size_t
pack_size(const txman_state::state_t& p);

END_CONSUS_NAMESPACE

#endif // consus_common_txman_state_h_
