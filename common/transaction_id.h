// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_transaction_id_h_
#define consus_common_transaction_id_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/buffer.h>

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class transaction_id
{
    public:
        transaction_id();
        transaction_id(paxos_group_id g, uint64_t start, uint64_t number);
        transaction_id(const transaction_id& other);
        ~transaction_id() throw ();

    public:
        size_t hash() const;
        bool preempts(const transaction_id& other) const;

    public:
        paxos_group_id group;
        uint64_t start;
        uint64_t number;
};

bool
operator == (const transaction_id& lhs, const transaction_id& rhs);
inline bool
operator != (const transaction_id& lhs, const transaction_id& rhs)
{ return !(lhs == rhs); }

std::ostream&
operator << (std::ostream& lhs, const transaction_id& rhs);

e::packer
operator << (e::packer pa, const transaction_id& rhs);
e::unpacker
operator >> (e::unpacker up, transaction_id& rhs);
size_t
pack_size(const transaction_id& txid);

END_CONSUS_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE

template <>
struct hash<consus::transaction_id>
{
    size_t operator()(const consus::transaction_id& kr) const
    {
        return kr.hash();
    }
};

END_E_COMPAT_NAMESPACE

#endif // consus_common_transaction_id_h_
