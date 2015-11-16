// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_transaction_group_h_
#define consus_common_transaction_group_h_

// consus
#include "namespace.h"
#include "common/transaction_id.h"

BEGIN_CONSUS_NAMESPACE

class transaction_group
{
    public:
        static uint64_t hash(const transaction_group& tg) { return tg.hash(); }

    public:
        transaction_group();
        explicit transaction_group(transaction_id t);
        transaction_group(paxos_group_id g, transaction_id t);
        transaction_group(const transaction_group& other);
        ~transaction_group() throw ();

    public:
        size_t hash() const;

    public:
        paxos_group_id group;
        transaction_id txid;
};

bool
operator == (const transaction_group& lhs, const transaction_group& rhs);
inline bool
operator != (const transaction_group& lhs, const transaction_group& rhs)
{ return !(lhs == rhs); }

std::ostream&
operator << (std::ostream& lhs, const transaction_group& rhs);

e::packer
operator << (e::packer pa, const transaction_group& rhs);
e::unpacker
operator >> (e::unpacker up, transaction_group& rhs);
size_t
pack_size(const transaction_group& tg);

END_CONSUS_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE

template <>
struct hash<consus::transaction_group>
{
    size_t operator()(const consus::transaction_group& kr) const
    {
        return kr.hash();
    }
};

END_E_COMPAT_NAMESPACE

#endif // consus_common_transaction_group_h_
