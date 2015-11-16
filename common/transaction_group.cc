// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/transaction_group.h"

using consus::transaction_group;

transaction_group :: transaction_group()
    : group()
    , txid()
{
}

transaction_group :: transaction_group(transaction_id t)
    : group(t.group)
    , txid(t)
{
}

transaction_group :: transaction_group(paxos_group_id g, transaction_id t)
    : group(g)
    , txid(t)
{
}

transaction_group :: transaction_group(const transaction_group& other)
    : group(other.group)
    , txid(other.txid)
{
}

transaction_group :: ~transaction_group() throw ()
{
}

size_t
transaction_group :: hash() const
{
    e::compat::hash<uint64_t> h;
    return h(group.get()) ^ txid.hash();
}

bool
consus :: operator == (const transaction_group& lhs, const transaction_group& rhs)
{
    return lhs.group == rhs.group && lhs.txid == rhs.txid;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const transaction_group& rhs)
{
    return lhs << "transaction_group(executing="
               << rhs.group << ", originating="
               << rhs.txid.group << ", number="
               << rhs.txid.number << ")";
}

e::packer
consus :: operator << (e::packer pa, const transaction_group& rhs)
{
    return pa << rhs.group << rhs.txid;
}

e::unpacker
consus :: operator >> (e::unpacker up, transaction_group& rhs)
{
    return up >> rhs.group >> rhs.txid;
}

size_t
consus :: pack_size(const transaction_group& tg)
{
    return pack_size(tg.group) + pack_size(tg.txid);
}
