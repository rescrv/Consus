// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/transaction_id.h"

using consus::transaction_id;

transaction_id :: transaction_id()
    : group()
    , start(0)
    , number(0)
{
}

transaction_id :: transaction_id(paxos_group_id g, uint64_t s, uint64_t n)
    : group(g)
    , start(s)
    , number(n)
{
}

transaction_id :: transaction_id(const transaction_id& other)
    : group(other.group)
    , start(other.start)
    , number(other.number)
{
}

transaction_id :: ~transaction_id() throw ()
{
}

size_t
transaction_id :: hash() const
{
    e::compat::hash<uint64_t> h;
    return h(start) ^ h(number) ^ h(group.get());
}

bool
transaction_id :: preempts(const transaction_id& other) const
{
    return start < other.start ||
           (start == other.start && number < other.number);
}

bool
consus :: operator == (const transaction_id& lhs, const transaction_id& rhs)
{
    return lhs.group == rhs.group &&
           lhs.start == rhs.start &&
           lhs.number == rhs.number;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const transaction_id& rhs)
{
    return lhs << "transaction_id(group="
               << rhs.group.get() << ", start="
               << rhs.start << ", number="
               << rhs.number << ")";
}

e::packer
consus :: operator << (e::packer pa, const transaction_id& rhs)
{
    return pa << rhs.group << rhs.start << rhs.number;
}

e::unpacker
consus :: operator >> (e::unpacker up, transaction_id& rhs)
{
    return up >> rhs.group >> rhs.start >> rhs.number;
}

size_t
consus :: pack_size(const transaction_id& x)
{
    return pack_size(x.group) + 2 * sizeof(uint64_t);
}
