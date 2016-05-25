// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/partition.h"

using consus::partition;

partition :: partition()
    : index(0)
    , id()
    , owner()
    , next_id()
    , next_owner()
{
}

partition :: ~partition() throw ()
{
}

e::packer
consus :: operator << (e::packer lhs, const partition& rhs)
{
    return lhs << rhs.index << rhs.id << rhs.owner << rhs.next_id << rhs.next_owner;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, partition& rhs)
{
    return lhs >> rhs.index >> rhs.id >> rhs.owner >> rhs.next_id >> rhs.next_owner;
}
