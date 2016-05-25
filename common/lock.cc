// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// consus
#include "common/lock.h"
#include "common/macros.h"

std::ostream&
consus :: operator << (std::ostream& lhs, lock_t rhs)
{
    switch (rhs)
    {
        STRINGIFY(LOCK_SHARE);
        STRINGIFY(LOCK_ATOMIC);
        STRINGIFY(LOCK_EXCL);
        default:
            lhs << "unknown lock_op";
    }

    return lhs;
}

std::ostream&
consus :: operator << (std::ostream& lhs, lock_op rhs)
{
    switch (rhs)
    {
        STRINGIFY(LOCK_LOCK);
        STRINGIFY(LOCK_UNLOCK);
        default:
            lhs << "unknown lock_op";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const lock_t& rhs)
{
    return lhs << e::pack_uint8<lock_t>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, lock_t& rhs)
{
    return lhs >> e::unpack_uint8<lock_t>(rhs);
}

size_t
consus :: pack_size(const lock_t&)
{
    return sizeof(uint8_t);
}

e::packer
consus :: operator << (e::packer lhs, const lock_op& rhs)
{
    return lhs << e::pack_uint8<lock_op>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, lock_op& rhs)
{
    return lhs >> e::unpack_uint8<lock_op>(rhs);
}

size_t
consus :: pack_size(const lock_op&)
{
    return sizeof(uint8_t);
}
