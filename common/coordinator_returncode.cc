// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/coordinator_returncode.h"

e::packer
consus :: operator << (e::packer lhs, const coordinator_returncode& rhs)
{
    return lhs << e::pack_uint16<coordinator_returncode>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, coordinator_returncode& rhs)
{
    return lhs >> e::unpack_uint16<coordinator_returncode>(rhs);
}

size_t
consus :: pack_size(const coordinator_returncode&)
{
    return sizeof(uint16_t);
}
