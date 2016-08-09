// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_common_lock_op_h_
#define consus_common_lock_op_h_

// C++
#include <iostream>

// e
#include <e/serialization.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

enum lock_op
{
    LOCK_LOCK   = 1,
    LOCK_UNLOCK = 2
};

std::ostream&
operator << (std::ostream& lhs, lock_op rhs);

e::packer
operator << (e::packer lhs, const lock_op& rhs);
e::unpacker
operator >> (e::unpacker lhs, lock_op& rhs);
size_t
pack_size(const lock_op& rhs);

END_CONSUS_NAMESPACE

#endif // consus_common_lock_op_h_
