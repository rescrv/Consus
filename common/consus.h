// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_consus_h_
#define consus_common_consus_h_

// e
#include <e/serialization.h>

// consus
#include <consus.h>
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

std::ostream&
operator << (std::ostream& lhs, const consus_returncode& rhs);

e::packer
operator << (e::packer lhs, const consus_returncode& rhs);
e::unpacker
operator >> (e::unpacker lhs, consus_returncode& rhs);
size_t
pack_size(const consus_returncode& p);

END_CONSUS_NAMESPACE

#endif // consus_common_consus_h_
