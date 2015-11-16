// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_crc32c_h_
#define consus_common_crc32c_h_

// C
#include <stdint.h>
#include <stdlib.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

uint32_t
crc32c(uint32_t init, const unsigned char* data, size_t n);

END_CONSUS_NAMESPACE

#endif // consus_common_crc32c_h_
