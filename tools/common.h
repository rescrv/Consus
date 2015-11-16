// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_tools_common_h_
#define consus_tools_common_h_

// STL
#include <string>

// e
#include <e/popt.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "tools/connect_opts.h"

BEGIN_CONSUS_NAMESPACE

bool finish(consus_client* cl, const char* prog, int64_t id, consus_returncode* status);

END_CONSUS_NAMESPACE

#endif // consus_tools_common_h_
