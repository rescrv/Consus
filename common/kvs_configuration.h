// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_kvs_configuration_h_
#define consus_common_kvs_configuration_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"

BEGIN_CONSUS_NAMESPACE

e::unpacker kvs_configuration(e::unpacker up,
                              cluster_id* cid,
                              version_id* vid,
                              uint64_t* flags,
                              std::vector<kvs_state>* kvss);
std::string kvs_configuration(const cluster_id& cid,
                              const version_id& vid,
                              uint64_t flags,
                              const std::vector<kvs_state>& kvss);

END_CONSUS_NAMESPACE

#endif // consus_common_kvs_configuration_h_
