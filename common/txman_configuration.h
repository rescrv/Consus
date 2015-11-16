// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_txman_configuration_h_
#define consus_common_txman_configuration_h_

// consus
#include "namespace.h"
#include "common/data_center.h"
#include "common/ids.h"
#include "common/kvs.h"
#include "common/paxos_group.h"
#include "common/txman_state.h"

BEGIN_CONSUS_NAMESPACE

e::unpacker txman_configuration(e::unpacker up,
                                cluster_id* cid,
                                version_id* vid,
                                uint64_t* flags,
                                std::vector<data_center>* dcs,
                                std::vector<txman_state>* txmans,
                                std::vector<paxos_group>* txman_groups,
                                std::vector<kvs>* kvss);
std::string txman_configuration(const cluster_id& cid,
                                const version_id& vid,
                                uint64_t flags,
                                const std::vector<data_center>& dcs,
                                const std::vector<txman_state>& txmans,
                                const std::vector<paxos_group>& txman_groups,
                                const std::vector<kvs>& kvss);

END_CONSUS_NAMESPACE

#endif // consus_common_txman_configuration_h_
