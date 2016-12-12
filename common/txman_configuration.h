// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
