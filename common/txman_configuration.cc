// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// STL
#include <sstream>

// consus
#include "common/txman_configuration.h"

e::unpacker
consus :: txman_configuration(e::unpacker up,
                              cluster_id* cid,
                              version_id* vid,
                              uint64_t* flags,
                              std::vector<data_center>* dcs,
                              std::vector<txman_state>* txmans,
                              std::vector<paxos_group>* txman_groups,
                              std::vector<kvs>* kvss)
{
    return up >> *cid >> *vid >> *flags >> *dcs >> *txmans >> *txman_groups >> *kvss;
}

std::string
consus :: txman_configuration(const cluster_id& cid,
                              const version_id& vid,
                              uint64_t,
                              const std::vector<data_center>& dcs,
                              const std::vector<txman_state>& txmans,
                              const std::vector<paxos_group>& txman_groups,
                              const std::vector<kvs>& kvss)
{
    std::ostringstream ostr;
    ostr << cid << "\n"
         << vid << "\n";

    if (dcs.empty())
    {
        ostr << "default data center only\n";
    }
    else if (dcs.size() == 1)
    {
        ostr << "1 configured data center:\n";
    }
    else
    {
        ostr << dcs.size() << " configured data centers:\n";
    }

    for (size_t i = 0; i < dcs.size(); ++i)
    {
        ostr << dcs[i] << "\n";
    }

    if (txmans.empty())
    {
        ostr << "no transaction managers\n";
    }
    else if (txmans.size() == 1)
    {
        ostr << "1 transaction manager:\n";
    }
    else
    {
        ostr << txmans.size() << " transaction managers:\n";
    }

    for (size_t i = 0; i < txmans.size(); ++i)
    {
        ostr << txmans[i] << "\n";
    }

    if (txman_groups.empty())
    {
        ostr << "no paxos groups\n";
    }
    else if (txman_groups.size() == 1)
    {
        ostr << "1 paxos group:\n";
    }
    else
    {
        ostr << txman_groups.size() << " paxos groups:\n";
    }

    for (size_t i = 0; i < txman_groups.size(); ++i)
    {
        ostr << txman_groups[i] << "\n";
    }

    if (kvss.empty())
    {
        ostr << "no key value stores\n";
    }
    else if (kvss.size() == 1)
    {
        ostr << "1 key value store\n";
    }
    else
    {
        ostr << kvss.size() << " key value stores:\n";
    }

    for (size_t i = 0; i < kvss.size(); ++i)
    {
        ostr << kvss[i] << "\n";
    }

    return ostr.str();
}
