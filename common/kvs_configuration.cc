// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// STL
#include <sstream>

// consus
#include "common/kvs_configuration.h"

e::unpacker
consus :: kvs_configuration(e::unpacker up,
                            cluster_id* cid,
                            version_id* vid,
                            uint64_t* flags,
                            std::vector<kvs_state>* kvss)
{
    return up >> *cid >> *vid >> *flags >> *kvss;
}

std::string
consus :: kvs_configuration(const cluster_id& cid,
                              const version_id& vid,
                              uint64_t,
                              const std::vector<kvs_state>& kvss)
{
    std::ostringstream ostr;
    ostr << cid << "\n"
         << vid << "\n";

    if (kvss.empty())
    {
        ostr << "no key-value stores\n";
    }
    else if (kvss.size() == 1)
    {
        ostr << "1 key-value store:\n";
    }
    else
    {
        ostr << kvss.size() << " key-value stores:\n";
    }

    for (size_t i = 0; i < kvss.size(); ++i)
    {
        ostr << kvss[i] << "\n";
    }

    return ostr.str();
}
