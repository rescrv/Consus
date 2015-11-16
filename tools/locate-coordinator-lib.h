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

BEGIN_CONSUS_NAMESPACE

class connect_opts
{
    public:
        connect_opts();
        ~connect_opts() throw ();

    public:
        const e::argparser& parser() { return m_ap; }
        bool validate();
        consus_client* create();

    private:
        e::argparser m_ap;

    private:
        connect_opts(const connect_opts&);
        connect_opts& operator = (const connect_opts&);
};

bool finish(consus_client* cl, const char* prog, int64_t id, consus_returncode* status);
bool locate_coordinator_lib(const char* argv0, std::string* path);

END_CONSUS_NAMESPACE

#endif // consus_tools_common_h_
