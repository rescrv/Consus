// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// STL
#include <vector>

// e
#include <e/subcommand.h>

int
main(int argc, const char* argv[])
{
    std::vector<e::subcommand> cmds;
    cmds.push_back(e::subcommand("client-configuration",    "Show the client configuration"));
    cmds.push_back(e::subcommand("txman-configuration",     "Show the transaction manager configuration"));
    cmds.push_back(e::subcommand("kvs-configuration",       "Show the key value store configuration"));
    return dispatch_to_subcommands(argc, argv,
                                   "consus debug", "Consus",
                                   PACKAGE_VERSION,
                                   "consus-debug-",
                                   "CONSUS_EXEC_PATH", CONSUS_EXEC_DIR,
                                   &cmds.front(), cmds.size());
}
