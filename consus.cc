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
	cmds.push_back(e::subcommand("transaction-manager",	"Start a new transaction manager"));
	cmds.push_back(e::subcommand("key-value-store",		"Start a new key value store"));
	cmds.push_back(e::subcommand("coordinator",			"Start a new coordinator"));
    cmds.push_back(e::subcommand("create-data-center",  "Create a new data center"));
    cmds.push_back(e::subcommand("set-default-data-center", "Set the default data center for new servers"));
    cmds.push_back(e::subcommand("availability-check",  "Check that the cluster has sufficient availability"));
    cmds.push_back(e::subcommand("debug",             	"Debug tools for Consus developers"));
    return dispatch_to_subcommands(argc, argv,
                                   "consus", "Consus",
                                   PACKAGE_VERSION,
                                   "consus-",
                                   "CONSUS_EXEC_PATH", CONSUS_EXEC_DIR,
                                   &cmds.front(), cmds.size());
}
