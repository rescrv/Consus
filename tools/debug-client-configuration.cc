// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/guard.h>
#include <e/popt.h>

// consus
#include <consus.h>
#include "client/consus-internal.h"
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    consus::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "consus-debug-client-configuration: invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "consus-debug-client-configuration takes zero positional arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    consus_client* cl = consus_create_conn_str(conn.conn_str());

    if (!cl)
    {
        std::cerr << "consus-debug-client-configuration: memory allocation failed" << std::endl;
        return EXIT_FAILURE;
    }

    e::guard g_cl = e::makeguard(consus_destroy, cl);
    consus_returncode rc;
    const char* str = NULL;

    if (consus_debug_client_configuration(cl, &rc, &str) < 0)
    {
        std::cerr << "consus-debug-client-configuration: " << consus_error_message(cl) << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << str << std::flush;
    return EXIT_SUCCESS;
}
