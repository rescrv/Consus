// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/guard.h>
#include <e/popt.h>

// consus
#include <consus-admin.h>
#include "tools/common.h"

int
main(int argc, const char* argv[])
{
    consus::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS] <dc-name>");
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "consus-create-data-center: invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 1)
    {
        std::cerr << "consus-create-data-center takes one positional argument\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    consus_client* cl = consus_create_conn_str(conn.conn_str());

    if (!cl)
    {
        std::cerr << "consus-create-data-center: memory allocation failed" << std::endl;
        return EXIT_FAILURE;
    }

    e::guard g_cl = e::makeguard(consus_destroy, cl);
    consus_returncode rc;

    if (consus_admin_create_data_center(cl, ap.args()[0], &rc) < 0)
    {
        std::cerr << "consus-create-data-center: " << consus_error_message(cl) << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
