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
    long txmans = 0;
    long txman_groups = 0;
    long kvss = 0;
    long timeout = 10;
    consus::connect_opts conn;
    e::argparser ap;
    ap.autohelp();
    ap.option_string("[OPTIONS]");
    ap.arg().long_name("transaction-managers")
            .description("wait for N transaction-managers to join the cluster (default: 0)")
            .metavar("N").as_long(&txmans);
    ap.arg().long_name("transaction-manager-groups")
            .description("wait for N transaction-manager groups to form (default: 0)")
            .metavar("N").as_long(&txman_groups);
    ap.arg().long_name("key-value-stores")
            .description("wait for N key-value-stores to join the cluster (default: 0)")
            .metavar("N").as_long(&kvss);
    ap.arg().name('t', "timeout")
            .description("wait at most S seconds (default: 10)")
            .metavar("S").as_long(&timeout);
    ap.add("Connect to a cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "consus-availability-check: invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "consus-availability-check takes zero positional arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (txmans < 0 || kvss < 0)
    {
        std::cerr << "consus-availability-check: negative values make no sense\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    consus_client* cl = consus_create_conn_str(conn.conn_str());

    if (!cl)
    {
        std::cerr << "consus-availability-check: memory allocation failed" << std::endl;
        return EXIT_FAILURE;
    }

    e::guard g_cl = e::makeguard(consus_destroy, cl);
    consus_returncode rc;
    consus_availability_requirements reqs;
    reqs.txmans = txmans;
    reqs.txman_groups = txman_groups;
    reqs.kvss = kvss;

    if (consus_admin_availability_check(cl, &reqs, timeout, &rc) < 0)
    {
        std::cerr << "consus-availability-check: " << consus_error_message(cl) << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
