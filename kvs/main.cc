// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// POSIX
#include <signal.h>

// C++
#include <string>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/hostname.h>
#include <po6/net/location.h>

// e
#include <e/popt.h>

// BusyBee
#include <busybee_utils.h>

// replicant
#include <replicant.h>

// consus
#include "common/constants.h"
#include "common/macros.h"
#include "kvs/daemon.h"
#include "tools/connect_opts.h"

extern bool s_debug_mode;

int
main(int argc, const char* argv[])
{
    bool daemonize = true;
    const char* data = ".";
    const char* log = NULL;
    bool listen = false;
    const char* listen_host = "auto";
    long listen_port = CONSUS_PORT_KVS;
    const char* data_center = "";
    const char* pidfile = "";
    bool has_pidfile = false;
    long threads = 0;
    bool log_immediate = false;
    sigset_t ss;

    if (sigfillset(&ss) < 0 ||
        sigprocmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        std::cerr << "could not block signals";
        return EXIT_FAILURE;
    }

    consus::connect_opts conn('c', "connect", 'P', "connect-port", 'C', "connect-string");
    e::argparser ap;
    ap.autohelp();
    ap.arg().name('d', "daemon")
            .description("run in the background")
            .set_true(&daemonize);
    ap.arg().name('f', "foreground")
            .description("run in the foreground")
            .set_false(&daemonize);
    ap.arg().name('D', "data")
            .description("store persistent state in this directory (default: .)")
            .metavar("dir").as_string(&data);
    ap.arg().name('L', "log")
            .description("store logs in this directory (default: --data)")
            .metavar("dir").as_string(&log);
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: auto)")
            .metavar("IP").as_string(&listen_host).set_true(&listen);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: " STR(CONSUS_PORT_KVS) ")")
            .metavar("port").as_long(&listen_port).set_true(&listen);
    ap.arg().name('c', "data-center")
            .description("data center containing this key value store")
            .metavar("name").as_string(&data_center);
    ap.arg().long_name("pidfile")
            .description("write the PID to a file (default: don't)")
            .metavar("file").as_string(&pidfile).set_true(&has_pidfile);
    ap.arg().name('t', "threads")
            .description("the number of threads which will handle network traffic")
            .metavar("N").as_long(&threads);
    ap.arg().long_name("log-immediate")
            .description("immediately flush all log output")
            .set_true(&log_immediate).hidden();
    ap.arg().long_name("debug")
            .description("start in debug mode")
            .set_true(&s_debug_mode).hidden();
    ap.add("Connect to the cluster:", conn.parser());

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no positional arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (listen_port >= (1 << 16) || listen_port <= 0)
    {
        std::cerr << "listen-port is out of range" << std::endl;
        return EXIT_FAILURE;
    }

    po6::net::ipaddr listen_ip;
    po6::net::location bind_to;

    if (strcmp(listen_host, "auto") == 0)
    {
        if (!busybee_discover(&listen_ip))
        {
            std::cerr << "cannot automatically discover local address; specify one manually" << std::endl;
            return EXIT_FAILURE;
        }

        bind_to = po6::net::location(listen_ip, listen_port);
    }
    else
    {
        if (listen_ip.set(listen_host))
        {
            bind_to = po6::net::location(listen_ip, listen_port);
        }

        if (bind_to == po6::net::location())
        {
            bind_to = po6::net::hostname(listen_host, 0).lookup(AF_UNSPEC, IPPROTO_TCP);
            bind_to.port = listen_port;
        }
    }

    if (bind_to == po6::net::location())
    {
        std::cerr << "cannot interpret listen address as hostname or IP address" << std::endl;
        return EXIT_FAILURE;
    }

    if (bind_to.address == po6::net::ipaddr::ANY())
    {
        std::cerr << "cannot bind to " << bind_to << " because it is not routable" << std::endl;
        return EXIT_FAILURE;
    }

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (log_immediate)
    {
        FLAGS_logbufsecs = 0;
    }

    if (threads <= 0)
    {
        threads += sysconf(_SC_NPROCESSORS_ONLN);

        if (threads <= 0)
        {
            std::cerr << "cannot create a non-positive number of threads" << std::endl;
            return EXIT_FAILURE;
        }
    }
    else if (threads > 512)
    {
        std::cerr << "refusing to create more than 512 threads" << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        consus::daemon d;
        return d.run(daemonize,
                     std::string(data),
                     std::string(log ? log : data),
                     std::string(pidfile), has_pidfile,
                     listen, bind_to,
                     conn.isset(), conn.conn_str(),
                     data_center, threads);
    }
    catch (std::exception& e)
    {
        std::cerr << "error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
