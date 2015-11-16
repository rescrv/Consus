// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// C
#include <cstdio>
#include <limits.h>
#include <stdint.h>

// POSIX
#include <sys/stat.h>

// C++
#include <iostream>

// STL
#include <vector>

// po6
#include <po6/errno.h>
#include <po6/io/fd.h>

// consus
#include "tools/common.h"

#ifdef CONSUS_EXEC_DIR
#define CONSUS_LIB_NAME "libconsus-coordinator"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wlarger-than="
static bool
locate_coordinator_lib(const char* argv0, std::string* path)
{
    // find the right library
    std::vector<std::string> paths;
    const char* env = getenv("CONSUS_COORD_LIB");
    static const char* exts[] = { "", ".so.0.0.0", ".so.0", ".so", ".dylib", 0 };

    for (size_t i = 0; exts[i]; ++i)
    {
        std::string base(CONSUS_LIB_NAME);
        base += exts[i];
        paths.push_back(po6::path::join(CONSUS_EXEC_DIR, base));
        paths.push_back(po6::path::join(po6::path::dirname(argv0), ".libs", base));

        if (env)
        {
            std::string envlib(env);
            envlib += exts[i];
            paths.push_back(envlib);
        }
    }

    // maybe we're running out of Git.  make it "just work"
    char selfbuf[PATH_MAX + 1];
    memset(selfbuf, 0, sizeof(selfbuf));

    if (readlink("/proc/self/exe", selfbuf, PATH_MAX) >= 0)
    {
        std::string workdir(selfbuf);
        workdir = po6::path::dirname(workdir);
        std::string gitdir(po6::path::join(workdir, ".git"));
        struct stat buf;

        if (stat(gitdir.c_str(), &buf) == 0 &&
            S_ISDIR(buf.st_mode))
        {
            std::string libdir(po6::path::join(workdir, ".libs"));

            for (size_t i = 0; exts[i]; ++i)
            {
                std::string libname(CONSUS_LIB_NAME);
                libname += exts[i];
                paths.push_back(po6::path::join(libdir, libname));
            }
        }
    }

    size_t idx = 0;

    while (idx < paths.size())
    {
        struct stat buf;

        if (stat(paths[idx].c_str(), &buf) == 0)
        {
            *path = paths[idx];
            return true;
        }

        ++idx;
    }

    return false;
}
#pragma GCC diagnostic pop
#undef CONSUS_LIB_NAME
#endif // CONSUS_EXEC_DIR

int
main(int argc, const char* argv[])
{
    std::string libpath;

    if (!locate_coordinator_lib(argv[0], &libpath))
    {
        std::cerr << "cannot locate the consus coordinator library" << std::endl;
        return EXIT_FAILURE;
    }

    // setup the environment
    if (setenv("REPLICANT_WRAP", "consus-coordinator", 1) < 0)
    {
        std::cerr << "could not setup the environment: " << po6::strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    // generate a random token
    uint64_t token;
    po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

    if (sysrand.get() < 0 ||
        sysrand.read(&token, sizeof(token)) != sizeof(token))
    {
        std::cerr << "could not generate random token for cluster" << std::endl;
        return EXIT_FAILURE;
    }

    char token_buf[21];
    snprintf(token_buf, 21, "%lu", (unsigned long) token);

    // exec replicant daemon
    std::vector<const char*> args;
    args.push_back("replicant");
    args.push_back("daemon");

    for (int i = 1; i < argc; ++i)
    {
        args.push_back(argv[i]);
    }

    args.push_back("--object");
    args.push_back("consus");
    args.push_back("--library");
    args.push_back(libpath.c_str());
    args.push_back("--init-string");
    args.push_back(token_buf);
    args.push_back(NULL);

    if (execvp("replicant", const_cast<char*const*>(&args[0])) < 0)
    {
        perror("could not exec replicant");
        return EXIT_FAILURE;
    }

    abort();
}
