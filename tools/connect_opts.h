// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_tools_connect_opts_h_
#define consus_tools_connect_opts_h_

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
        connect_opts(char hn, const char* host_name,
                     char pn, const char* port_name,
                     char sn, const char* str_name);
        ~connect_opts() throw ();

    public:
        const e::argparser& parser() { return m_ap; }
        bool validate();
        bool isset();
        const char* conn_str();

    private:
        void create_parser(char hn, const char* host_name,
                           char pn, const char* port_name,
                           char sn, const char* str_name);

    private:
        e::argparser m_ap;
        bool m_connect1;
        const char* m_connect_host;
        long m_connect_port;
        bool m_connect2;
        const char* m_connect_string;
        std::string m_valid_conn_str;

    private:
        connect_opts(const connect_opts&);
        connect_opts& operator = (const connect_opts&);
};

END_CONSUS_NAMESPACE

#endif // consus_tools_connect_opts_h_
