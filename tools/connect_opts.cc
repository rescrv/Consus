// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// replicant
#include <replicant.h>

// consus
#include "tools/connect_opts.h"

using consus::connect_opts;

connect_opts :: connect_opts()
    : m_ap()
    , m_connect1(false)
    , m_connect_host("127.0.0.1")
    , m_connect_port(1982)
    , m_connect2(false)
    , m_connect_string(NULL)
    , m_valid_conn_str()
{
    create_parser('h', "host", 'p', "port", 'c', "cluster");
}

connect_opts :: connect_opts(char hn, const char* host_name,
                             char pn, const char* port_name,
                             char sn, const char* str_name)
    : m_ap()
    , m_connect1(false)
    , m_connect_host("127.0.0.1")
    , m_connect_port(1982)
    , m_connect2(false)
    , m_connect_string(NULL)
    , m_valid_conn_str()
{
    create_parser(hn, host_name, pn, port_name, sn, str_name);
}

connect_opts :: ~connect_opts() throw ()
{
}

bool
connect_opts :: validate()
{
    char* conn_str = NULL;

    if (m_connect1 && m_connect2)
    {
        conn_str = replicant_client_add_to_conn_str(m_connect_string, m_connect_host, m_connect_port);
    }
    else if (m_connect1)
    {
        conn_str = replicant_client_host_to_conn_str(m_connect_host, m_connect_port);
    }
    else if (m_connect2)
    {
        conn_str = replicant_client_validate_conn_str(m_connect_string);
    }
    else
    {
        conn_str = strdup("127.0.0.1:1982");
    }

    if (!conn_str)
    {
        return false;
    }

    m_valid_conn_str = conn_str;
    free(conn_str);
    return true;
}

void
connect_opts :: create_parser(char hn, const char* host_name,
                              char pn, const char* port_name,
                              char sn, const char* str_name)
{
    m_ap.arg().name(hn, host_name)
              .description("connect to an IP address or hostname (default: 127.0.0.1)")
              .metavar("addr").as_string(&m_connect_host).set_true(&m_connect1);
    m_ap.arg().name(pn, port_name)
              .description("connect to an alternative port (default: 1982)")
              .metavar("port").as_long(&m_connect_port).set_true(&m_connect1);
    m_ap.arg().name(sn, str_name)
              .description("connect to a list of hosts (default: none)")
              .metavar("hosts").as_string(&m_connect_string).set_true(&m_connect2);
}

bool
connect_opts :: isset()
{
    return m_connect1 || m_connect2;
}

const char*
connect_opts :: conn_str()
{
    bool valid = validate();
    assert(valid);
    return m_valid_conn_str.c_str();
}
