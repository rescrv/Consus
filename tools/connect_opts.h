// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
