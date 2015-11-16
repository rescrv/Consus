// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_pending_string_h_
#define consus_client_pending_string_h_

// consus
#include "client/pending.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE

class pending_string : public pending
{
    public:
        pending_string(const char* s);
        pending_string(const std::string& s);
        virtual ~pending_string() throw ();

    public:
        virtual std::string describe();
        virtual void kickstart_state_machine(client*) {}
        const char* string() { return m_str.c_str(); }

    private:
        friend class e::intrusive_ptr<pending_string>;

    private:
        std::string m_str;

    private:
        pending_string(const pending_string&);
        pending_string& operator = (const pending_string&);
};

END_CONSUS_NAMESPACE

#endif // consus_client_pending_string_h_
