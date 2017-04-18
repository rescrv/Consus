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

#ifndef consus_client_pending_h_
#define consus_client_pending_h_

// STL
#include <memory>

// e
#include <e/error.h>
#include <e/intrusive_ptr.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/ids.h"
#include "common/network_msgtype.h"
#include "common/transaction_group.h"

BEGIN_CONSUS_NAMESPACE
class client;

class pending
{
    public:
        pending(int64_t client_id, consus_returncode* status);
        virtual ~pending() throw ();

    public:
        int64_t client_id() const { return m_client_id; }
        void set_status(consus_returncode st) { *m_status = st; }
        consus_returncode status() const { return *m_status; }
        e::error error() const { return m_error; }

    // general stuff only useful for debugging, but then very useful
    public:
        virtual std::string describe() = 0;

    // called when this operation's id is returned to the client via loop/wait
    // will be called once per time the pending returns from these calls
    public:
        virtual void returning();

    // state machine
    public:
        virtual void kickstart_state_machine(client* cl) = 0;
        // the entity has been failed according to the configuration
        virtual void handle_server_failure(client* cl, comm_id si);
        // the entity has been disrupted according to busybee
        virtual void handle_server_disruption(client* cl, comm_id si);
        // a busybee message
        virtual void handle_busybee_op(client* cl,
                                       uint64_t nonce,
                                       std::auto_ptr<e::buffer> msg,
                                       e::unpacker up);
        virtual bool transaction_finished(client* cl, const transaction_group& tg, uint64_t outcome);

    // refcount
    protected:
        friend class e::intrusive_ptr<pending>;
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }
        size_t m_ref;

    // errors
    protected:
        std::ostream& error(const char* file, size_t line);
        void set_error(const e::error& err);
        void success();
        e::error m_error;

    // operation state
    private:
        int64_t m_client_id;
        consus_returncode* m_status;

    // noncopyable
    private:
        pending(const pending& other);
        pending& operator = (const pending& rhs);
};

#define PENDING_ERROR(CODE) \
    this->set_status(CONSUS_ ## CODE); \
    this->error(__FILE__, __LINE__)

END_CONSUS_NAMESPACE

#endif // consus_client_pending_h_
