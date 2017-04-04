// Copyright (c) 2017, Robert Escriva, Cornell University
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

#ifndef consus_common_transmit_limiter_h_
#define consus_common_transmit_limiter_h_

// The "work_state_machine" paradigm employed nearly everywhere within Consus
// ensures that the system always makes progress toward a valid state.  But
// because work_state_machine is called each and every time a state transition
// happens, sending the requisite messages to make progress each and every time
// it is called is dangerous and can cause a chaotic feedback loop.
//
// The transmit_limiter ensures that messages are transmitted at least once,
// and then with some periodicity.  It also handles integrating with the durable
// log to ensure that a message delayed waiting for a log entry to become
// durable will not be retransmitted.  Absent this mechanism, it is possible
// for duplicate retransmitted messages to end up enqueued waiting for a single
// log entry to become durable.

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

template <typename T, class daemon>
class transmit_limiter
{
    public:
        transmit_limiter();
        ~transmit_limiter() throw ();

    public:
        const T& value() const { return m_value; }
        void skip_transmissions(unsigned skip) { m_skip_transmissions = skip; }
        bool may_transmit(const T& value, uint64_t now, daemon* d);
        void transmit_now(const T& value, uint64_t now);
        void transmit_now(const T& value, uint64_t now, uint64_t log, uint64_t* durable,
                          void (daemon::**func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>));

    private:
        uint64_t m_last_transmitted;
        uint64_t m_log_durable_seqno;
        unsigned m_skip_transmissions;
        unsigned m_skip_countdown;
        T m_value;
};

template <typename T, class daemon>
transmit_limiter<T, daemon> :: transmit_limiter()
    : m_last_transmitted(0)
    , m_log_durable_seqno(0)
    , m_skip_transmissions(0)
    , m_skip_countdown(0)
    , m_value()
{
}

template <typename T, class daemon>
transmit_limiter<T, daemon> :: ~transmit_limiter() throw ()
{
}

template <typename T, class daemon>
bool
transmit_limiter<T, daemon> :: may_transmit(const T& value, uint64_t now, daemon* d)
{
    if (m_value != value)
    {
        m_skip_countdown = m_skip_transmissions;
    }

    bool may = m_value != value ||
               m_last_transmitted + d->resend_interval() < now;

    if (may && m_skip_countdown > 0)
    {
        --m_skip_countdown;
        transmit_now(value, now);
        return false;
    }

    return may;
}

template <typename T, class daemon>
void
transmit_limiter<T, daemon> :: transmit_now(const T& value, uint64_t now)
{
    m_value = value;
    m_last_transmitted = now;
}

template <typename T, class daemon>
void
transmit_limiter<T, daemon> :: transmit_now(const T& value, uint64_t now, uint64_t log, uint64_t* durable,
                                            void (daemon::**func)(int64_t, paxos_group_id, std::auto_ptr<e::buffer>))
{
    if (m_value != value)
    {
        m_value = value;
        m_log_durable_seqno = log;
        *durable = log;
        *func = &daemon::send_when_durable;
    }
    else
    {
        *durable = m_log_durable_seqno;
        *func = &daemon::send_if_durable;
    }

    m_last_transmitted = now;
}

END_CONSUS_NAMESPACE

#endif // consus_common_transmit_limiter_h_
