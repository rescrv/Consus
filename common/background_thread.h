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

#ifndef consus_common_background_thread_h_
#define consus_common_background_thread_h_

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/garbage_collector.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

class background_thread
{
    public:
        background_thread(e::garbage_collector* gc);
        virtual ~background_thread() throw ();

    public:
        void start();
        void shutdown();

    protected:
        po6::threads::mutex* mtx() { return &m_protect; }
        void wakeup() { m_wakeup_thread.signal(); }
        virtual const char* thread_name() = 0;
        virtual bool have_work() = 0;
        virtual void do_work() = 0;

    private:
        void run();
        void block_signals();

    private:
        background_thread(const background_thread&);
        background_thread& operator = (const background_thread&);

    private:
        po6::threads::thread m_thread;
        e::garbage_collector* m_gc;
        po6::threads::mutex m_protect;
        po6::threads::cond m_wakeup_thread;
        bool m_shutdown;
};

END_CONSUS_NAMESPACE

#endif // consus_common_background_thread_h_
