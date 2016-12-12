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

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// consus
#include "common/background_thread.h"

using po6::threads::make_obj_func;
using consus::background_thread;

background_thread :: background_thread(e::garbage_collector* gc)
    : m_thread(make_obj_func(&background_thread::run, this))
    , m_gc(gc)
    , m_protect()
    , m_wakeup_thread(&m_protect)
    , m_shutdown(true)
{
    po6::threads::mutex::hold hold(&m_protect);
}

background_thread :: ~background_thread() throw ()
{
    shutdown();
}

void
background_thread :: start()
{
    po6::threads::mutex::hold hold(&m_protect);
    m_thread.start();
    m_shutdown = false;
}

void
background_thread :: shutdown()
{
    bool already_shutdown;

    {
        po6::threads::mutex::hold hold(&m_protect);
        m_wakeup_thread.broadcast();
        already_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!already_shutdown)
    {
        m_thread.join();
    }
}

void
background_thread :: run()
{
    LOG(INFO) << this->thread_name() << " thread started";
    block_signals();
    e::garbage_collector::thread_state ts;
    m_gc->register_thread(&ts);

    while (true)
    {
        {
            m_gc->quiescent_state(&ts);
            po6::threads::mutex::hold hold(&m_protect);

            while (!this->have_work() && !m_shutdown)
            {
                m_gc->offline(&ts);
                m_wakeup_thread.wait();
                m_gc->online(&ts);
            }

            if (m_shutdown)
            {
                break;
            }
        }

        this->do_work();
    }

    m_gc->deregister_thread(&ts);
    LOG(INFO) << this->thread_name() << " thread stopped";
}

void
background_thread :: block_signals()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }
}
