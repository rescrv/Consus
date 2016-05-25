// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
