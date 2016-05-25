// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
