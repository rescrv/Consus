// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#define __STDC_FORMAT_MACROS

// C
#include <inttypes.h>

// STL
#include <algorithm>
#include <map>
#include <sstream>
#include <string>

// e
#include <e/strescape.h>

// consus
#include "common/coordinator_returncode.h"
#include "common/macros.h"
#include "coordinator/coordinator.h"
#include "coordinator/util.h"

#pragma GCC diagnostic ignored "-Wlarger-than="

using consus::coordinator;

namespace
{

template <typename T>
std::string
to_string(const T& t)
{
    std::ostringstream ostr;
    ostr << t;
    return ostr.str();
}

template <typename T, typename TID>
T*
get_by_id(std::vector<T>& TS, TID id)
{
    for (size_t i = 0; i < TS.size(); ++i)
    {
        if (TS[i].id() == id)
        {
            return &TS[i];
        }
    }

    return NULL;
}

template <typename T, typename TID>
bool
address_is_free(std::vector<T>& TS, TID id, const po6::net::location& bind_to)
{
    for (size_t i = 0; i < TS.size(); ++i)
    {
        if (TS[i].id() != id &&
            TS[i].bind_to() == bind_to)
        {
            return false;
        }
    }

    return true;
}

} // namespace

coordinator :: coordinator()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_counter(1)
    , m_dc_default()
    , m_dcs()
    , m_txmans()
    , m_txman_groups()
    , m_txman_quiescence_counter(0)
    , m_txmans_changed(false)
    , m_kvss()
    , m_kvs_quiescence_counter(0)
    , m_kvss_changed(false)
{
}

coordinator :: ~coordinator() throw ()
{
}

void
coordinator :: init(rsm_context* ctx, uint64_t token)
{
    if (m_cluster != cluster_id())
    {
        rsm_log(ctx, "cannot initialize consus cluster(%" PRIu64 ") "
                     "because it is already initialized to cluster(%" PRIu64 ")\n", token, m_cluster.get());
        // we lie to the client and pretend all is well
        return generate_response(ctx, COORD_SUCCESS);
    }

    rsm_tick_interval(ctx, "tick", 1 /*PER SECOND*/);
    rsm_log(ctx, "initializing consus cluster(%" PRIu64 ")\n", token);
    m_cluster = cluster_id(token);
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

#define INVARIANT(X) \
    do { \
        if (!(X)) { \
            rsm_log(ctx, "invariant violated: %s\n", STR(X)); \
            rsm_log(ctx, "                    at %s:%d\n", __FILE__, __LINE__); \
            abort(); \
        } \
    } while (0)

void
coordinator :: invariant_check(rsm_context* ctx)
{
    INVARIANT(m_dc_default == data_center_id() || get_data_center(m_dc_default));

    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        INVARIANT(m_dcs[i].id.get() < m_counter);
    }

    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        INVARIANT(get_data_center(m_txmans[i].tx.dc));
    }

    for (size_t i = 0; i < m_txman_groups.size(); ++i)
    {
        INVARIANT(m_txman_groups[i].id.get() < m_counter);
        INVARIANT(get_data_center(m_txman_groups[i].dc));

        for (unsigned m = 0; m < m_txman_groups[i].members_sz; ++m)
        {
            txman_state* ts = get_txman(m_txman_groups[i].members[m]);
            INVARIANT(ts);
            INVARIANT(ts->tx.id == m_txman_groups[i].members[m]);
            INVARIANT(ts->tx.dc == m_txman_groups[i].dc);
        }
    }

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        INVARIANT(get_data_center(m_kvss[i].kv.dc));
    }
}

consus::data_center*
coordinator :: get_data_center(data_center_id id)
{
    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        if (m_dcs[i].id == id)
        {
            return &m_dcs[i];
        }
    }

    return NULL;
}

consus::data_center*
coordinator :: get_data_center(const std::string& name)
{
    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        if (m_dcs[i].name == name)
        {
            return &m_dcs[i];
        }
    }

    return NULL;
}

consus::data_center*
coordinator :: new_data_center(const std::string& name)
{
    assert(!get_data_center(name));
    data_center_id id(m_counter);
    ++m_counter;
    m_dcs.push_back(data_center(id, name));
    return &m_dcs.back();
}

void
coordinator :: data_center_create(rsm_context* ctx, const std::string& name)
{
    data_center* dc = get_data_center(name);

    if (dc)
    {
        rsm_log(ctx, "data center \"%s\" already exists", e::strescape(name).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    dc = new_data_center(name);
    rsm_log(ctx, "created data center %s", e::strescape(name).c_str());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: data_center_default(rsm_context* ctx, const std::string& name)
{
    data_center* dc = get_data_center(name);

    if (!dc)
    {
        rsm_log(ctx, "cannot make \"%s\" the default data center because it doesn't exist", e::strescape(name).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    m_dc_default = dc->id;
    return generate_response(ctx, COORD_SUCCESS);
}

consus::txman_state*
coordinator :: get_txman(comm_id tx)
{
    return get_by_id(m_txmans, tx);
}

consus::txman_state*
coordinator :: new_txman(const txman& t)
{
    assert(!get_txman(t.id));
    m_txmans.push_back(txman_state(t));
    return &m_txmans.back();
}

void
coordinator :: txman_register(rsm_context* ctx, const txman& t)
{
    txman_state* ts = get_txman(t.id);

    if (ts)
    {
        rsm_log(ctx, "register %s failed: already registered", to_string(t).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    ts = new_txman(t);
    ts->tx.dc = m_dc_default;
    rsm_log(ctx, "registered %s", to_string(t).c_str());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: txman_online(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce)
{
    txman_state* ts = get_txman(id);

    if (!ts)
    {
        rsm_log(ctx, "cannot bring transaction manager %" PRIu64
                     " online because it is not registered", id.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (ts->state != txman_state::REGISTERED &&
        ts->state != txman_state::ONLINE &&
        ts->state != txman_state::OFFLINE)
    {
        rsm_log(ctx, "cannot bring transaction manager %" PRIu64
                     " online because the server is "
                     "%s\n", id.get(), txman_state::to_string(ts->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    bool changed = false;

    if (ts->state != txman_state::ONLINE)
    {
        rsm_log(ctx, "changing transaction manager %" PRIu64
                     " from %s to %s\n",
                     id.get(), txman_state::to_string(ts->state),
                     txman_state::to_string(txman_state::ONLINE));
        ts->state = txman_state::ONLINE;
        ts->nonce = nonce;
        changed = true;
        txman_availability_changed();
    }
    else if (ts->nonce != nonce)
    {
        rsm_log(ctx, "changing transaction manager %" PRIu64
                     "'s nonce from %" PRIu64 " to %" PRIu64 "\n",
                     id.get(), ts->nonce, nonce);
        ts->nonce = nonce;
        changed = true;
    }

    if (ts->tx.bind_to != bind_to)
    {
        std::string from(to_string(ts->tx.bind_to));
        std::string to(to_string(bind_to));

        if (address_is_free(m_txmans, id, bind_to))
        {
            rsm_log(ctx, "changing transaction manager %" PRIu64 "'s address from %s to %s\n",
                         id.get(), from.c_str(), to.c_str());
            ts->tx.bind_to = bind_to;
            changed = true;
        }
        else
        {
            rsm_log(ctx, "cannot change transaction manager %" PRIu64 " to %s "
                         "because that address is already in use\n", id.get(), to.c_str());
            return generate_response(ctx, consus::COORD_DUPLICATE);
        }
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: txman_offline(rsm_context* ctx, comm_id id, const po6::net::location&, uint64_t nonce)
{
    txman_state* ts = get_txman(id);

    if (!ts)
    {
        rsm_log(ctx, "cannot bring transaction manager %" PRIu64
                     " offline because it is not registered", id.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (ts->nonce == nonce && ts->state == txman_state::ONLINE)
    {
        rsm_log(ctx, "changing transaction manager %" PRIu64 " from %s to %s\n",
                     id.get(), txman_state::to_string(ts->state),
                     txman_state::to_string(txman_state::OFFLINE));
        ts->state = txman_state::OFFLINE;
        txman_availability_changed();
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

consus::kvs_state*
coordinator :: get_kvs(comm_id lk)
{
    return get_by_id(m_kvss, lk);
}

consus::kvs_state*
coordinator :: new_kvs(const kvs& k)
{
    assert(!get_kvs(k.id));
    m_kvss.push_back(kvs_state(k));
    return &m_kvss.back();
}

#if 0
consus::ring*
coordinator :: get_or_create_ring(data_center_id id)
{
    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        if (m_rings[i].dc == id)
        {
            return &m_rings[i];
        }
    }

    m_rings.push_back(ring(id));
    return &m_rings.back();
}
#endif

void
coordinator :: kvs_register(rsm_context* ctx, const kvs& k)
{
    kvs_state* kv = get_kvs(k.id);

    if (kv)
    {
        rsm_log(ctx, "register %s failed: already registered", to_string(k).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    kv = new_kvs(k);
    kv->kv.dc = m_dc_default;
    rsm_log(ctx, "registered %s", to_string(k).c_str());
    generate_next_configuration(ctx);
    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: kvs_online(rsm_context* ctx, comm_id id, const po6::net::location& bind_to, uint64_t nonce)
{
    kvs_state* kv = get_kvs(id);

    if (!kv)
    {
        rsm_log(ctx, "cannot bring key value store %" PRIu64
                     " online because it is not registered", id.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (kv->state != kvs_state::REGISTERED &&
        kv->state != kvs_state::ONLINE &&
        kv->state != kvs_state::OFFLINE)
    {
        rsm_log(ctx, "cannot bring key value store %" PRIu64
                     " online because the server is "
                     "%s\n", id.get(), kvs_state::to_string(kv->state));
        return generate_response(ctx, COORD_NO_CAN_DO);
    }

    bool changed = false;

    if (kv->state != kvs_state::ONLINE)
    {
        rsm_log(ctx, "changing key value store %" PRIu64
                     " from %s to %s\n",
                     id.get(), kvs_state::to_string(kv->state),
                     kvs_state::to_string(kvs_state::ONLINE));
        kv->state = kvs_state::ONLINE;
        kv->nonce = nonce;
        changed = true;
        kvs_availability_changed();
    }
    else if (kv->nonce != nonce)
    {
        rsm_log(ctx, "changing key value store %" PRIu64
                     "'s nonce from %" PRIu64 " to %" PRIu64 "\n",
                     id.get(), kv->nonce, nonce);
        kv->nonce = nonce;
        changed = true;
    }

    if (kv->kv.bind_to != bind_to)
    {
        std::string from(to_string(kv->kv.bind_to));
        std::string to(to_string(bind_to));

        if (address_is_free(m_kvss, id, bind_to))
        {
            rsm_log(ctx, "changing key value store %" PRIu64 "'s address from %s to %s\n",
                         id.get(), from.c_str(), to.c_str());
            kv->kv.bind_to = bind_to;
            changed = true;
        }
        else
        {
            rsm_log(ctx, "cannot change key value store %" PRIu64 " to %s "
                         "because that address is in use\n", id.get(), to.c_str());
            return generate_response(ctx, consus::COORD_DUPLICATE);
        }
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: kvs_offline(rsm_context* ctx, comm_id id, const po6::net::location&, uint64_t nonce)
{
    kvs_state* kv = get_kvs(id);

    if (!kv)
    {
        rsm_log(ctx, "cannot bring key value store %" PRIu64
                     " offline because it is not registered", id.get());
        return generate_response(ctx, COORD_NOT_FOUND);
    }

    if (kv->nonce == nonce && kv->state == kvs_state::ONLINE)
    {
        rsm_log(ctx, "changing key value store %" PRIu64 " from %s to %s\n",
                     id.get(), kvs_state::to_string(kv->state),
                     kvs_state::to_string(kvs_state::OFFLINE));
        kv->state = kvs_state::OFFLINE;
        kvs_availability_changed();
        generate_next_configuration(ctx);
    }

    return generate_response(ctx, COORD_SUCCESS);
}

void
coordinator :: tick(rsm_context* ctx)
{
    ++m_txman_quiescence_counter;

    if (m_txman_quiescence_counter > 5 && m_txmans_changed)
    {
        rsm_log(ctx, "regenerating paxos groups because of recent changes to transaction manager availability");
        regenerate_paxos_groups(ctx);
        generate_next_configuration(ctx);
        m_txmans_changed = false;
    }
    else if (m_txmans_changed)
    {
        rsm_log(ctx, "delaying regeneration of paxos groups because transaction managers' availability has not quiesced");
    }

    ++m_kvs_quiescence_counter;

    if (m_kvs_quiescence_counter > 5 && m_kvss_changed)
    {
        rsm_log(ctx, "restructuring rings because of recent changes to key-value store availability");
        // XXX
        m_kvss_changed = false;
    }
    else if (m_kvss_changed)
    {
        rsm_log(ctx, "delaying ring restructuring because key-value stores' availability has not quiesced");
    }
}

coordinator*
coordinator :: recreate(rsm_context* ctx,
                        const char* data, size_t data_sz)
{
    // XXX
    std::auto_ptr<coordinator> c(new coordinator());

    if (!c.get())
    {
        return NULL;
    }

    e::unpacker(data, data_sz)
        >> c->m_cluster >> c->m_version
        >> c->m_flags >> c->m_counter
        >> c->m_txmans;
    c->generate_next_configuration(ctx);
    return c.release();
}

int
coordinator :: snapshot(rsm_context* /*ctx*/,
                        char** data, size_t* data_sz)
{
    // XXX
    std::string buf;
    e::packer(&buf)
        << m_cluster << m_version
        << m_flags << m_counter
        << m_txmans;
    char* ptr = static_cast<char*>(malloc(buf.size()));
    *data = ptr;
    *data_sz = buf.size();

    if (*data)
    {
        memmove(ptr, buf.data(), buf.size());
    }

    return 0;
}

void
coordinator :: generate_next_configuration(rsm_context* ctx)
{
    m_version = version_id(m_version.get() + 1);
    rsm_log(ctx, "issuing new configuration version %" PRIu64 "\n", m_version);
    std::vector<txman> txmans;

    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].state == txman_state::ONLINE)
        {
            txmans.push_back(m_txmans[i].tx);
        }
    }

    std::vector<kvs> kvss;

    for (size_t i = 0; i < m_kvss.size(); ++i)
    {
        if (m_kvss[i].state == kvs_state::ONLINE)
        {
            kvss.push_back(m_kvss[i].kv);
        }
    }

    // client configuration
    std::string clientconf;
    e::packer(&clientconf) << m_cluster << m_version << m_flags << txmans;
    rsm_cond_broadcast_data(ctx, "clientconf", clientconf.data(), clientconf.size());

    // txman configuration
    std::string txmanconf;
    e::packer(&txmanconf)
        << m_cluster << m_version << m_flags
        << m_dcs << m_txmans << m_txman_groups << kvss;
    rsm_cond_broadcast_data(ctx, "txmanconf", txmanconf.data(), txmanconf.size());

    // kvs configuration
    std::string kvsconf;
    e::packer(&kvsconf)
        << m_cluster << m_version << m_flags << m_kvss;
    rsm_cond_broadcast_data(ctx, "kvsconf", kvsconf.data(), kvsconf.size());
}

void
coordinator :: txman_availability_changed()
{
    m_txman_quiescence_counter = 0;
    m_txmans_changed = true;
}

void
coordinator :: kvs_availability_changed()
{
    m_kvs_quiescence_counter = 0;
    m_kvss_changed = true;
}

namespace
{
using namespace consus;

void
update_scatters_and_pairs(const paxos_group& g,
                          std::map<comm_id, unsigned>* scatters,
                          std::set<std::pair<comm_id, comm_id> >* pairs)
{
    for (size_t i = 0; i < g.members_sz; ++i)
    {
        (*scatters)[g.members[i]] += g.members_sz - 1;

        for (size_t j = 0; j < g.members_sz; ++j)
        {
            pairs->insert(std::make_pair(g.members[i], g.members[j]));
            pairs->insert(std::make_pair(g.members[j], g.members[i]));
        }
    }
}

void
sort_nodes_by_width(const std::vector<txman_state>& txmans,
                    std::map<comm_id, unsigned>* scatters,
                    std::vector<comm_id>* sorted)
{
    std::vector<std::pair<unsigned, comm_id> > widths;

    for (size_t i = 0; i < txmans.size(); ++i)
    {
        if (txmans[i].state != txman_state::ONLINE)
        {
            continue;
        }

        comm_id id(txmans[i].tx.id);
        widths.push_back(std::make_pair((*scatters)[id], id));
    }

    std::stable_sort(widths.begin(), widths.end());
    sorted->clear();

    for (size_t i = 0; i < widths.size(); ++i)
    {
        sorted->push_back(widths[i].second);
    }
}

} // namespace

void
coordinator :: regenerate_paxos_groups(rsm_context*)
{
    const unsigned SCATTER = 3;
    const unsigned REPLICATION = 3;
    std::map<comm_id, unsigned> scatters;
    std::set<std::pair<comm_id, comm_id> > pairs;

    for (size_t i = 0; i < m_txman_groups.size(); )
    {
        bool remove = false;

        for (size_t j = 0; j < m_txman_groups[i].members_sz; ++j)
        {
            txman_state* ts = get_txman(m_txman_groups[i].members[j]);

            if (!ts || ts->state != txman_state::ONLINE)
            {
                remove = true;
            }
        }

        if (remove)
        {
            std::swap(m_txman_groups[i], m_txman_groups.back());
            m_txman_groups.pop_back();
        }
        else
        {
            update_scatters_and_pairs(m_txman_groups[i], &scatters, &pairs);
            ++i;
        }
    }

    size_t txmans_sz = m_txman_groups.size() + 1;

    while (m_txman_groups.size() != txmans_sz)
    {
        txmans_sz = m_txman_groups.size();

        for (size_t node = 0; node < m_txmans.size(); ++node)
        {
            const txman_state& ts(m_txmans[node]);

            if (ts.state != txman_state::ONLINE || scatters[ts.tx.id] >= SCATTER)
            {
                continue;
            }

            paxos_group g;
            g.dc = ts.tx.dc;
            g.members_sz = 1;
            g.members[0] = ts.tx.id;
            std::vector<comm_id> candidates;
            sort_nodes_by_width(m_txmans, &scatters, &candidates);

            for (size_t i = 0; i < candidates.size(); ++i)
            {
                if (ts.tx.id == candidates[i])
                {
                    continue;
                }

                bool valid = true;
                comm_id a = candidates[i];
                txman_state* ats = get_txman(a);

                for (unsigned p = 0; valid && p < g.members_sz; ++p)
                {
                    comm_id b = g.members[p];
                    assert(a != b);
                    txman_state* bts = get_txman(b);

                    // nodes must not already be in a replica set together
                    if (pairs.find(std::make_pair(a, b)) != pairs.end())
                    {
                        valid = false;
                    }

                    // nodes must be in the same data center
                    if (ats->tx.dc != bts->tx.dc)
                    {
                        valid = false;
                    }

                    // additional constraints should set valid=false if not met
                }

                if (valid)
                {
                    g.members[g.members_sz] = a;
                    ++g.members_sz;
                }

                if (g.members_sz == REPLICATION)
                {
                    break;
                }
            }

            if (g.members_sz == 1 &&
                pairs.find(std::make_pair(ts.tx.id, ts.tx.id)) != pairs.end())
            {
                continue;
            }

            g.id = paxos_group_id(m_counter);
            ++m_counter;
            update_scatters_and_pairs(g, &scatters, &pairs);
            m_txman_groups.push_back(g);
        }
    }
}

#if 0
namespace
{
using namespace consus;

void
select_by_data_center(const std::vector<kvs_state>& kvss,
                      data_center_id id,
                      std::vector<comm_id>* ids)
{
    for (size_t i = 0; i < kvss.size(); ++i)
    {
        if (kvss[i].kv.dc == id)
        {
            ids->push_back(kvss[i].kv.id);
        }
    }
}

void
expand_to_constant(std::vector<comm_id>& ids,
                   comm_id* out, size_t out_sz)
{
    if (ids.size() >= out_sz)
    {
        std::copy(ids.begin(), ids.begin() + out_sz, out);
        return;
    }

    const size_t per = out_sz / ids.size();
    const size_t lim = out_sz % ids.size();
    size_t start = 0;

    for (size_t i = 0; i < ids.size(); ++i)
    {
        const size_t limit = start + per + (i < lim ? 1 : 0);

        for (size_t j = start; j < limit; ++j)
        {
            out[j] = ids[i];
        }

        start = limit;
    }
}

struct assignment
{
    assignment() : p(), t() {}
    assignment(unsigned _p, comm_id _t) : p(_p), t(_t) {}

    bool operator < (const assignment& rhs) { return p < rhs.p; }

    unsigned p;
    comm_id t;
};

struct reassignment
{
    reassignment() : p(), f(), t() {}
    reassignment(unsigned _p, comm_id _f, comm_id _t) : p(_p), f(_f), t(_t) {}

    bool operator < (const reassignment& rhs) { return p < rhs.p; }

    unsigned p;
    comm_id f;
    comm_id t;
};

void
log_assignments(rsm_context* ctx, const std::string& name, std::vector<assignment>* assignments)
{
    std::sort(assignments->begin(), assignments->end());
    assignment* ptr = &(*assignments)[0];
    assignment* const end = ptr + assignments->size();

    while (ptr < end)
    {
        assignment* eor = ptr;

        while (eor < end &&
               ptr->t == eor->t &&
               eor->p - ptr->p == eor - ptr)
        {
            ++eor;
        }

        if (ptr + 1 == eor)
        {
            rsm_log(ctx, "assigning partition %u to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         ptr->p, ptr->t.get(), e::strescape(name).c_str());
        }
        else
        {
            assert(ptr != eor);
            rsm_log(ctx, "assigning partitions %u-%u to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         ptr->p, (eor - 1)->p, ptr->t.get(), e::strescape(name).c_str());
        }

        ptr = eor;
    }
}

void
log_reassignments(rsm_context* ctx, const std::string& name, std::vector<reassignment>* reassignments)
{
    std::sort(reassignments->begin(), reassignments->end());
    reassignment* ptr = &(*reassignments)[0];
    reassignment* const end = ptr + reassignments->size();

    while (ptr < end)
    {
        reassignment* eor = ptr;

        while (eor < end &&
               ptr->f == eor->f &&
               ptr->t == eor->t &&
               eor->p - ptr->p == eor - ptr)
        {
            ++eor;
        }

        if (ptr + 1 == eor)
        {
            rsm_log(ctx, "reassigning partition %u from kvs(%" PRIu64 ") to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         ptr->p, ptr->f.get(), ptr->t.get(), e::strescape(name).c_str());
        }
        else
        {
            assert(ptr != eor);
            rsm_log(ctx, "reassigning partitions %u-%u from kvs(%" PRIu64 ") to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         ptr->p, (eor - 1)->p, ptr->f.get(), ptr->t.get(), e::strescape(name).c_str());
        }

        ptr = eor;
    }
}

} // namespace

void
coordinator :: maintain_kvs_rings(rsm_context* ctx)
{
    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        std::vector<comm_id> kvss;
        select_by_data_center(m_kvss, m_dcs[i].id, &kvss);
        comm_id partitions[CONSUS_KVS_PARTITIONS];
        expand_to_constant(kvss, partitions, CONSUS_KVS_PARTITIONS);
        ring* r = get_or_create_ring(m_dcs[i].id);
        std::vector<assignment> assignments;
        std::vector<reassignment> reassignments;

        for (size_t p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
        {
            if (r->partitions[p].assigned == comm_id())
            {
                assignments.push_back(assignment(p, partitions[p]));
                r->partitions[p].assigned = partitions[p];
                ++r->partitions[p].version; 
            }
            else if (r->partitions[p].assigned != partitions[p])
            {
                reassignments.push_back(reassignment(p, r->partitions[p].assigned, partitions[p]));
            }
        }

        log_assignments(ctx, m_dcs[i].name, &assignments);
        log_reassignments(ctx, m_dcs[i].name, &reassignments);

#if 0
#if 0
                ring_changed = true;

#endif
#if 0
                ring_changed = true;

                rsm_log(ctx, "assigning partition(%u) to kvs(%" PRIu64 ") in data center \"%s\"\n",
                             p, partitions[p].get(), e::strescape(m_dcs[i].name).c_str());
                rsm_log(ctx, "assigning kvs(%" PRIu64 ") to partition(%u) in data center \"%s\"\n",
                             partitions[p].get(), p, e::strescape(m_dcs[i].name).c_str());
                rsm_log(ctx, "re-assigning partition(%u) from kvs(%" PRIu64 " to kvs(%" PRIu64 ") in data center \"%s\"\n",
                             p, r->partitions[p].assigned.get(), partitions[p].get(), e::strescape(m_dcs[i].name).c_str());
#endif
        if (ring_changed)
        {
            ++r->version;
        }
#endif
    }
}
#endif
