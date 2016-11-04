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
    , m_rings()
    , m_migrated()
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
    //XXX INVARIANT(m_dc_default == data_center_id() || get_data_center(m_dc_default));

    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        INVARIANT(m_dcs[i].id.get() < m_counter);
    }

    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        //XXX INVARIANT(get_data_center(m_txmans[i].tx.dc));
    }

    for (size_t i = 0; i < m_txman_groups.size(); ++i)
    {
        INVARIANT(m_txman_groups[i].id.get() < m_counter);
        //XXX INVARIANT(get_data_center(m_txman_groups[i].dc));

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
        //XXX INVARIANT(get_data_center(m_kvss[i].kv.dc));
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
    rsm_log(ctx, "created data center %s", e::strescape(dc->name).c_str());
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
coordinator :: txman_register(rsm_context* ctx, const txman& t, const std::string& dc_name)
{
    txman_state* ts = get_txman(t.id);

    if (ts)
    {
        rsm_log(ctx, "register %s failed: already registered", to_string(t).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    data_center_id dcid = m_dc_default;

    if (dc_name != "")
    {
        data_center* dc = get_data_center(dc_name);

        if (!dc)
        {
            rsm_log(ctx, "register %s failed: data center %s doesn't exist", to_string(t).c_str(), dc_name.c_str());
            return generate_response(ctx, consus::COORD_NOT_FOUND);
        }

        dcid = dc->id;
    }

    ts = new_txman(t);
    ts->tx.dc = dcid;
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

void
coordinator :: kvs_register(rsm_context* ctx, const kvs& k, const std::string& dc_name)
{
    kvs_state* kv = get_kvs(k.id);

    if (kv)
    {
        rsm_log(ctx, "register %s failed: already registered", to_string(k).c_str());
        return generate_response(ctx, consus::COORD_DUPLICATE);
    }

    data_center_id dcid = m_dc_default;

    if (dc_name != "")
    {
        data_center* dc = get_data_center(dc_name);

        if (!dc)
        {
            rsm_log(ctx, "register %s failed: data center %s doesn't exist", to_string(k).c_str(), dc_name.c_str());
            return generate_response(ctx, consus::COORD_NOT_FOUND);
        }

        dcid = dc->id;
    }

    kv = new_kvs(k);
    kv->kv.dc = dcid;
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
coordinator :: kvs_migrated(rsm_context*, partition_id id)
{
    m_migrated.push_back(id);
}

void
coordinator :: is_stable(rsm_context* ctx)
{
    if (m_txmans_changed || m_kvss_changed)
    {
        return generate_response(ctx, COORD_NO_CAN_DO);
    }
    else
    {
        return generate_response(ctx, COORD_SUCCESS);
    }
}

void
coordinator :: tick(rsm_context* ctx)
{
    const unsigned TXMAN_TICK_LIMIT = 5;
    const unsigned KVS_TICK_LIMIT = 5;
    ++m_txman_quiescence_counter;
    bool changed = false;

    if (m_txman_quiescence_counter >= TXMAN_TICK_LIMIT && m_txmans_changed)
    {
        rsm_log(ctx, "regenerating paxos groups because of recent changes to transaction manager availability");
        regenerate_paxos_groups(ctx);
        m_txmans_changed = false;
        changed = true;
    }
    else if (m_txmans_changed)
    {
        rsm_log(ctx, "delaying regeneration of paxos groups because transaction managers' availability has not quiesced (regenerating in %d ticks)", TXMAN_TICK_LIMIT - m_txman_quiescence_counter);
    }

    if (!m_migrated.empty())
    {
        if (finish_migrations(ctx))
        {
            changed = true;
        }
    }

    ++m_kvs_quiescence_counter;

    if (m_kvs_quiescence_counter >= KVS_TICK_LIMIT && m_kvss_changed)
    {
        rsm_log(ctx, "restructuring rings because of recent changes to key-value store availability");
        maintain_kvs_rings(ctx);
        m_kvss_changed = false;
        changed = true;
    }
    else if (m_kvss_changed)
    {
        rsm_log(ctx, "delaying ring restructuring because key-value stores' availability has not quiesced (regenerating in %d ticks)", KVS_TICK_LIMIT - m_kvs_quiescence_counter);
    }

    if (changed)
    {
        generate_next_configuration(ctx);
    }
}

coordinator*
coordinator :: recreate(rsm_context* /*ctx*/,
                        const char* data, size_t data_sz)
{
    std::auto_ptr<coordinator> c(new coordinator());

    if (!c.get())
    {
        return NULL;
    }

    e::unpacker up(data, data_sz);
    up = up >> c->m_cluster >> c->m_version
            >> c->m_flags >> c->m_counter
            >> c->m_dc_default >> c->m_dcs
            >> c->m_txmans
            >> c->m_txman_groups
            >> c->m_txman_quiescence_counter
            >> e::unpack_uint8<bool>(c->m_txmans_changed)
            >> c->m_kvss
            >> c->m_kvs_quiescence_counter
            >> e::unpack_uint8<bool>(c->m_kvss_changed)
            >> c->m_rings
            >> c->m_migrated;

    if (up.error())
    {
        return NULL;
    }

    return c.release();
}

int
coordinator :: snapshot(rsm_context* /*ctx*/,
                        char** data, size_t* data_sz)
{
    std::string buf;
    e::packer(&buf)
        << m_cluster << m_version
        << m_flags << m_counter
        << m_dc_default << m_dcs
        << m_txmans
        << m_txman_groups
        << m_txman_quiescence_counter
        << e::pack_uint8<bool>(m_txmans_changed)
        << m_kvss
        << m_kvs_quiescence_counter
        << e::pack_uint8<bool>(m_kvss_changed)
        << m_rings
        << m_migrated;
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
    rsm_log(ctx, "issuing new configuration version %" PRIu64 "\n", m_version.get());
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
        << m_cluster << m_version << m_flags << m_kvss << m_rings;
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

namespace
{
using namespace consus;

void
select_active_by_data_center(const std::vector<kvs_state>& kvss,
                             data_center_id id,
                             std::vector<comm_id>* ids)
{
    for (size_t i = 0; i < kvss.size(); ++i)
    {
        if (kvss[i].kv.dc == id &&
            kvss[i].state > kvs_state::REGISTERED &&
            kvss[i].state < kvs_state::RETIRING)
        {
            ids->push_back(kvss[i].kv.id);
        }
    }
}

template <typename T>
unsigned
compute_partition_count(T parts[CONSUS_KVS_PARTITIONS])
{
    unsigned idx = 0;
    unsigned partitions = 0;

    while (idx < CONSUS_KVS_PARTITIONS)
    {
        unsigned end = idx + 1;

        while (end < CONSUS_KVS_PARTITIONS &&
               parts[idx] == parts[end])
        {
            ++end;
        }

        ++partitions;
        idx = end;
    }

    return partitions;
}

void
map_owners_to_indices(comm_id owners[CONSUS_KVS_PARTITIONS],
                      unsigned partitions[CONSUS_KVS_PARTITIONS])
{
    unsigned idx = 0;
    unsigned count = 0;

    while (idx < CONSUS_KVS_PARTITIONS)
    {
        unsigned end = idx;

        while (end < CONSUS_KVS_PARTITIONS &&
               owners[idx] == owners[end])
        {
            partitions[end] = count;
            ++end;
        }

        ++count;
        idx = end;
    }
}

void
evenly_distribute_indices(unsigned count,
                          unsigned partitions[CONSUS_KVS_PARTITIONS])
{
    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        partitions[i] = 0;
    }

    const unsigned size = CONSUS_KVS_PARTITIONS / count;
    const unsigned excess = CONSUS_KVS_PARTITIONS % count;
    unsigned idx = 0;

    for (size_t i = 0; i < count; ++i)
    {
        const unsigned this_size = size + (i < excess ? 1 : 0);

        for (size_t j = 0; j < this_size; ++j)
        {
            partitions[idx + j] = i;
        }

        idx += this_size;
    }

    assert(partitions[CONSUS_KVS_PARTITIONS - 1] == count - 1);
}

void
stable_marriage(comm_id current_owners[CONSUS_KVS_PARTITIONS],
                comm_id new_owners[CONSUS_KVS_PARTITIONS],
                unsigned new_partitions)
{
    // map CONSUS_KVS_PARTITIONS buckets onto a smaller number of buckets (the
    // value of each element in the arrays)
    unsigned part1[CONSUS_KVS_PARTITIONS];
    unsigned part2[CONSUS_KVS_PARTITIONS];
    // label each contiguous block of owners with a unique id
    map_owners_to_indices(current_owners, part1);
    // create an ideal distribution of N partitions
    evenly_distribute_indices(new_partitions, part2);
    const unsigned n1 = compute_partition_count(part1);
    const unsigned n2 = new_partitions;
    assert(n2 == compute_partition_count(part2));
    // preference weights for the stable marriage algorithm
    // prefs1: part1/current for part2/new
    // prefs2: part2/new for part1/current
    std::map<std::pair<unsigned, unsigned>, unsigned> prefs1;
    std::map<std::pair<unsigned, unsigned>, unsigned> prefs2;

    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        ++prefs1[std::make_pair(part1[i], part2[i])];
        ++prefs2[std::make_pair(part2[i], part1[i])];
    }

    // who is assigned to whom; matches part1/part2
    // assign1: values [0:n1] value CONSUS_KVS_PARTITIONS means unassigned
    // assign2: values [0:n2] value CONSUS_KVS_PARTITIONS means unassigned
    // it's sized CONSUS_KVS_PARTITIONS for static allocation
    unsigned assign1[CONSUS_KVS_PARTITIONS];
    unsigned assign2[CONSUS_KVS_PARTITIONS];

    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        assign1[i] = CONSUS_KVS_PARTITIONS;
        assign2[i] = CONSUS_KVS_PARTITIONS;
    }

    std::set<std::pair<unsigned, unsigned> > proposals;

    while (true)
    {
        // if either assign1 or assign2 is completely mapped
        if (std::find(assign1, assign1 + n1, CONSUS_KVS_PARTITIONS) == assign1 + n1 ||
            std::find(assign2, assign2 + n2, CONSUS_KVS_PARTITIONS) == assign2 + n2)
        {
            break;
        }

        // consider everyone in the first group
        for (unsigned idx1 = 0; idx1 < n1; ++idx1)
        {
            // if already matched
            if (assign1[idx1] < CONSUS_KVS_PARTITIONS)
            {
                continue;
            }

            unsigned best_candidate = CONSUS_KVS_PARTITIONS;
            unsigned best_preference = 0;

            // consider everyone in the second group
            for (unsigned idx2 = 0; idx2 < n2; ++idx2)
            {
                // if there's already a proposal from 1->2
                if (proposals.find(std::make_pair(idx1, idx2)) != proposals.end())
                {
                    continue;
                }

                std::map<std::pair<unsigned, unsigned>, unsigned>::iterator it;
                it = prefs1.find(std::make_pair(idx1, idx2));
                // what's idx1's preference for idx2?
                const unsigned preference = it != prefs1.end() ? it->second : 0;

                // pick
                if (best_candidate == CONSUS_KVS_PARTITIONS ||
                    best_preference < preference)
                {
                    best_candidate = idx2;
                    best_preference = preference;
                }
            }

            // if no one to propose to
            if (best_candidate == CONSUS_KVS_PARTITIONS)
            {
                continue;
            }

            proposals.insert(std::make_pair(idx1, best_candidate));

            // if idx1's preference is unmatched, match them
            if (assign2[best_candidate] == CONSUS_KVS_PARTITIONS)
            {
                assign1[idx1] = best_candidate;
                assign2[best_candidate] = idx1;
            }
            else
            {
                std::map<std::pair<unsigned, unsigned>, unsigned>::iterator it;
                it = prefs2.find(std::make_pair(best_candidate, assign2[best_candidate]));
                // candidate's preference for its current match
                const unsigned cur_preference = it != prefs2.end() ? it->second : 0;
                it = prefs2.find(std::make_pair(best_candidate, idx1));
                // candidate's preference for idx1
                const unsigned new_preference = it != prefs2.end() ? it->second : 0;

                // trade up
                if (cur_preference < new_preference)
                {
                    assign1[assign2[best_candidate]] = CONSUS_KVS_PARTITIONS;
                    assign1[idx1] = best_candidate;
                    assign2[best_candidate] = idx1;
                }
            }
        }
    }

    // fill in new owners
    for (size_t i = 0; i < CONSUS_KVS_PARTITIONS; ++i)
    {
        assert(part2[i] < n2);

        // if this bucket is unassigned
        if (assign2[part2[i]] == CONSUS_KVS_PARTITIONS)
        {
            new_owners[i] = comm_id();
            continue;
        }

        // find an element in part1 with the value this partition matches
        assert(assign2[part2[i]] < n1);
        unsigned* const ptr = std::find(part1, part1 + CONSUS_KVS_PARTITIONS, assign2[part2[i]]);
        const unsigned idx = ptr - part1;
        assert(idx < CONSUS_KVS_PARTITIONS);
        new_owners[i] = current_owners[idx];
    }
}

void
unassign_discontinuous(comm_id old_owners[CONSUS_KVS_PARTITIONS],
                       comm_id new_owners[CONSUS_KVS_PARTITIONS])
{
    std::map<comm_id, unsigned> counts;
    comm_id* ptr = new_owners;
    comm_id* const end = ptr + CONSUS_KVS_PARTITIONS;

    while (ptr < end)
    {
        // find the contiguous size of this assignment
        comm_id* tmp = ptr;
        // how many partitions are the same in old and new
        unsigned maintained = 0;

        while (tmp < end && *tmp == *ptr)
        {
            if (old_owners[tmp - new_owners] == *ptr)
            {
                ++maintained;
            }

            ++tmp;
        }

        // look for any previous range assigned to *ptr
        std::map<comm_id, unsigned>::iterator it = counts.find(*ptr);

        // if *ptr was previously assigned and previous assignment maintained
        // fewer partitions
        if (it != counts.end() && it->second < maintained)
        {
            for (comm_id* c = new_owners; c < ptr; ++c)
            {
                if (*c == *ptr)
                {
                    *c = comm_id();
                }
            }

            it->second = maintained;
        }
        else if (it == counts.end())
        {
            counts.insert(std::make_pair(*ptr, maintained));
        }

        ptr = tmp;
    }
}

void
assign_unassigned(comm_id owners[CONSUS_KVS_PARTITIONS], comm_id* kvs, size_t kvs_sz)
{
    comm_id* const owners_end = owners + CONSUS_KVS_PARTITIONS;
    unsigned part[CONSUS_KVS_PARTITIONS];
    evenly_distribute_indices(kvs_sz, part);

    for (size_t i = 0; i < kvs_sz; ++i)
    {
        if (std::find(owners, owners_end, kvs[i]) != owners_end)
        {
            continue;
        }

        unsigned fill = CONSUS_KVS_PARTITIONS;

        for (size_t j = 0; j < CONSUS_KVS_PARTITIONS; ++j)
        {
            if (owners[j] == comm_id() && fill == CONSUS_KVS_PARTITIONS)
            {
                fill = part[j];
            }

            if (part[j] == fill && owners[j] == comm_id())
            {
                owners[j] = kvs[i];
            }
        }
    }
}

struct assignment
{
    assignment() : p(), t() {}
    assignment(unsigned _p, comm_id _t) : p(_p), t(_t) {}

    bool operator < (const assignment& rhs) const { return p < rhs.p; }

    unsigned p;
    comm_id t;
};

struct reassignment
{
    reassignment() : p(), f(), t() {}
    reassignment(unsigned _p, comm_id _f, comm_id _t) : p(_p), f(_f), t(_t) {}

    bool operator < (const reassignment& rhs) const { return p < rhs.p; }

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
log_reassignments(rsm_context* ctx, const char* verb, const std::string& name, std::vector<reassignment>* reassignments)
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
            rsm_log(ctx, "%s partition %u from kvs(%" PRIu64 ") to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         verb, ptr->p, ptr->f.get(), ptr->t.get(), e::strescape(name).c_str());
        }
        else
        {
            assert(ptr != eor);
            rsm_log(ctx, "%s partitions %u-%u from kvs(%" PRIu64 ") to kvs(%" PRIu64 ") in data center \"%s\"\n",
                         verb, ptr->p, (eor - 1)->p, ptr->f.get(), ptr->t.get(), e::strescape(name).c_str());
        }

        ptr = eor;
    }
}

} // namespace

ring*
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

void
coordinator :: maintain_kvs_rings(rsm_context* ctx)
{
    // XXX ensure this algorithm will never create a cycle of migrations, where
    // vertices are servers and directed edges are src->dst migrations

    for (size_t i = 0; i < m_dcs.size(); ++i)
    {
        ring* r = get_or_create_ring(m_dcs[i].id);
        std::vector<comm_id> kvss;
        select_active_by_data_center(m_kvss, m_dcs[i].id, &kvss);

        if (kvss.empty())
        {
            continue;
        }

        comm_id current_owners[CONSUS_KVS_PARTITIONS];
        r->get_owners(current_owners);
        comm_id new_owners[CONSUS_KVS_PARTITIONS];
        stable_marriage(current_owners, new_owners, kvss.size());
        unassign_discontinuous(current_owners, new_owners);
        assign_unassigned(new_owners, &kvss[0], kvss.size());
        r->set_owners(new_owners, &m_counter);
        std::vector<assignment> assignments;
        std::vector<reassignment> reassignments;

        for (unsigned p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
        {
            if (new_owners[p] == comm_id())
            {
                continue;
            }

            if (current_owners[p] == comm_id())
            {
                assignments.push_back(assignment(p, new_owners[p]));
            }
            else if (current_owners[p] != new_owners[p])
            {
                reassignments.push_back(reassignment(p, current_owners[p], new_owners[p]));
            }
        }

        log_assignments(ctx, m_dcs[i].name, &assignments);
        log_reassignments(ctx, "reassigning", m_dcs[i].name, &reassignments);
    }
}

bool
coordinator :: finish_migrations(rsm_context* ctx)
{
    std::sort(m_migrated.begin(), m_migrated.end());
    std::vector<partition_id>::iterator pit;
    pit = std::unique(m_migrated.begin(), m_migrated.end());
    m_migrated.resize(pit - m_migrated.begin());
    bool ret = false;

    for (size_t i = 0; i < m_rings.size(); ++i)
    {
        std::vector<reassignment> reassignments;

        for (unsigned p = 0; p < CONSUS_KVS_PARTITIONS; ++p)
        {
            partition* part = &m_rings[i].partitions[p];

            if (part->next_id != partition_id() &&
                std::binary_search(m_migrated.begin(),
                                   m_migrated.end(),
                                   part->next_id))
            {
                reassignments.push_back(reassignment(p, part->owner, part->next_owner));
                part->id = part->next_id;
                part->owner = part->next_owner;
                part->next_id = partition_id();
                part->next_owner = comm_id();
            }
        }

        data_center* dc = get_data_center(m_rings[i].dc);
        assert(dc);
        log_reassignments(ctx, "migrated", dc->name, &reassignments);
        ret = ret || !reassignments.empty();
    }

    m_migrated.clear();
    return ret;
}
