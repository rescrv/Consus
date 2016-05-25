# Copyright (c) 2015, Robert Escriva
# All rights reserved.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

AM_CPPFLAGS  = -DCONSUS_EXEC_DIR="\"$(consusexecdir)\"" -I${abs_top_srcdir}/include $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(REPLICANT_CFLAGS)
AM_CFLAGS    = -fvisibility=hidden $(WANAL_CFLAGS)
AM_CXXFLAGS  = -fvisibility=hidden -fvisibility-inlines-hidden $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(REPLICANT_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
HELP2MAN_FLAGS = --no-discard-stderr --libtool --no-info --version-string=$(VERSION) --manual="Consus User Manual"
TESTS_ENVIRONMENT = . $(abs_top_srcdir)/test/env.sh "${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";

consusexecdir = $(libexecdir)/$(PACKAGE)-$(VERSION)
pkgconfigdir = $(libdir)/pkgconfig

BUILT_SOURCES =

EXTRA_DIST =

CLEANFILES =

include_HEADERS =
noinst_HEADERS =

lib_LTLIBRARIES =

bin_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =
check_SCRIPTS =

consusexec_PROGRAMS =
consusexec_LTLIBRARIES =

dist_man_MANS =

pyexec_LTLIBRARIES =

TESTS =

################################################################################
#################################### Common ####################################
################################################################################

%.h2m: %.md
	$(pandoc_verbose)pandoc -s -w man -o $@ $<
	$(sed_verbose)sed -e 's/^\.SH \(.*\)$$/[\1]/' -e 's/^.PP$$//' $@ > $@.tmp
	@mv $@.tmp $@

help2man_verbose = $(help2man_verbose_$(V))
help2man_verbose_ = $(help2man_verbose_$(AM_DEFAULT_VERBOSITY))
help2man_verbose_0 = @echo "  MAN     " $@;
pandoc_verbose = $(pandoc_verbose_$(V))
pandoc_verbose_ = $(pandoc_verbose_$(AM_DEFAULT_VERBOSITY))
pandoc_verbose_0 = @echo "  PANDOC  " $@;
pyx_verbose = $(pyx_verbose_$(V))
pyx_verbose_ = $(pyx_verbose_$(AM_DEFAULT_VERBOSITY))
pyx_verbose_0 = @echo "  PYX     " $@;
sed_verbose = $(sed_verbose_$(V))
sed_verbose_ = $(sed_verbose_$(AM_DEFAULT_VERBOSITY))
sed_verbose_0 = @echo "  SED     " $@;

th_sources = test/th_main.cc test/th.cc test/th.h

EXTRA_DIST += LICENSE

noinst_HEADERS += namespace.h
noinst_HEADERS += visibility.h
noinst_HEADERS += common/client_configuration.h
noinst_HEADERS += common/constants.h
noinst_HEADERS += common/consus.h
noinst_HEADERS += common/coordinator_link.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/crc32c.h
noinst_HEADERS += common/data_center.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += common/kvs_configuration.h
noinst_HEADERS += common/kvs.h
noinst_HEADERS += common/kvs_state.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/paxos_group.h
noinst_HEADERS += common/transaction_id.h
noinst_HEADERS += common/transaction_group.h
noinst_HEADERS += common/txman_configuration.h
noinst_HEADERS += common/txman.h
noinst_HEADERS += common/txman_state.h

################################################################################
############################## Transaction Manager #############################
################################################################################

consusexec_PROGRAMS += consus-transaction-manager
dist_man_MANS += man/consus-transaction-manager.1

noinst_HEADERS += txman/configuration.h
noinst_HEADERS += txman/daemon.h
noinst_HEADERS += txman/durable_log.h
noinst_HEADERS += txman/generalized_paxos.h
noinst_HEADERS += txman/global_voter.h
noinst_HEADERS += txman/local_voter.h
noinst_HEADERS += txman/log_entry_t.h
noinst_HEADERS += txman/mapper.h
noinst_HEADERS += txman/paxos_synod.h
noinst_HEADERS += txman/transaction.h

consus_transaction_manager_SOURCES =
consus_transaction_manager_SOURCES += common/consus.cc
consus_transaction_manager_SOURCES += common/coordinator_link.cc
consus_transaction_manager_SOURCES += common/crc32c.cc
consus_transaction_manager_SOURCES += common/data_center.cc
consus_transaction_manager_SOURCES += common/ids.cc
consus_transaction_manager_SOURCES += common/lock.cc
consus_transaction_manager_SOURCES += common/kvs.cc
consus_transaction_manager_SOURCES += common/network_msgtype.cc
consus_transaction_manager_SOURCES += common/paxos_group.cc
consus_transaction_manager_SOURCES += common/transaction_id.cc
consus_transaction_manager_SOURCES += common/transaction_group.cc
consus_transaction_manager_SOURCES += common/txman.cc
consus_transaction_manager_SOURCES += common/txman_configuration.cc
consus_transaction_manager_SOURCES += common/txman_state.cc
consus_transaction_manager_SOURCES += txman/configuration.cc
consus_transaction_manager_SOURCES += txman/daemon.cc
consus_transaction_manager_SOURCES += txman/durable_log.cc
consus_transaction_manager_SOURCES += txman/generalized_paxos.cc
consus_transaction_manager_SOURCES += txman/global_voter.cc
consus_transaction_manager_SOURCES += txman/kvs_lock_op.cc
consus_transaction_manager_SOURCES += txman/kvs_read.cc
consus_transaction_manager_SOURCES += txman/kvs_write.cc
consus_transaction_manager_SOURCES += txman/local_voter.cc
consus_transaction_manager_SOURCES += txman/log_entry_t.cc
consus_transaction_manager_SOURCES += txman/main.cc
consus_transaction_manager_SOURCES += txman/mapper.cc
consus_transaction_manager_SOURCES += txman/paxos_synod.cc
consus_transaction_manager_SOURCES += txman/transaction.cc
consus_transaction_manager_SOURCES += tools/connect_opts.cc
consus_transaction_manager_LDADD =
consus_transaction_manager_LDADD += $(REPLICANT_LIBS)
consus_transaction_manager_LDADD += $(BUSYBEE_LIBS)
consus_transaction_manager_LDADD += $(E_LIBS)
consus_transaction_manager_LDADD += $(PO6_LIBS)
consus_transaction_manager_LDADD += $(GLOG_LIBS)
consus_transaction_manager_LDADD += $(POPT_LIBS)

EXTRA_DIST += man/consus-transaction-manager.1.md
EXTRA_DIST += man/consus-transaction-manager.1.h2m
man/consus-transaction-manager.1: man/consus-transaction-manager.1.h2m txman/main.cc | consus-transaction-manager${EXEEXT}
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-transaction-manager$(EXEEXT)

################################################################################
###################################### KVS #####################################
################################################################################

consusexec_PROGRAMS += consus-key-value-store
dist_man_MANS += man/consus-key-value-store.1

noinst_HEADERS += kvs/configuration.h
noinst_HEADERS += kvs/daemon.h
noinst_HEADERS += kvs/mapper.h

consus_key_value_store_SOURCES =
consus_key_value_store_SOURCES += common/background_thread.cc
consus_key_value_store_SOURCES += common/consus.cc
consus_key_value_store_SOURCES += common/coordinator_link.cc
consus_key_value_store_SOURCES += common/ids.cc
consus_key_value_store_SOURCES += common/lock.cc
consus_key_value_store_SOURCES += common/kvs.cc
consus_key_value_store_SOURCES += common/kvs_configuration.cc
consus_key_value_store_SOURCES += common/kvs_state.cc
consus_key_value_store_SOURCES += common/network_msgtype.cc
consus_key_value_store_SOURCES += common/partition.cc
consus_key_value_store_SOURCES += common/ring.cc
consus_key_value_store_SOURCES += common/transaction_id.cc
consus_key_value_store_SOURCES += common/transaction_group.cc
consus_key_value_store_SOURCES += kvs/configuration.cc
consus_key_value_store_SOURCES += kvs/daemon.cc
consus_key_value_store_SOURCES += kvs/datalayer.cc
consus_key_value_store_SOURCES += kvs/leveldb_datalayer.cc
consus_key_value_store_SOURCES += kvs/main.cc
consus_key_value_store_SOURCES += kvs/mapper.cc
consus_key_value_store_SOURCES += kvs/migrator.cc
consus_key_value_store_SOURCES += kvs/read_replicator.cc
consus_key_value_store_SOURCES += kvs/write_replicator.cc
consus_key_value_store_SOURCES += tools/connect_opts.cc
consus_key_value_store_LDADD =
consus_key_value_store_LDADD += -lleveldb
consus_key_value_store_LDADD += $(REPLICANT_LIBS)
consus_key_value_store_LDADD += $(BUSYBEE_LIBS)
consus_key_value_store_LDADD += $(PO6_LIBS)
consus_key_value_store_LDADD += $(GLOG_LIBS)
consus_key_value_store_LDADD += $(POPT_LIBS)

EXTRA_DIST += man/consus-key-value-store.1.md
EXTRA_DIST += man/consus-key-value-store.1.h2m
man/consus-key-value-store.1: man/consus-key-value-store.1.h2m kvs/main.cc | consus-key-value-store${EXEEXT}
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-key-value-store$(EXEEXT)

################################################################################
################################## Coordinator #################################
################################################################################

consusexec_LTLIBRARIES += libconsus-coordinator.la
consusexec_PROGRAMS += consus-coordinator
dist_man_MANS += man/consus-coordinator.1

noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/transitions.h
noinst_HEADERS += coordinator/util.h

libconsus_coordinator_la_SOURCES =
libconsus_coordinator_la_SOURCES += common/data_center.cc
libconsus_coordinator_la_SOURCES += common/ids.cc
libconsus_coordinator_la_SOURCES += common/kvs.cc
libconsus_coordinator_la_SOURCES += common/kvs_state.cc
libconsus_coordinator_la_SOURCES += common/network_msgtype.cc
libconsus_coordinator_la_SOURCES += common/partition.cc
libconsus_coordinator_la_SOURCES += common/paxos_group.cc
libconsus_coordinator_la_SOURCES += common/ring.cc
libconsus_coordinator_la_SOURCES += common/txman.cc
libconsus_coordinator_la_SOURCES += common/txman_state.cc
libconsus_coordinator_la_SOURCES += coordinator/coordinator.cc
libconsus_coordinator_la_SOURCES += coordinator/symtable.c
libconsus_coordinator_la_SOURCES += coordinator/transitions.cc
libconsus_coordinator_la_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
libconsus_coordinator_la_LIBADD =
libconsus_coordinator_la_LIBADD += $(E_LIBS)

consus_coordinator_SOURCES = tools/coordinator.cc
consus_coordinator_CPPFLAGS = -DCONSUS_EXEC_DIR=\""$(consusexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
consus_coordinator_LDADD =
consus_coordinator_LDADD += $(BUSYBEE_LIBS)
consus_coordinator_LDADD += $(E_LIBS)
consus_coordinator_LDADD += $(POPT_LIBS)

EXTRA_DIST += man/consus-coordinator.1.md
EXTRA_DIST += man/consus-coordinator.1.h2m
man/consus-coordinator.1: man/consus-coordinator.1.h2m | consus-coordinator${EXEEXT}
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-coordinator$(EXEEXT)

################################################################################
#################################### Client ####################################
################################################################################

include_HEADERS += include/consus.h
include_HEADERS += include/consus-admin.h

lib_LTLIBRARIES += libconsus.la

noinst_HEADERS += client/client.h
noinst_HEADERS += client/configuration.h
noinst_HEADERS += client/consus-internal.h
noinst_HEADERS += client/mapper.h
noinst_HEADERS += client/pending_begin_transaction.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_string.h
noinst_HEADERS += client/pending_transaction_abort.h
noinst_HEADERS += client/pending_transaction_commit.h
noinst_HEADERS += client/pending_transaction_read.h
noinst_HEADERS += client/pending_transaction_write.h
noinst_HEADERS += client/server_selector.h
noinst_HEADERS += client/transaction.h

libconsus_la_SOURCES =
libconsus_la_SOURCES += common/client_configuration.cc
libconsus_la_SOURCES += common/consus.cc
libconsus_la_SOURCES += common/data_center.cc
libconsus_la_SOURCES += common/ids.cc
libconsus_la_SOURCES += common/lock.cc
libconsus_la_SOURCES += common/kvs.cc
libconsus_la_SOURCES += common/kvs_configuration.cc
libconsus_la_SOURCES += common/kvs_state.cc
libconsus_la_SOURCES += common/network_msgtype.cc
libconsus_la_SOURCES += common/partition.cc
libconsus_la_SOURCES += common/paxos_group.cc
libconsus_la_SOURCES += common/ring.cc
libconsus_la_SOURCES += common/transaction_id.cc
libconsus_la_SOURCES += common/txman.cc
libconsus_la_SOURCES += common/txman_configuration.cc
libconsus_la_SOURCES += common/txman_state.cc
libconsus_la_SOURCES += client/c.cc
libconsus_la_SOURCES += client/client.cc
libconsus_la_SOURCES += client/configuration.cc
libconsus_la_SOURCES += client/mapper.cc
libconsus_la_SOURCES += client/pending_begin_transaction.cc
libconsus_la_SOURCES += client/pending.cc
libconsus_la_SOURCES += client/pending_string.cc
libconsus_la_SOURCES += client/pending_transaction_abort.cc
libconsus_la_SOURCES += client/pending_transaction_commit.cc
libconsus_la_SOURCES += client/pending_transaction_read.cc
libconsus_la_SOURCES += client/pending_transaction_write.cc
libconsus_la_SOURCES += client/pending_unsafe_lock_op.cc
libconsus_la_SOURCES += client/pending_unsafe_read.cc
libconsus_la_SOURCES += client/pending_unsafe_write.cc
libconsus_la_SOURCES += client/server_selector.cc
libconsus_la_SOURCES += client/transaction.cc
libconsus_la_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
libconsus_la_LIBADD =
libconsus_la_LIBADD += $(REPLICANT_LIBS)
libconsus_la_LIBADD += $(BUSYBEE_LIBS)
libconsus_la_LIBADD += $(TREADSTONE_LIBS)
libconsus_la_LIBADD += $(E_LIBS)
libconsus_la_LIBADD += $(PO6_LIBS)

################################################################################
################################### Bindings ###################################
################################################################################

####################################### C ######################################

noinst_PROGRAMS += bindings/c/testcompile

bindings_c_testcompile_SOURCES = bindings/c/testcompile.c
bindings_c_testcompile_LDADD = libconsus.la $(TREADSTONE_LIBS)

#################################### Python ####################################

pyexec_LTLIBRARIES += bindings/python/consus.la

CLEANFILES += bindings/python/consus.so

EXTRA_DIST += bindings/python/consus.pyx
bindings_python_consus_la_SOURCES = bindings/python/consus.c
bindings_python_consus_la_CPPFLAGS =
bindings_python_consus_la_CPPFLAGS += $(PYTHON_CPPFLAGS)
bindings_python_consus_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_python_consus_la_CPPFLAGS += $(CPPFLAGS)
bindings_python_consus_la_CFLAGS =
bindings_python_consus_la_CFLAGS += -fvisibility=default
bindings_python_consus_la_CFLAGS += $(CFLAGS)
bindings_python_consus_la_LIBADD =
bindings_python_consus_la_LIBADD += libconsus.la
bindings_python_consus_la_LIBADD += $(PYTHON_LDFLAGS)
bindings_python_consus_la_LDFLAGS = -module -avoid-version -export-symbols-regex $(PYTHON_SYMBOL)consus $(AM_LDFLAGS) $(LDFLAGS)
bindings/python/consus.c: bindings/python/consus.pyx
	$(pyx_verbose)cython bindings/python/consus.pyx

################################################################################
##################################### Tests ####################################
################################################################################

EXTRA_DIST += test/env.sh
EXTRA_DIST += consus.supp

EXTRA_DIST += test/unit/00.client.py
EXTRA_DIST += test/unit/00.nop-abort.py
EXTRA_DIST += test/unit/00.nop-commit.py
EXTRA_DIST += test/unit/10.single-get.py
EXTRA_DIST += test/unit/10.single-put.py
EXTRA_DIST += test/unit/11.put-get-separate-commits.py

gremlins =
### begin automatically generated gremlins
gremlins += test/1-node-1-dc-cluster.gremlin
gremlins += test/2-node-1-dc-cluster.gremlin
gremlins += test/3-node-1-dc-cluster.gremlin
gremlins += test/4-node-1-dc-cluster.gremlin
gremlins += test/5-node-1-dc-cluster.gremlin
gremlins += test/1-node-1-dc-cluster.gremlin
gremlins += test/1-node-2-dc-cluster.gremlin
gremlins += test/1-node-3-dc-cluster.gremlin
gremlins += test/1-node-4-dc-cluster.gremlin
gremlins += test/1-node-5-dc-cluster.gremlin
gremlins += test/1-node-6-dc-cluster.gremlin
gremlins += test/1-node-7-dc-cluster.gremlin
gremlins += test/5-node-1-dc-cluster.gremlin
gremlins += test/5-node-2-dc-cluster.gremlin
gremlins += test/5-node-3-dc-cluster.gremlin
gremlins += test/5-node-4-dc-cluster.gremlin
gremlins += test/5-node-5-dc-cluster.gremlin
gremlins += test/5-node-6-dc-cluster.gremlin
gremlins += test/5-node-7-dc-cluster.gremlin
gremlins += test/unit/00.client.1n.1dc.gremlin
gremlins += test/unit/00.client.1n.2dc.gremlin
gremlins += test/unit/00.client.1n.3dc.gremlin
gremlins += test/unit/00.client.1n.4dc.gremlin
gremlins += test/unit/00.client.1n.5dc.gremlin
gremlins += test/unit/00.client.1n.6dc.gremlin
gremlins += test/unit/00.client.1n.7dc.gremlin
gremlins += test/unit/00.client.2n.1dc.gremlin
gremlins += test/unit/00.client.3n.1dc.gremlin
gremlins += test/unit/00.client.4n.1dc.gremlin
gremlins += test/unit/00.client.5n.1dc.gremlin
gremlins += test/unit/00.client.5n.2dc.gremlin
gremlins += test/unit/00.client.5n.3dc.gremlin
gremlins += test/unit/00.client.5n.4dc.gremlin
gremlins += test/unit/00.client.5n.5dc.gremlin
gremlins += test/unit/00.client.5n.6dc.gremlin
gremlins += test/unit/00.client.5n.7dc.gremlin
gremlins += test/unit/00.nop-abort.1n.1dc.gremlin
gremlins += test/unit/00.nop-abort.1n.2dc.gremlin
gremlins += test/unit/00.nop-abort.1n.3dc.gremlin
gremlins += test/unit/00.nop-abort.1n.4dc.gremlin
gremlins += test/unit/00.nop-abort.1n.5dc.gremlin
gremlins += test/unit/00.nop-abort.1n.6dc.gremlin
gremlins += test/unit/00.nop-abort.1n.7dc.gremlin
gremlins += test/unit/00.nop-abort.2n.1dc.gremlin
gremlins += test/unit/00.nop-abort.3n.1dc.gremlin
gremlins += test/unit/00.nop-abort.4n.1dc.gremlin
gremlins += test/unit/00.nop-abort.5n.1dc.gremlin
gremlins += test/unit/00.nop-abort.5n.2dc.gremlin
gremlins += test/unit/00.nop-abort.5n.3dc.gremlin
gremlins += test/unit/00.nop-abort.5n.4dc.gremlin
gremlins += test/unit/00.nop-abort.5n.5dc.gremlin
gremlins += test/unit/00.nop-abort.5n.6dc.gremlin
gremlins += test/unit/00.nop-abort.5n.7dc.gremlin
gremlins += test/unit/00.nop-commit.1n.1dc.gremlin
gremlins += test/unit/00.nop-commit.1n.2dc.gremlin
gremlins += test/unit/00.nop-commit.1n.3dc.gremlin
gremlins += test/unit/00.nop-commit.1n.4dc.gremlin
gremlins += test/unit/00.nop-commit.1n.5dc.gremlin
gremlins += test/unit/00.nop-commit.1n.6dc.gremlin
gremlins += test/unit/00.nop-commit.1n.7dc.gremlin
gremlins += test/unit/00.nop-commit.2n.1dc.gremlin
gremlins += test/unit/00.nop-commit.3n.1dc.gremlin
gremlins += test/unit/00.nop-commit.4n.1dc.gremlin
gremlins += test/unit/00.nop-commit.5n.1dc.gremlin
gremlins += test/unit/00.nop-commit.5n.2dc.gremlin
gremlins += test/unit/00.nop-commit.5n.3dc.gremlin
gremlins += test/unit/00.nop-commit.5n.4dc.gremlin
gremlins += test/unit/00.nop-commit.5n.5dc.gremlin
gremlins += test/unit/00.nop-commit.5n.6dc.gremlin
gremlins += test/unit/00.nop-commit.5n.7dc.gremlin
gremlins += test/unit/10.single-get.1n.1dc.gremlin
gremlins += test/unit/10.single-get.1n.2dc.gremlin
gremlins += test/unit/10.single-get.1n.3dc.gremlin
gremlins += test/unit/10.single-get.1n.4dc.gremlin
gremlins += test/unit/10.single-get.1n.5dc.gremlin
gremlins += test/unit/10.single-get.1n.6dc.gremlin
gremlins += test/unit/10.single-get.1n.7dc.gremlin
gremlins += test/unit/10.single-get.2n.1dc.gremlin
gremlins += test/unit/10.single-get.3n.1dc.gremlin
gremlins += test/unit/10.single-get.4n.1dc.gremlin
gremlins += test/unit/10.single-get.5n.1dc.gremlin
gremlins += test/unit/10.single-get.5n.2dc.gremlin
gremlins += test/unit/10.single-get.5n.3dc.gremlin
gremlins += test/unit/10.single-get.5n.4dc.gremlin
gremlins += test/unit/10.single-get.5n.5dc.gremlin
gremlins += test/unit/10.single-get.5n.6dc.gremlin
gremlins += test/unit/10.single-get.5n.7dc.gremlin
gremlins += test/unit/10.single-put.1n.1dc.gremlin
gremlins += test/unit/10.single-put.1n.2dc.gremlin
gremlins += test/unit/10.single-put.1n.3dc.gremlin
gremlins += test/unit/10.single-put.1n.4dc.gremlin
gremlins += test/unit/10.single-put.1n.5dc.gremlin
gremlins += test/unit/10.single-put.1n.6dc.gremlin
gremlins += test/unit/10.single-put.1n.7dc.gremlin
gremlins += test/unit/10.single-put.2n.1dc.gremlin
gremlins += test/unit/10.single-put.3n.1dc.gremlin
gremlins += test/unit/10.single-put.4n.1dc.gremlin
gremlins += test/unit/10.single-put.5n.1dc.gremlin
gremlins += test/unit/10.single-put.5n.2dc.gremlin
gremlins += test/unit/10.single-put.5n.3dc.gremlin
gremlins += test/unit/10.single-put.5n.4dc.gremlin
gremlins += test/unit/10.single-put.5n.5dc.gremlin
gremlins += test/unit/10.single-put.5n.6dc.gremlin
gremlins += test/unit/10.single-put.5n.7dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.1dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.2dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.3dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.4dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.5dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.6dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.1n.7dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.2n.1dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.3n.1dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.4n.1dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.1dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.2dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.3dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.4dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.5dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.6dc.gremlin
gremlins += test/unit/11.put-get-separate-commits.5n.7dc.gremlin
### end automatically generated gremlins
EXTRA_DIST += ${gremlins}
TESTS += ${gremlins}

check_PROGRAMS += test/paxos/generalized
TESTS += test/paxos/generalized
test_paxos_generalized_SOURCES = test/paxos/generalized.cc txman/generalized_paxos.cc common/ids.cc ${th_sources}
test_paxos_generalized_LDADD = ${E_LIBS}

check_PROGRAMS += test/paxos/generalized-brute-force
test_paxos_generalized_brute_force_SOURCES = test/paxos/generalized-brute-force.cc txman/generalized_paxos.cc common/ids.cc
test_paxos_generalized_brute_force_LDADD = ${E_LIBS} $(POPT_LIBS)

################################################################################
##################################### Tools ####################################
################################################################################

noinst_HEADERS += tools/common.h
noinst_HEADERS += tools/connect_opts.h
noinst_HEADERS += tools/locate-coordinator-lib.h

bin_PROGRAMS += consus
bin_PROGRAMS += consus-debug
consusexec_PROGRAMS += consus-create-data-center
consusexec_PROGRAMS += consus-set-default-data-center
consusexec_PROGRAMS += consus-availability-check
consusexec_PROGRAMS += consus-debug-client-configuration
consusexec_PROGRAMS += consus-debug-txman-configuration
consusexec_PROGRAMS += consus-debug-kvs-configuration
dist_man_MANS += man/consus.1
dist_man_MANS += man/consus-create-data-center.1
dist_man_MANS += man/consus-set-default-data-center.1
dist_man_MANS += man/consus-availability-check.1
dist_man_MANS += man/consus-debug.1
dist_man_MANS += man/consus-debug-client-configuration.1
dist_man_MANS += man/consus-debug-txman-configuration.1
dist_man_MANS += man/consus-debug-kvs-configuration.1

# consus
EXTRA_DIST += man/consus.1.md
EXTRA_DIST += man/consus.1.h2m
consus_SOURCES = consus.cc
consus_LDADD =
consus_LDADD += $(PO6_LIBS)
consus_LDADD += $(POPT_LIBS)
man/consus.1: man/consus.1.h2m consus.cc | consus$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus$(EXEEXT)

# consus-create-data-center
EXTRA_DIST += man/consus-create-data-center.1.md
EXTRA_DIST += man/consus-create-data-center.1.h2m
consus_create_data_center_SOURCES = tools/create-data-center.cc tools/common.cc tools/connect_opts.cc
consus_create_data_center_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-create-data-center.1: man/consus-create-data-center.1.h2m tools/create-data-center.cc | consus-create-data-center$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-create-data-center$(EXEEXT)

# consus-set-default-data-center
EXTRA_DIST += man/consus-set-default-data-center.1.md
EXTRA_DIST += man/consus-set-default-data-center.1.h2m
consus_set_default_data_center_SOURCES = tools/set-default-data-center.cc tools/common.cc tools/connect_opts.cc
consus_set_default_data_center_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-set-default-data-center.1: man/consus-set-default-data-center.1.h2m tools/set-default-data-center.cc | consus-set-default-data-center$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-set-default-data-center$(EXEEXT)

# consus-availability-check
EXTRA_DIST += man/consus-availability-check.1.md
EXTRA_DIST += man/consus-availability-check.1.h2m
consus_availability_check_SOURCES = tools/availability-check.cc tools/common.cc tools/connect_opts.cc
consus_availability_check_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-availability-check.1: man/consus-availability-check.1.h2m tools/availability-check.cc | consus-availability-check$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-availability-check$(EXEEXT)

# consus-debug
EXTRA_DIST += man/consus-debug.1.md
EXTRA_DIST += man/consus-debug.1.h2m
consus_debug_SOURCES = consus-debug.cc
consus_debug_LDADD =
consus_debug_LDADD += $(PO6_LIBS)
consus_debug_LDADD += $(POPT_LIBS)
man/consus-debug.1: man/consus-debug.1.h2m consus-debug.cc | consus-debug$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-debug$(EXEEXT)

# consus-debug-client-configuration
EXTRA_DIST += man/consus-debug-client-configuration.1.md
EXTRA_DIST += man/consus-debug-client-configuration.1.h2m
consus_debug_client_configuration_SOURCES = tools/debug-client-configuration.cc tools/common.cc tools/connect_opts.cc
consus_debug_client_configuration_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-debug-client-configuration.1: man/consus-debug-client-configuration.1.h2m tools/debug-client-configuration.cc | consus-debug-client-configuration$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-debug-client-configuration$(EXEEXT)

# consus-debug-txman-configuration
EXTRA_DIST += man/consus-debug-txman-configuration.1.md
EXTRA_DIST += man/consus-debug-txman-configuration.1.h2m
consus_debug_txman_configuration_SOURCES = tools/debug-txman-configuration.cc tools/common.cc tools/connect_opts.cc
consus_debug_txman_configuration_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-debug-txman-configuration.1: man/consus-debug-txman-configuration.1.h2m tools/debug-txman-configuration.cc | consus-debug-txman-configuration$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-debug-txman-configuration$(EXEEXT)

# consus-debug-kvs-configuration
EXTRA_DIST += man/consus-debug-kvs-configuration.1.md
EXTRA_DIST += man/consus-debug-kvs-configuration.1.h2m
consus_debug_kvs_configuration_SOURCES = tools/debug-kvs-configuration.cc tools/common.cc tools/connect_opts.cc
consus_debug_kvs_configuration_LDADD = libconsus.la $(TREADSTONE_LIBS) $(PO6_LIBS) $(POPT_LIBS)
man/consus-debug-kvs-configuration.1: man/consus-debug-kvs-configuration.1.h2m tools/debug-kvs-configuration.cc | consus-debug-kvs-configuration$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/consus-debug-kvs-configuration$(EXEEXT)

################################################################################
################################# Documentation ################################
################################################################################

EXTRA_DIST += $(wildcard doc/*.tex)
EXTRA_DIST += $(wildcard doc/*.xtx)
EXTRA_DIST += $(wildcard doc/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*/*/*.tex)
EXTRA_DIST += doc/Makefile
EXTRA_DIST += doc/latex.py

################################################################################
################################## Maintenance #################################
################################################################################

EXTRA_DIST += maint/generate-unit-test-gremlins
EXTRA_DIST += maint/valgrind-gremlins

# run valgrind-gremlins after all other gremlin generators
# XXX maint/valgrind-gremlins
maintainer-generate:
	maint/generate-unit-test-gremlins
