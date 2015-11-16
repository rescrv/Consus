export CONSUS_SRCDIR="$1"
export CONSUS_BUILDDIR="$2"
export CONSUS_VERSION="$3"

export CONSUS_EXEC_PATH="${CONSUS_BUILDDIR}"
export CONSUS_COORD_LIB="${CONSUS_BUILDDIR}"/.libs/libconsus-coordinator

export PATH=${CONSUS_BUILDDIR}:${CONSUS_SRCDIR}:${PATH}

export PYTHONPATH="${CONSUS_BUILDDIR}"/bindings/python:"${CONSUS_BUILDDIR}"/bindings/python/.libs:${PYTHONPATH}
