#!/bin/bash

PG_ISREADY="/opt/pgpro/ent-16/bin/pg_isready"
PSQL="/opt/pgpro/ent-16/bin/psql"

if [[ -e /etc/keepalived/no_vip.block ]]; then
    exit 1
fi

if ! "$PG_ISREADY" -q -h localhost -p 5432 -U postgres; then
    exit 1
fi

is_slave=$("$PSQL" -h localhost -p 5432 -U postgres -tAc "SELECT pg_is_in_recovery();")
if [[ "$is_slave" = "t" ]]; then
    exit 1
else
    exit 0
fi
