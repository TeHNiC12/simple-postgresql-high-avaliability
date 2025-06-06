#!/bin/bash

if [[ -e /etc/keepalived/no_vip.block ]]; then
    exit 1
fi

/usr/lib/postgresql/11/bin/pg_isready -q -h localhost -p 5432 -U postgres
if [ $? -ne 0 ]; then
    exit 1
fi

is_slave=$(/usr/lib/postgresql/11/bin/psql -h localhost -p 5432 -U postgres -tAc "SELECT pg_is_in_recovery();")
if [ "$is_slave" = "t" ]; then
    exit 1
else
    exit 0
fi
