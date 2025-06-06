#!/bin/bash

LOCK_FILE_DIR="/etc/ha_controller/locks"

if ! [[ -e "$LOCK_FILE_DIR"/active.lock ]]; then
    rm --force "$LOCK_FILE_DIR"/remote_pg.status
    rm --force "$LOCK_FILE_DIR"/script_in_progress.lock
    echo "Cleand pg ha controller locks after restart"
    exit 0
else
    echo "<3>Failed to clean pg ha controller locks after restart. Cause: active.lock is present."
    exit 1
fi
