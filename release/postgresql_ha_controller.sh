#!/bin/bash

#=================================SETTINGS=================================#
#---------------------------Files & Directories----------------------------#
# script directories
LOGFILE_PATH="/etc/ha-controller/failover.log"
LOCK_FILE_DIR="/etc/ha-controller/locks"
KEEPALIVED_DIR="/etc/keepalived"
PG_LOG_ARCIVE_DIR="/etc/ha-controller/pg_log_archive"

# PostgreSQL directories
PGDATA="/pgpro-ent-16/pgdata"
PGWAL="/pgpro-ent-16/pgwal"
PGLOG="log"

# PostgreSQL binaries
PG_ISREADY="/opt/pgpro/ent-16/bin/pg_isready"
PG_CTL="/opt/pgpro/ent-16/bin/pg_ctl"
PSQL="/opt/pgpro/ent-16/bin/psql"
PG_CONTROLDATA="/opt/pgpro/ent-16/bin/pg_controldata"
PG_BASEBACKUP="/opt/pgpro/ent-16/bin/pg_basebackup"
PG_REWIND="/opt/pgpro/ent-16/bin/pg_rewind"


#-----------------------------------Users----------------------------------#
SYSTEM_ADMIN_USER="root"
SYSTEM_POSTGRES_USER="postgres"
PG_ADMIN_USER="postgres"
PG_REPLICATION_USER="replicator"

#----------------------------Cluster information---------------------------#
CLUSTER_NAME_LOCAL="pg_ha_1"
CLUSTER_NAME_REMOTE="pg_ha_2"
CLUSTER_IP_REMOTE="10.7.2.93"
PGPORT="5432"

#-----------------------------Retries & Timeouts---------------------------#
# retries = 1 - disables retries
# timeout = 0 - disables timeout

LOCK_RETRIES=1
UNLOCK_RETRIES=1

SSH_CONNECT_TIMEOUT=4

PG_STATUS_CHECK_RETRIES=3               
PG_STATUS_CHECK_TIMEOUT=6

PG_REPLCATION_CHECK_RETRY_DELAY=5
PG_REPLICATION_CHECK_TIMEOUT=240

PG_BASEBACKUP_RESTORE_TIMEOUT=120

PG_START_TIMEOUT=120

PG_STOP_TIMEOUT=120

PG_REWIND_TIMEOUT=120

STATE_VERIFICATION_TIMEOUT=120       # timeout for retrieving remote's statuses for verification
                                    # 0 - disables timeout
STATE_VERIFICATION_RETRY_DELAY=5    # delay between state verification attempts

RETRY_DELAY=3                       # time in seconds between command reties
#------------------------------Script behavior-----------------------------#
LOG_LEVEL=5                         # 0 - Emergency 
                                    # 1 - Alert
                                    # 2 - Critical [FATAL]
                                    # 3 - Error [ERROR]
                                    # 4 - Warning [WARNING]
                                    # 5 - Notice
                                    # 6 - Informational [INFO]
                                    # 7 - Debug [DEBUG]

LOG_TO_JOURNAL=1                    # 1 - enables logging to systemd-journal
                                    # 0 - disables

JOURNAL_LOG_LEVEL=5                 # 0 - Emergency 
                                    # 1 - Alert
                                    # 2 - Critical [FATAL]
                                    # 3 - Error [ERROR]
                                    # 4 - Warning [WARNING]
                                    # 5 - Notice
                                    # 6 - Informational [INFO]
                                    # 7 - Debug [DEBUG]

PG_LOG_ACHIVING=1                   # Saves logs of local PostgreSQL instance
                                    # before destructive actions for later analysis
                                    #  1 - enables
                                    #  0 - disables

PG_ALLOW_STANDALONE_MASTER=1        # Allows standalone master for faster recovery
                                    # 1 - enables
                                    # 0 - disables

PG_USE_RECOVERY_CONF=0              # Changes if recovery.conf is used or if recuvery.signal / standby.signal is used
                                    # 1 - use recovery.conf
                                    # 0 - use .signal files

PG_CATCHUP_TIMEOUT=20               # Determines for how long async replica is allowed to catch up before being recreated
                                    # timeout is specified in minutes
                                    # 0 - disables timeout

PG_REPLICATION_SLOT_NAME="ha_reserved_slot"     # Sets a name for replication slot
#==========================================================================#

declare -A LOGGING_LEVELS
LOGGING_LEVELS["SUCCESS"]=-1
LOGGING_LEVELS["EMERGERNCY"]=0
LOGGING_LEVELS["ALERT"]=1
LOGGING_LEVELS["FATAL"]=2
LOGGING_LEVELS["ERROR"]=3
LOGGING_LEVELS["WARNING"]=4
LOGGING_LEVELS["NOTICE"]=5
LOGGING_LEVELS["INFO"]=6
LOGGING_LEVELS["DEBUG"]=7


log_message() {
    # Logging levels
    # 0 - Emergency 
    # 1 - Alert
    # 2 - Critical [FATAL]
    # 3 - Error [ERROR]
    # 4 - Warning [WARNING]
    # 5 - Notice
    # 6 - Informational [INFO]
    # 7 - Debug [DEBUG]

    local message="$1"
    local log_level="$2"
    local timestamp
    local priority

    timestamp=$(date +"%Y-%m-%d %H:%M:%S.%3N")
    priority="${LOGGING_LEVELS[$log_level]}"
    
    if [[ "$log_level" == "END" ]]; then
        if (( $(date +"%s") - $(stat -c %Y "$LOGFILE_PATH" 2>/dev/null) <= 5 )); then
            echo -e "\n" >> "$LOGFILE_PATH"
        fi
        return 0
    fi

    if (( priority <= LOG_LEVEL )); then
        echo -e "[Node: $CLUSTER_NAME_LOCAL] [$timestamp] [$log_level]  $message" >> "$LOGFILE_PATH"
        echo -e "[Node: $CLUSTER_NAME_LOCAL] [$timestamp] [$log_level]  $message" >> "$LOCK_FILE_DIR"/current.log.temp
    fi

    if (( priority <= JOURNAL_LOG_LEVEL )); then
        if [[ "$LOG_TO_JOURNAL" -eq 1 ]]; then
            if [[ "$priority" -eq -1 ]]; then
                priority=5
            fi
            echo -e  "$message" | systemd-cat --priority="$priority" --identifier="pg-ha-controller"
        fi
    fi

    return 0
}


command_handler() {
    # returns 0 if command executed successfully and outputs result
    # returns 124 if command timed out
    # returns command's error code if the command failed and outputs error
    
    local command="$1"          # Command to execute
    local retries="$2"          # Number of retries
    local timeout="$3"          # Timeout in seconds
    local command_message="$4"  # Short command description

    local attempt=1
    local exit_code
    local result

    log_message "   $command_message locally" "INFO"

    if [[ "$retries" -eq 1 ]]; then
        if [[ "$timeout" -eq 0 ]]; then
            result=$(bash -c "$command" 2>&1)
            exit_code="$?"
        else
            result=$(timeout "$timeout" bash -c "$command" 2>&1)
            exit_code="$?"
        fi
    else
        while (( attempt <= retries )); do
            log_message "       attempt $attempt/$retries" "INFO"

            if [[ "$timeout" -eq 0 ]]; then
                result=$(bash -c "$command" 2>&1)
                exit_code="$?"
            else
                result=$(timeout "$timeout" bash -c "$command" 2>&1)
                exit_code="$?"
            fi

            if [[ "$exit_code" -eq 0 ]]; then
                log_message "       running command succeded on attempt $attempt" "INFO"
                break
            elif [[ "$exit_code" -eq 124 ]]; then
                log_message "       attempt $attempt of $command_message timed out after $timeout seconds" "WARNING"
            else
                log_message "       attempt $attempt of $command_message failed with error: \n$result" "WARNING"
            fi

            if (( attempt < retries )); then
                log_message "       retrying in $RETRY_DELAY s" "INFO"
                sleep "$RETRY_DELAY"
            fi

            ((attempt++))
        done
    fi

    if [[ "$exit_code" -eq 0 ]]; then
        log_message "   $command_message locally, succeded" "INFO"
        echo "$result"
        return "$exit_code"
    elif [[ "$exit_code" -eq 124 ]]; then
        log_message "   $command_message locally, failed due to timeout" "ERROR"
        return "$exit_code"
    else
        log_message "   $command_message locally, failed due to error: \n$result" "ERROR"
        echo "$result"
        return "$exit_code"
    fi
}


remote_command_handler() {
    # returns 0 if command executed successfully and outputs result
    # returns 124 if command timed out
    # returns 255 if encountered ssh error and outputs ssh error
    # returns remote command's error code if the command failed and outputs error

    local command="$1"          # Command to execute
    local retries="$2"          # Number of retries
    local timeout="$3"          # Timeout in seconds
    local command_message="$4"  # Short command description
    local user="$5"             # remote SSH user
    local host="$6"             # remote SSH host

    local attempt=1
    local exit_code
    local result

    log_message "   $command_message remotely on $user@$host" "INFO"

    if [[ "$retries" -eq 1 ]]; then
        if [[ "$timeout" -eq 0 ]]; then
            result=$(ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$user"@"$host" "$command" 2>&1)
            exit_code="$?"
        else
            result=$(timeout "$timeout" ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$user"@"$host" "$command" 2>&1)
            exit_code="$?"
        fi
    else
        while (( attempt <= retries )); do
            log_message "       attempt $attempt/$retries" "INFO"

            if [[ "$timeout" -eq 0 ]]; then
                result=$(ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$user"@"$host" "$command" 2>&1)
                exit_code="$?"
            else
                result=$(timeout "$timeout" ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$user"@"$host" "$command" 2>&1)
                exit_code="$?"
            fi

            if [[ "$exit_code" -eq 0 ]]; then
                log_message "       running command succeded on attempt $attempt" "INFO"
                break
            elif [[ "$exit_code" -eq 124 ]]; then
                log_message "       attempt $attempt of $command_message timed out after $timeout seconds" "WARNING"
            elif [[ "$exit_code" -eq 255 ]]; then
                log_message "       attempt $attempt of $command_message failed with SSH error: \n$result" "WARNING"
            else
                log_message "       attempt $attempt of $command_message failed with error: \n$result" "WARNING"
            fi

            if (( attempt < retries )); then
                log_message "       retrying in $RETRY_DELAY s" "INFO"
                sleep "$RETRY_DELAY"
            fi

            ((attempt++))
        done
    fi

    if [[ "$exit_code" -eq 0 ]]; then
        log_message "   $command_message remotely on $user@$host, succeded" "INFO"
        echo "$result"
        return "$exit_code"
    elif [[ "$exit_code" -eq 124 ]]; then
        log_message "   $command_message remotely on $user@$host, failed due to timeout" "ERROR"
        return "$exit_code"
    elif [[ "$exit_code" -eq 255 ]]; then
        log_message "   $command_message remotely on $user@$host, failed due to SSH error: \n$result" "ERROR"
        echo "$result"
        return "$exit_code"
    else
        log_message "   $command_message remotely on $user@$host, failed due to error: \n$result" "ERROR"
        echo "$result"
        return "$exit_code"
    fi
}


archive_local_postgresql_logs() {
    # Archives local PostgreSQL instances logs to a separate folder before destructive actions
    # returns 0 if successfull
    # returns 1 if failes

    if [[ "$PG_LOG_ACHIVING" -ne 1 ]]; then
        return 0
    else 
        log_message "Archiving local PostgreSQL logs" "NOTICE"

        if command_handler "tar -czvf $PG_LOG_ARCIVE_DIR/pg_logs_$(date +"%Y-%m-%d_%H:%M:%S_%3N").tar.gz -C $PGDATA/ $PGLOG/" "1" "0" "archiving logs" > /dev/null; then
            log_message "Succesfully archived PostgreSQL log directory to:  $PG_LOG_ARCIVE_DIR/pg_logs_$(date +"%Y-%m-%d_%H:%M:%S_%3N").tar.gz" "NOTICE"
            return 0
        else
            log_message "Failed to archive PostgreSQL log directory" "WARNING"
            return 1
        fi
    fi
}


check_local_postgres() {
    # returns 1 if PostgreSQL is running and is in Master role
    # returns 2 if PostgreSQL is running and is in Slave role
    # returns 0 if PostgreSQL isn't running and outputs latest checkpoint's REDO location
    # returns 10 if encountered error
    
    local host=$1           # PostgreSQL host
    local port=$2           # PostgreSQL port 
    local user=$3           # PostgreSQL user

    log_message "Checking local PostgreSQL status" "INFO"

    command_handler "$PG_ISREADY -h $host -p $port -U $user" "1" "$PG_STATUS_CHECK_TIMEOUT" "cheking if PostgreSQL is running" > /dev/null
    local pg_is_ready_exit_code="$?"
    if [[ "$pg_is_ready_exit_code" -eq 0 ]]; then                       # PostgreSQL is ready to accept connections
        local is_slave
        local is_slave_exit_code
        is_slave=$(command_handler "$PSQL -h $host -p $port -U $user -tAc \"SELECT pg_is_in_recovery();\"" "1" "$PG_STATUS_CHECK_TIMEOUT" "checking PostgreSQL replication role")
        is_slave_exit_code="$?"

        if [[ "$is_slave_exit_code" -eq 0 ]]; then
            if [[ "$is_slave" = "t" ]]; then
                log_message "Local PostgreSQL server is running in \"Slave\" role" "INFO"
                return 2
            else
                log_message "Local PostgreSQL server is running in \"Master\" role" "INFO"
                return 1
            fi
        else
            log_message "Unable to determine local PostgreSQL replication status" "ERROR"
            return 10
        fi
    elif [[ "$pg_is_ready_exit_code" -eq 1 ]]; then                     # PostgreSQL is starting up
            log_message "Local PostgreSQL is starting up" "NOTICE"
            return 10
    else
        log_message "Local PostgreSQL server is not running" "INFO"     # PostgreSQL is not running

        local controldata_result
        local controldata_exit_code
        controldata_result=$(command_handler "$PG_CONTROLDATA -D $PGDATA" "1" "$PG_STATUS_CHECK_TIMEOUT" "getting latest cheekpoint's REDO")
        controldata_exit_code="$?"

        if [[ "$controldata_exit_code" -eq 0 ]]; then
            local redo_location
            redo_location=$(echo "$controldata_result" | grep "REDO location:" | awk '{print $NF}')
            log_message "Local PostgreSQL latest checkpoint's REDO location = $redo_location" "INFO"
            echo "$redo_location"
            return 0
        else
            echo "<unable to retrieve latest checkpoint's REDO>"
            return 0
        fi
    fi
}


check_remote_postgres() {
    # returns 1 if PostgreSQL is running and is in Master role
    # returns 2 if PostgreSQL is running and is in Slave role
    # returns 0 if PostgreSQL isn't running and outputs latest checkpoint's REDO location
    # returns 3 if encountered ssh error
    # returns 10 if encountered other error
    
    local host=$1           # PostgreSQL host
    local port=$2           # PostgreSQL port 
    local user=$3           # PostgreSQL user
    local ssh_user=$4       # remote system user
    local ssh_host=$5       # remote system address

    log_message "Checking remote PostgreSQL status on $ssh_user@$ssh_host" "INFO"

    remote_command_handler "$PG_ISREADY -h $host -p $port -U $user" "1" "$PG_STATUS_CHECK_TIMEOUT" "cheking if PostgreSQL is running" "$ssh_user" "$ssh_host" > /dev/null
    local pg_is_ready_exit_code="$?"
    if [[ "$pg_is_ready_exit_code" -eq 0 ]]; then                       # PostgreSQL is ready to accept connections
        local is_slave
        local is_slave_exit_code
        is_slave=$(remote_command_handler "$PSQL -h $host -p $port -U $user -tAc \"SELECT pg_is_in_recovery();\"" "1" "$PG_STATUS_CHECK_TIMEOUT" "checking PostgreSQL replication role" "$ssh_user" "$ssh_host")
        is_slave_exit_code="$?"

        if [[ "$is_slave_exit_code" -eq 0 ]]; then
            if [[ "$is_slave" = "t" ]]; then
                log_message "Remote PostgreSQL server on $ssh_user@$ssh_host is running in \"Slave\" role" "INFO"
                return 2
            else
                log_message "Remote PostgreSQL server on $ssh_user@$ssh_host is running in \"Master\" role" "INFO"
                return 1
            fi
        elif [[ "$is_slave_exit_code" -eq 255 ]]; then                  # SSH error
            log_message "Unable to determine remote PostgreSQL replication status on $ssh_user@$ssh_host due to SSH error" "INFO"
            return 3
        else
            log_message "Unable to determine remote PostgreSQL replication status on $ssh_user@$ssh_host" "ERROR"
            return 10
        fi
    elif [[ "$pg_is_ready_exit_code" -eq 1 ]]; then                     # PostgreSQL is starting up
            log_message "Remote PostgreSQL on $ssh_user@$ssh_host is starting up" "NOTICE"
            return 10
    elif [[ "$pg_is_ready_exit_code" -eq 255 ]]; then                   # SSH error
            log_message "Couldn't check remote PostgreSQL status on on $ssh_user@$ssh_host due to SSH error" "INFO"
            return 3
    else
        log_message "Remote PostgreSQL server on $ssh_user@$ssh_host is not running" "INFO"     # PostgreSQL is not running

        local controldata_result
        local controldata_exit_code
        controldata_result=$(remote_command_handler "$PG_CONTROLDATA -D $PGDATA" "1" "$PG_STATUS_CHECK_TIMEOUT" "getting latest cheekpoint's REDO" "$ssh_user" "$ssh_host")
        controldata_exit_code="$?"

        if [[ "$controldata_exit_code" -eq 0 ]]; then
            local redo_location
            redo_location=$(echo "$controldata_result" | grep "REDO location:" | awk '{print $NF}')
            log_message "Remote PostgreSQL on $ssh_user@$ssh_host latest checkpoint's REDO location = $redo_location" "INFO"
            echo "$redo_location"
            return 0
        elif [[ "$pg_is_ready_exit_code" -eq 255 ]]; then               # SSH error
            log_message "Couldn't retrieve remote PostgreSQL on $ssh_user@$ssh_host latest checkpoint's REDO due to SSH error" "INFO"
            echo "<unable to retrieve latest checkpoint's REDO>"
            return 3
        else
            echo "<unable to retrieve latest checkpoint's REDO>"
            return 0
        fi
    fi
}


check_postgresql() {
    # returns 0 and outputs both PostgreSQL instances replication roles (and if PostgreSQL instance is down, it's latest checkpoint's REDO location)
    # returns 1 if fails to aquire PostgreSQL replication pair status

    local host=$1
    local port=$2
    local user=$3
    local ssh_user=$4
    local ssh_host=$5

    local local_postgres_status
    local local_postgres_redo
    local remote_postgres_status
    local remote_postgres_redo

    local attempt=1

    log_message "Checking PostgreSQL replication pair status" "INFO"

    while (( attempt <= PG_STATUS_CHECK_RETRIES )); do
        log_message "Attempt $attempt/$PG_STATUS_CHECK_RETRIES of checking PostgreSQL replication pair status" "INFO"

        local_postgres_redo=$(check_local_postgres "$host" "$port" "$user")
        local_postgres_status="$?"
        if [[ "$local_postgres_status" -eq 10 ]]; then
            log_message "Unable to gather PostgreSQL replication pair status on attempt $attempt" "ERROR"
        else
            remote_postgres_redo=$(check_remote_postgres "$host" "$port" "$user" "$ssh_user" "$ssh_host")
            remote_postgres_status="$?"
            if [[ "$remote_postgres_status" -eq 10 ]]; then
                log_message "Unable to gather PostgreSQL replication pair status on attempt $attempt" "ERROR"
            else
                break
            fi
        fi

        if (( attempt <= retries )); then
            log_message "Retrying in $RETRY_DELAY s" "INFO"
            sleep "$RETRY_DELAY"
        fi
        ((attempt++))
    done

    if (( local_postgres_status + remote_postgres_status >= 10)); then
        log_message "Failed to aquire PostgreSQL replication pair status" "FATAL"
        return 1
    else
        log_message "PostgreSQL replication pair status aquired on attempt $attempt" "INFO"
        echo "$local_postgres_status:$local_postgres_redo:$remote_postgres_status:$remote_postgres_redo"
        return 0
    fi
}


check_postgresql_replication_sync_state() {
    # uses psql to check standby's replication sync state
    # option 1:
        # 1 if master is running locally
        # 0 if master is running remotely

    # returns 0 if replica is synchronous
    # returns 1 if replica is asynchronous
    # returns 5 if no replication is happening
    # returns 10 if fails to check

    local master_is_local=$1
    local host

    if [[ "$master_is_local" -eq 1 ]]; then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi

    local replication_status
    local exit_code
    replication_status=$(command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT sync_state FROM pg_stat_replication\"" "1" "0" "cheching replication sync state")
    exit_code="$?"

    if [[ "$exit_code" -eq 0 ]]; then
        if [[ "$replication_status" = "sync" ]]; then
            log_message "Replica is synchronous" "INFO"
            return 0
        elif [[ "$replication_status" = "async" ]]; then
            log_message "Replica is asynchronous" "INFO"
            return 1
        else
            return 5
        fi
    fi

    log_message "Failed to fetch replication sync state" "ERROR"
    return 10
}

check_postgresql_replication_state() {
    # uses psql to check standby's replication state
    # option 1:
        # 1 if master is running locally
        # 0 if master is running remotely

    # returns 0 if replica is streaming
    # returns 1 if replica is catchup
    # returns 5 if replica in some other state
    # returns 10 if fails to check

    local master_is_local=$1
    local host

    if [[ "$master_is_local" -eq 1 ]]; then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi

    local replication_state
    local exit_code
    replication_state=$(command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT state FROM pg_stat_replication\"" "1" "0" "cheching replication sync state")
    exit_code="$?"

    if [[ "$exit_code" -eq 0 ]]; then
        if [[ "$replication_state" = "streaming" ]]; then
            log_message "Replica is streaming" "INFO"
            return 0
        elif [[ "$replication_state" = "catchup" ]]; then
            log_message "Replica is catching up" "INFO"
            return 1
        else
            return 5
        fi
    fi

    log_message "Failed to fetch replication sync state" "ERROR"
    return 10
}


check_previous_local_cluster_role() {
    # returns 0 if previous role file not found
    # return 1 if previous local PostgreSQL role was found and it is master
    # return 2 if previous local PostgreSQL role was found and it is replica

    log_message "Checking previous local PostgreSQL replication role" "NOTICE"
    local previous_role
    local exit_code
    previous_role=$(command_handler "cat $LOCK_FILE_DIR/previous_local_cluster_role" "1" "0" "checking previous replication role")
    exit_code="$?"
    if [[ "$exit_code" -ne 0 ]]; then
        log_message "Previous local PostgreSQL cluster role not found" "FATAL"
        return 0
    else
        if [[ "$previous_role" = "M" ]]; then
            return 1
        else
            return 2
        fi
    fi
}


configure_local_master() {
    # Configures local postgresql instance as master
    # option 1:
        # 1 if you want to make replica synchronous
        # 0 if you want to make replica asynchronous

    # returns 0 if config was generated successfylly
    # returns 1 if config failed to generate

    local attach_synchronous_replica=$1

    log_message "Generating postgresql.auto.conf for master" "NOTICE"
    
    log_message "Cleaning current postgresql.auto.conf" "NOTICE"
    rm --force ${PGDATA:?}/postgresql.auto.conf > /dev/null

    if (( PG_ALLOW_STANDALONE_MASTER == 0 )) || (( attach_synchronous_replica == 1 )) ; then
        log_message "Adding synchronous replica to configuration" "NOTICE"
        echo "synchronous_standby_names = '$CLUSTER_NAME_REMOTE'" > "$PGDATA"/postgresql.auto.conf
    else
        echo "" > "$PGDATA"/postgresql.auto.conf
    fi

    chmod 600 "$PGDATA"/postgresql.auto.conf > /dev/null
    chown postgres:postgres "$PGDATA"/postgresql.auto.conf > /dev/null

    if (( PG_USE_RECOVERY_CONF == 1 )); then
        log_message "Removing old recovery.done config file" "NOTICE"
        rm --force ${PGDATA:?}/recovery.done > /dev/null
    else
        log_message "Removing unnecessary standby.signal" "NOTICE"
        rm --force ${PGDATA:?}/standby.signal > /dev/null
    fi 

    if [[ -e "$PGDATA"/postgresql.auto.conf ]]; then
        if ! sync; then
            log_message "Failed to generate postgresql.auto.conf (fsync failed)" "ERROR"
            return 1
        fi
        
        log_message "Successfully generated postgresql.auto.conf" "NOTICE"
        return 0
    else
        log_message "Failed to generate postgresql.auto.conf" "ERROR"
        return 1
    fi
}


configure_local_replica() {
    # Configures local postgresql instance as replica
    # option 1:
        # 1 if you want replica to use replication slot
        # 0 if you don't want replica to use replication slot

    # returns 0 if config was generated successfylly
    # returns 1 if config failed to generate

    local enable_replication_slot=$1

    if [[ "$PG_USE_RECOVERY_CONF" -eq 1 ]]; then
        # configure with recovery.conf
        # creating postgresql.auto.conf
        log_message "Generating postgresql.auto.conf for replica" "NOTICE"
    
        log_message "Cleaning current postgresql.auto.conf" "NOTICE"
        rm --force ${PGDATA:?}/postgresql.auto.conf > /dev/null

        echo "" > "$PGDATA"/postgresql.auto.conf

        chmod 600 "$PGDATA"/postgresql.auto.conf > /dev/null
        chown postgres:postgres "$PGDATA"/postgresql.auto.conf > /dev/null

        # creating recovery.conf
        log_message "Generating recovery.conf for replica" "NOTICE"
    
        log_message "Cleaning current recovery.conf" "NOTICE"
        rm --force ${PGDATA:?}/recovery.conf > /dev/null

        echo "standby_mode = 'on'" > "$PGDATA"/recovery.conf
        {
            echo "recovery_target_timeline = 'latest'" 
            echo "recovery_target_action = 'pause'"
            echo "primary_conninfo = 'user=$PG_REPLICATION_USER host=$CLUSTER_IP_REMOTE port=5432 application_name=$CLUSTER_NAME_LOCAL'"
        } >> "$PGDATA"/recovery.conf;
        if (( enable_replication_slot == 1 )) ; then
            log_message "Adding replication slot to configuration" "NOTICE"
            echo "primary_slot_name = '$PG_REPLICATION_SLOT_NAME'" >> "$PGDATA"/recovery.conf
        fi

        chmod 600 "$PGDATA"/recovery.conf > /dev/null
        chown postgres:postgres "$PGDATA"/recovery.conf > /dev/null

        if [[ -e "$PGDATA"/recovery.conf ]] && [[ -e "$PGDATA"/postgresql.auto.conf ]]; then
            if ! sync; then
                log_message "Failed to generate PostgreSQL replica configuration (fsync failed)" "ERROR"
                return 1
            fi
            
            log_message "Successfully generated PostgreSQL replica configuration" "NOTICE"
            return 0
        else
            log_message "Failed to generate PostgreSQL replica configuration" "ERROR"
            return 1
        fi
    else
        # configure with postgresql.auto.conf
        log_message "Generating postgresql.auto.conf for replica" "NOTICE"
    
        log_message "Cleaning current postgresql.auto.conf" "NOTICE"
        rm --force ${PGDATA:?}/postgresql.auto.conf > /dev/null

        echo "recovery_target_timeline = 'latest'" > $PGDATA/postgresql.auto.conf
        echo "recovery_target_action = 'pause'" >> $PGDATA/postgresql.auto.conf
        echo "primary_conninfo = 'user=$PG_REPLICATION_USER host=$CLUSTER_IP_REMOTE port=5432 application_name=$CLUSTER_NAME_LOCAL'" >> $PGDATA/postgresql.auto.conf
        if (( enable_replication_slot == 1 )) ; then
            log_message "Adding replication slot to configuration" "NOTICE"
            echo "primary_slot_name = '$PG_REPLICATION_SLOT_NAME'" >> "$PGDATA"/postgresql.auto.conf
        fi

        chmod 600 "$PGDATA"/postgresql.auto.conf > /dev/null
        chown postgres:postgres "$PGDATA"/postgresql.auto.conf > /dev/null

        log_message "Adding standby.signal" "NOTICE"
        echo "" > "$PGDATA"/standby.signal

        chmod 600 "$PGDATA"/standby.signal > /dev/null
        chown postgres:postgres "$PGDATA"/standby.signal > /dev/null

        if [[ -e "$PGDATA"/postgresql.auto.conf ]] && [[ -e "$PGDATA"/standby.signal ]]; then
            if ! sync; then
                log_message "Failed to generate PostgreSQL replica configuration (fsync failed)" "ERROR"
                return 1
            fi
            
            log_message "Successfully generated PostgreSQL replica configuration" "NOTICE"
            return 0
        else
            log_message "Failed to generate PostgreSQL replica configuration" "ERROR"
            return 1
        fi
    fi
}


manipulate_replication_slot() {
    # uses psql to create or drop replication slot
    # option 1:
        # 1 if master is running locally
        # 0 if master is running remotely
    # option 2:
        # 1 if you want to create a replication slot
        # 0 if you want to drop the replication slot

    # returns 0 if operation successfull
    # returns 1 if operation failed

    local master_is_local=$1
    local create_replication_slot=$2
    local host

    if [[ "$master_is_local" -eq 1 ]]; then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi
    
    if [[ "$create_replication_slot" -eq 1 ]]; then
        log_message "Creating replication slot" "NOTICE"
        if ! command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT * FROM pg_create_physical_replication_slot('$PG_REPLICATION_SLOT_NAME', true)\"" "1" "0" "creating replication slot" > /dev/null; then
            log_message "Failed to create replication slot" "ERROR"
            return 1
        fi
    else
        log_message "Dropping replication slot" "NOTICE"
        if ! command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT * FROM pg_drop_replication_slot('$PG_REPLICATION_SLOT_NAME')\"" "1" "0" "dropping replication slot" > /dev/null; then
            log_message "Failed to drop replication slot" "ERROR"
            return 1
        fi
    fi

    return 0
}

check_replication_slot() {
    # checks if replication slot exisis and if it's not in use
    # returns 0 if replication slot is free
    # returns 1 if replication slot does not exist
    # returns 2 if replication slot is in use
    # returns 124 if check failed on timeout

    log_message "Checking replication slot state" "NOTICE"

    local start_time
    start_time=$(date +%s)
    local exit_code

    while true; do
        local replication_slot
        local replication_slot_exit_code
        replication_slot=$(command_handler "$PSQL -h localhost -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT active FROM pg_replication_slots\"" "1" "0" "cheching replication slot")
        replication_slot_exit_code="$?"

        if (( replication_slot_exit_code == 0)); then
            if [[ "$replication_slot" = "f" ]]; then
                log_message "Replication slot is not in use" "NOTICE"
                exit_code="0"
                break
            fi
            
            if [[ "$replication_slot" = "" ]]; then
                log_message "Replication slot doen't exist" "ERROR"
                exit_code="1"
                break
            fi
        fi

        if (( $(date +%s) - start_time >= PG_REPLICATION_CHECK_TIMEOUT )); then
            log_message "Replication slot state check failed by timeout" "ERROR"
            exit_code="124"
            break
        fi
        sleep "$PG_REPLCATION_CHECK_RETRY_DELAY"
    done
    
    return "$exit_code"
}



local_postgresql_control() {
    # uses pg_ctl to stop, start, restart and reload configuration
    # option 1:
        # start - starts local PostgreSQL instance
        # stop - stops local PostgreSQL instance
        # restart - resatarts local PostgreSQL instance
        # reload - reloads local PostgreSQL instance's configuration
        # promote - promotes local PostgreSQL replica to master
    # option 2: pg_role acccepts "master" / "replica" to correct log messages

    # returns 0 if operation successfull
    # returns 1 if operation failed

    local command_option=$1
    local pg_role=$2

    if [[ "$command_option" = "start" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null start" "1" "$PG_START_TIMEOUT" "starting PostgreSQL" > /dev/null; then
            log_message "Failed to start local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully started local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "stop" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null stop" "1" "$PG_STOP_TIMEOUT" "stopping PostgreSQL" > /dev/null; then
            log_message "Failed to stop local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully stopped local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "restart" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null restart" "1" "0" "restarting PostgreSQL" > /dev/null; then
            log_message "Failed to restart local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully restarted local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "reload" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null reload" "1" "0" "reloading PostgreSQL config" > /dev/null; then
            log_message "Failed to reload local PostgreSQL $pg_role instance's config" "ERROR"
            return 1
        else
            log_message "Successfully reloaded local PostgreSQL $pg_role instance's config" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "promote" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null promote" "1" "0" "promoting PostgreSQL" > /dev/null; then
            log_message "Failed to promote local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully promoted local PostgreSQL $pg_role instance to new master" "NOTICE"
            return 0
        fi
    else
        log_message "Provided option is incorrect" "ERROR"
        return 1
    fi

}


master_replication_check() {
    # chercks for replication presence on master
    # option 1:
        # 1 checks for synchronous replication
        # 0 checks for asynchronous replication
    # returns 0 if replication on master started
    # returns 124 if syncronous replication check timed out

    local check_for_sync=$1

    local replication_status="-1"
    local replication_status_target
    local replication_status_target_name

    if (( check_for_sync == 1 )); then
        replication_status_target=0
        replication_status_target_name="Synchronous"
    else
        replication_status_target=1
        replication_status_target_name="Asynchronous"
    fi

    log_message "Waiting for $replication_status_target_name replication to start on master..." "NOTICE"

    local start_time
    start_time=$(date +%s)
    local exit_code

    while true; do
        check_postgresql_replication_sync_state "1"
        replication_status="$?"

        if (( replication_status == replication_status_target)); then
            log_message "$replication_status_target_name replication on master started" "NOTICE"
            exit_code="0"
            break
        fi

        if (( $(date +%s) - start_time >= PG_REPLICATION_CHECK_TIMEOUT )); then
            log_message "$replication_status_target_name replication on master didn't start after $PG_REPLICATION_CHECK_TIMEOUT s, failed by timeout" "ERROR"
            exit_code="124"
            break
        fi
        sleep "$PG_REPLCATION_CHECK_RETRY_DELAY"
    done
    
    return "$exit_code"
}


slave_replication_check() {
    # returns 0 if replication on replica started
    # returns 124 if replication check timed out

    local host=$1
    local port=$2
    local user=$3

    local wal_receiver_status=""

    log_message "Waiting for wal receiver to start streaming on replica..." "NOTICE"

    local start_time
    start_time=$(date +%s)
    local exit_code

    while true; do
        wal_receiver_status=$(command_handler "$PSQL -h $host -p $port -U $user -tAc \"SELECT status from pg_stat_wal_receiver\"" "1" "0" "cheching PostgreSQL replica wal receiver status")

        if [[ "$wal_receiver_status" = "streaming" ]]; then
            log_message "Wal receiver started streaming on replica, replication started" "NOTICE"
            exit_code="0"
            break
        elif (( $(date +%s) - start_time >= PG_REPLICATION_CHECK_TIMEOUT )); then
            log_message "Wal receiver didn't start streaming after $PG_REPLICATION_CHECK_TIMEOUT s, failed by timeout" "ERROR"
            exit_code="124"
            break
        else
            sleep "$PG_REPLCATION_CHECK_RETRY_DELAY"
        fi
    done
    
    return "$exit_code"
}


restore_as_replica() {
    # option 1:
        # 1 - retores as replica not utilizing replication slot
        # 0 - retores as replica utilizing replication slot if applicable
    # 0 If successfully restored local replica with pg_basebackup
    # 1 If failed
    local restore_without_replication_slot=$1
    
    log_message "Restoring local PostgreSQL with pg_basebackup as replica" "NOTICE"
    
    archive_local_postgresql_logs

    log_message "Cleaning current pg_data and pg_wal directories" "NOTICE"
    if ! command_handler "rm --recursive --force ${PGWAL:?}/*" "3" "0" "emptying pg_wal directory" > /dev/null; then
        log_message "Failed to restore local PostgreSQL as replica: unable to cleanup previous data directory" "ERROR"
        return 1
    fi
    if ! command_handler "rm --recursive --force ${PGDATA:?}/*" "3" "0" "emptying pg_data directory" > /dev/null; then
        log_message "Failed to restore local PostgreSQL as replica: unable to cleanup previous data directory" "ERROR"
        return 1
    fi

    log_message "Restoring local Postgresql data directory with pg_basebackup" "NOTICE"
    if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_BASEBACKUP -D $PGDATA --waldir $PGWAL -U $PG_REPLICATION_USER -h $CLUSTER_IP_REMOTE -R" "1" "$PG_BASEBACKUP_RESTORE_TIMEOUT" "restoring with pg_basebackup" > /dev/null; then
        log_message "Failed to restore local PostgreSQL as replica: pg_basebackup failed" "ERROR"
        return 1
    else
        log_message "Local PostgreSQL data directory successfully restored from current master" "NOTICE"
    fi

    if (( restore_without_replication_slot == 0 )) && (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        if ! configure_local_replica "1"; then
            log_message "Failed to restore local PostgreSQL as replica: unable to configure PostgreSQL instance" "ERROR"
        return 1
        fi
    else
        if ! configure_local_replica "0"; then
            log_message "Failed to restore local PostgreSQL as replica: unable to configure PostgreSQL instance" "ERROR"
        return 1
        fi
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to restore local PostgreSQL as replica: PostgreSQL instance didn't start" "ERROR"
        return 1
    fi

    log_message "Local PostgreSQL successfully restored as replica" "NOTICE"
    return 0
}


recover_local_replica() {
    # attempts to recover local replica by restarting or restoring from pg_basebackup
    # 0 If successfully recovered local replica
    # 1 If failed

    log_message "Starting recovery process for local replica" "NOTICE"

    if (( PG_ALLOW_STANDALONE_MASTER == 1)); then
        if configure_local_replica "1"; then
            if local_postgresql_control "start" "replica"; then
                if slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
                    if save_async_replication_start; then
                        log_message "Recovery attempt succeded: replica reattached after restart" "NOTICE"
                        return 0
                    fi
                fi
            fi
        fi
    else
        if configure_local_replica "0"; then
            if local_postgresql_control "start" "replica"; then
                if slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
                    log_message "Recovery attempt succeded: replica reattached after restart" "NOTICE"
                    return 0
                fi
            fi
        fi
    fi
    log_message "Restarting local replica attempt failed" "WARNING"

    if restore_as_replica "0"; then
        if slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
            if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                if save_async_replication_start; then
                    log_message "Recovery attempt succeded: replica reattached after restoration from pg_basebackup" "NOTICE"
                    return 0
                fi
            else
                log_message "Recovery attempt succeded: replica reattached after restoration from pg_basebackup" "NOTICE"
                    return 0
            fi
        fi
    fi

    log_message "Failed to recover replica: restart and restoration attempts failed" "FATAL"
    return 1
}

prepare_replica_for_sync_transition() {
    # reconfigures local replica to stop using replciation slot
    # 0 - if succeds
    # 1 - if fails

    log_message "Preparing local replica for sync transition" "NOTICE"

    if ! local_postgresql_control "stop" "replica"; then
        log_message "Failed to prepare local replica for sync transition: failed to stop local replica" "ERROR"
        return 1
    fi

    if ! configure_local_replica "0"; then
        log_message "Failed to prepare local replica for sync transition: failed to reconfigure local replica" "ERROR"
        return 1
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to prepare local replica for sync transition: failed to start local replica with new configuration" "ERROR"
        return 1
    fi

    if ! slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
        log_message "Failed to prepare local replica for sync transition: replication check failed" "ERROR"
        return 1
    fi

    log_message "Local replica is ready for sync transition" "NOTICE"
    return 0
}

restore_as_replcia_to_catch_up() {
    # restores replica with pg_basebackup to catch up to master
    # 0 - if succeds
    # 1 - if fails

    log_message "Restoring local PostgreSQL as replica to catch up to master" "NOTICE"

    local_postgresql_control "stop" "replica" > /dev/null

    if ! restore_as_replica "1"; then
        log_message "Failed to restore local PostgreSQL as replica to catch up to master: failed to restore local Postgresql as replica" "ERROR"
        return 1
    fi

    if ! slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
        log_message "Failed to restore local PostgreSQL as replica to catch up to master: replication check failed" "ERROR"
        return 1
    fi

    log_message "Local replica is ready for sync transition" "NOTICE"
    return 0
}


promote_local_replica() {
    # 0 If successfully promoted local replica
    # 1 If failed

    log_message "Promoting local replica" "NOTICE"

    if ! local_postgresql_control "promote" "replica"; then
        log_message "Failed to promote local replica: pg_ctl promote failed" "FATAL"
        return 1
    fi

    if ! local_postgresql_control "stop" "master"; then
        log_message "Failed to promote local replica: new master didn't stop" "FATAL"
        return 1
    fi

    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        if ! configure_local_master "0"; then
            log_message "Failed to promote local replica: unable configure new master" "FATAL"
            return 1
        fi
    else
        if ! configure_local_master "1"; then
            log_message "Failed to promote local replica: unable configure new master" "FATAL"
            return 1
        fi
    fi

    if ! local_postgresql_control "start" "master"; then
        log_message "Failed to promote local replica: failed to start master after configuring" "FATAL"
        return 1
    fi

    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        if ! manipulate_replication_slot "1" "1"; then
            log_message "Failed to promote local replica: failed to create a replication slot" "FATAL"
            return 1
        fi
    fi

    log_message "Promoting local replica succeded, new master is ready to start replication" "NOTICE"
}


rewind_local_master() {
    # 0 If successfully rewound previous master
    # 1 If failed

    log_message "Starting rewinding process for previous local master" "NOTICE"
    
    archive_local_postgresql_logs

    log_message "Ensuring clean PostgreSQL shutdown" "NOTICE"

    if ! local_postgresql_control "start" "master"; then
        log_message "Failed to rewind previous master: failed to start master instance" "ERROR"
        return 1
    fi

    if ! local_postgresql_control "stop" "master"; then
        log_message "Failed to rewind previous master: failed to stop master instance" "ERROR"
        return 1
    fi

    log_message "Rewinding previous local master" "NOTICE"
    if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_REWIND --target-pgdata=$PGDATA --source-server=\"host=$CLUSTER_IP_REMOTE port=$PGPORT user=$PG_ADMIN_USER\"" "1" "$PG_REWIND_TIMEOUT" "rewinding PostgreSQL"> /dev/null; then
        log_message "Failed to rewind previous master: pg_rewind failed" "ERROR"
        return 1
    fi
    log_message "Successfully rewound previous master" "NOTICE"

    if (( PG_ALLOW_STANDALONE_MASTER == 1)); then
        if ! configure_local_replica "1"; then
            log_message "Failed to rewind previous master: unable to configure instance" "ERROR"
            return 1
        fi
    else
        if ! configure_local_replica "0"; then
            log_message "Failed to rewind previous master: unable to configure instance" "ERROR"
            return 1
        fi
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to rewind previous master: failed to start replica instance" "ERROR"
        return 1
    fi

    log_message "Old master successfully rewound and started as replica" "NOTICE"
    return 0
}


reattach_master_as_replica() {
    # 0 If successfully reattached old master as replica
    # 1 If failed

    log_message "Reattaching old master as replica" "NOTICE"

    if rewind_local_master; then 
        if slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
            if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                if save_async_replication_start; then
                    log_message "Old master reattached as replica after successfull pg_rewind" "NOTICE"
                    return 0
                fi
            else
                log_message "Old master reattached as replica after successfull pg_rewind" "NOTICE"
                return 0
            fi
        fi
    fi
    log_message "Attempt to reattach old master as replica failed: unable to complete pg_rewind" "ERROR"

    if restore_as_replica "0"; then
        if slave_replication_check "localhost" "$PGPORT" "$PG_ADMIN_USER"; then
            if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                if save_async_replication_start; then
                    log_message "Old master reattached as replica after restoring from pg_basebackup" "NOTICE"
                    return 0
                fi
            else
                log_message "Old master reattached as replica after restoring from pg_basebackup" "NOTICE"
                    return 0
            fi
        fi
    fi

    log_message "Failed to reattach old master as replica" "FATAL"
    return 1
}

reconfigure_master_for_standalone_mode() {
    # Disables synchronous configuration on master and creates a replication slot
    # 0 - if successull
    # 1 - if failed

    log_message "Reconfiguring master for standalone mode" "NOTICE"

    if ! configure_local_master "0"; then
        log_message "Reconfiguring master for standalone mode: unable to write configuration" "FATAL"
        return 1
    fi

    if ! local_postgresql_control "reload" "replica"; then
        log_message "Reconfiguring master for standalone mode: master failed to read new configuration" "FATAL"
        return 1
    fi

    if ! manipulate_replication_slot "1" "1"; then
        log_message "Reconfiguring master for standalone mode: failed to create a replication slot" "FATAL"
        return 1
    fi

    return 0
}

reconfigure_master_for_synchronous_mode() {
    # Enables synchronous configuration on master and removes the replication slot
    # option 1:
        # if 1 will attempt to drop replication slot
        # if 0 won't attempt to drop replication slot
    # 0 - if successull
    # 1 - if failed

    local drop_replication_slot=$1

    log_message "Reconfiguring master for synchronous mode" "NOTICE"

    if (( drop_replication_slot == 1)); then
        if ! manipulate_replication_slot "1" "0"; then
            log_message "Reconfiguring master for synchronous mode: failed to drop the replication slot" "FATAL"
            return 1
        fi
    fi

    if ! configure_local_master "1"; then
        log_message "Reconfiguring master for synchronous mode: unable to write configuration" "FATAL"
        return 1
    fi

    if ! local_postgresql_control "reload" "replica"; then
        log_message "Reconfiguring master for standsynchronousalone mode: master failed to read new configuration" "FATAL"
        return 1
    fi

    return 0
}

reconfigure_replica_for_slot_use() {
    if ! configure_local_replica "1"; then
        log_message "Failed configure local replica to use replication slot" "ERROR"
        return 1
    fi

    if ! local_postgresql_control "restart" "replica"; then
        log_message "Failed to restart local replica after configuring" "ERROR"
        return 1
    fi

    if ! save_async_replication_start; then
        log_message "Failed to save async replication start time" "ERROR"
        return 1
    fi

    log_message "Removing failed SSH interconnect signal" "NOTICE"
    rm --force "$LOCK_FILE_DIR"/ssh_interconnect.lost > /dev/null

    if ! sync; then
        log_message "Failed to remove lost SSH interconnect signal (fsync failed)" "ERROR"
        return 1
    fi

    log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
    if ! create_file_remote "$LOCK_FILE_DIR" "remote.unlock" "unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
        log_message "Failed to unlock script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "ERROR"
        return 1
    fi

    return 0
}

dump_current_log(){
    if find "$LOCK_FILE_DIR" -type f -name "*.fatal" | grep -q .; then
        local lock_file_name
        lock_file_name=$(basename -- "$(find "$LOCK_FILE_DIR" -type f -name "*.fatal" | head -n 1)" .fatal)

        command_handler "cat $LOCK_FILE_DIR/current.log.temp > $LOCK_FILE_DIR/$lock_file_name.fatal" "1" "0" "dumping current log" > /dev/null
    fi

    command_handler "rm --force $LOCK_FILE_DIR/current.log.temp" "1" "0" "clearing current log"
}

sync_cought_up_async_replica() {
    # Attempts to promote caught up asynchronous replica to synchronous
    # 0 - if succedes
    # 1 - if fails

    log_message "Attempting to promote asynchronous replica to synchronous" "NOTICE"

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to prepare replica for sync transition" "NOTICE"
    if ! create_file_remote "$LOCK_FILE_DIR" "prepare_transition.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to prepare replica for sync transition"; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: unable to signal replica to prepare for sync transition" "ERROR"
        return 1
    fi

    if ! check_replication_slot; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: replication slot is in still in use" "ERROR"
        return 1
    fi

    if ! reconfigure_master_for_synchronous_mode "1"; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: reconfiguring master for sync replication failed" "ERROR"
        return 1
    fi

    if ! master_replication_check "1"; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: checking for sync replication failed" "ERROR"
        return 1
    fi

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to clean async replication start" "NOTICE"
    if ! create_file_remote "$LOCK_FILE_DIR" "clean_replication_start.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to clean async replication start"; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: unable to signal replica to clean async replication start" "ERROR"
        return 1
    fi

    if ! clean_async_replication_start; then
        log_message "Attempt to promote asynchronous replica to synchronous failed: unable to clean local async replication start" "ERROR"
        return 1
    fi

    log_message "Successfully promoted asynchronous replica to synchronous" "NOTICE"
    return 0
}

sync_lagging_async_replica() {
    # Attempts to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup
    # 0 - if succedes
    # 1 - if fails

    log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup" "NOTICE"

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restore replica to catch up to master" "NOTICE"
    if ! create_file_remote "$LOCK_FILE_DIR" "restore_catchup.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restore replica to catch up to master"; then
        log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: unable to signal replica to restore replica to catch up to master" "ERROR"
        return 1
    fi

    local replication_slot_result
    check_replication_slot
    replication_slot_result="$?"

    if (( replication_slot_result == 0 )); then
        if ! reconfigure_master_for_synchronous_mode "1"; then
            log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: reconfiguring master for sync replication failed" "ERROR"
            return 1
        fi
    elif (( replication_slot_result == 1 )); then
        if ! reconfigure_master_for_synchronous_mode "0"; then
            log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: reconfiguring master for sync replication failed" "ERROR"
            return 1
        fi
    else
        log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: replication slot is in still in use" "ERROR"
        return 1
    fi

    if ! master_replication_check "1"; then
        log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: checking for sync replication failed" "ERROR"
        return 1
    fi

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to clean async replication start" "NOTICE"
    if ! create_file_remote "$LOCK_FILE_DIR" "clean_replication_start.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to clean async replication start"; then
        log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: unable to signal replica to clean async replication start" "ERROR"
        return 1
    fi
    
    if ! clean_async_replication_start; then
        log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup failed: unable to clean local async replication start" "ERROR"
        return 1
    fi

    log_message "Successfully promoted asynchronous replica to synchronous by catching up with pg_basebackup" "NOTICE"
    return 0
}


# ------------------------------------------- State Negotiation -------------------------------------------
share_state_with_remote() {
    # creates a file with current detected state of PostgreSQL instances on remote

    local local_postgres_status=$1
    local remote_postgres_status=$2
    remote_command_handler "echo \"$remote_postgres_status:$local_postgres_status\" >> $LOCK_FILE_DIR/remote_pg.status" "1" "0" "sharing statuses" "$SYSTEM_ADMIN_USER" "$CLUSTER_IP_REMOTE" > /dev/null
    return "$?"
}

read_remote_state() {
    # reades current PostgreSQL instances state detected on remote from local file

    read_remote_state_result=$(command_handler "cat $LOCK_FILE_DIR/remote_pg.status" "1" "0" "reading remote's statuses")
    read_remote_state_exit_code="$?"
    echo "$read_remote_state_result"
    return "$read_remote_state_exit_code"
}

clean_shared_state() {
    # removes remotes state file on local machine
    # 0 if successful
    # 1 if failed

    log_message "Cleaning up PostgreSQL state data, shared by remote" "NOTICE"

    local loop_start_time

    loop_start_time=$(date +"%s")
    while ! [[ -e "$LOCK_FILE_DIR"/remote_pg.status ]];
    do

        if [[ $STATE_VERIFICATION_TIMEOUT -ne 0 ]]; then
            if (( $(date +"%s") - loop_start_time >=  STATE_VERIFICATION_TIMEOUT * 3 )); then
                log_message "Failed clean up remote PostgreSQL state (remote's state wasn't found)" "FATAL"
                return 1
            fi
        fi

         sleep "$STATE_VERIFICATION_RETRY_DELAY"
    done

    if command_handler "rm --force $LOCK_FILE_DIR/remote_pg.status" "10" "0" "cleaning_remote_state" > /dev/null; then
        return 0
    else
        log_message "Failed clean up remote PostgreSQL state (clean shared state manually)" "FATAL"
        return 1
    fi
}

send_state_handler() {
    # handles the process of sending local state to remote
    # returns 0 if state was sent successfully
    # returns 1 is sending state failed on timeout
    # returns 255 if no SSH connection is present

    local local_postgres_status=$1
    local remote_postgres_status=$2

    local loop_start_time
    
    log_message "Sending detected PostgreSQL state to remote" "NOTICE"
    
    loop_start_time=$(date +"%s")
    while true;
    do
        local share_status_exit_code
        share_state_with_remote "$local_postgres_status" "$remote_postgres_status"
        share_status_exit_code="$?"

        if [[ "$share_status_exit_code" -eq 0 ]]; then
            return 0
        elif [[ "$share_status_exit_code" -eq 255 ]]; then
            log_message "Failed to send detected PostgreSQL state to remote, no SSH connection" "WARNING"
            return 255
        fi

        if [[ $STATE_VERIFICATION_TIMEOUT -ne 0 ]]; then
            if (( $(date +"%s") - loop_start_time >= STATE_VERIFICATION_TIMEOUT )); then
                log_message "Failed to send detected PostgreSQL state to remote" "ERROR"
                return 1
            fi
        fi

        sleep "$STATE_VERIFICATION_RETRY_DELAY"
    done
}

receive_state_handler() {
    # handles the process of receieving state from remote and outputs it
    # returns 0 if state was receieved successfully
    # returns 1 is receieving state failed on timeout

    local loop_start_time

    log_message "Checking remote's PostgreSQL state" "NOTICE"

    loop_start_time=$(date +"%s")
    while true;
    do
        if [[ -e "$LOCK_FILE_DIR"/remote_pg.status ]]; then
            local read_remote_state_result
            local read_remote_state_exit_code
            read_remote_state_result=$(read_remote_state)
            read_remote_state_exit_code="$?"
            
            if [[ "$read_remote_state_exit_code" -eq 0 ]]; then
                echo "$read_remote_state_result"
                return 0
            fi
        fi
        
        if [[ $STATE_VERIFICATION_TIMEOUT -ne 0 ]]; then
            if (( $(date +"%s") - loop_start_time >= STATE_VERIFICATION_TIMEOUT )); then
                log_message "Failed to check PostgreSQL state from remote" "ERROR"
                return 1
            fi
        fi

        sleep "$STATE_VERIFICATION_RETRY_DELAY"
    done
}

negotiate_state(){
    # handles the process of comparing state of PostgreSQL instances between local and remote scripts
    # returns 0 if state is the same on both scripts
    # returns 10 if state is different on local and remote script instance
    # returns 1 is comparing state failed
    # returns 255 if no SSH connection is present
    # script won't be locked on FATAL, the log message is meant for raising awareness of admins

    local local_postgres_status=$1
    local remote_postgres_status=$2

    local local_postgres_status_verification=-1
    local remote_postgres_status_verification=-1

    log_message "Negotiating PostgreSQL instances state with remote" "NOTICE"

    local send_state_handler_exit_code
    send_state_handler "$local_postgres_status" "$remote_postgres_status"
    send_state_handler_exit_code="$?"
    if [[ "$send_state_handler_exit_code" -eq 1 ]]; then
        log_message "Failed to negotiate state with remote" "FATAL"
        return 1
    elif [[ "$send_state_handler_exit_code" -eq 255 ]]; then
        log_message "Failed to negotiate state with remote, no SSH connection present" "WARNING"
        return 255
    fi

    local receive_state_handler_result
    local receive_state_handler_exit_code
    receive_state_handler_result=$(receive_state_handler)
    receive_state_handler_exit_code="$?"
    if [[ "$receive_state_handler_exit_code" -eq 1 ]]; then
        log_message "Failed to negotiate state with remote" "FATAL"
        return 1
    fi

    local IFS=":"
    read -r local_postgres_status_verification remote_postgres_status_verification <<< "$receive_state_handler_result"

    if [[ "$local_postgres_status" -eq "$local_postgres_status_verification" ]]; then
        if [[ "$remote_postgres_status" -eq "$remote_postgres_status_verification" ]]; then
            log_message "Local PostgreSQL state and remote's state are in sync" "NOTICE"
            return 0
        fi
    fi

    log_message "Local PostgreSQL state and remote's state are different" "WARNING"

    return 10
}

block_state_negotiation() {
    log_message "Blocking PostgreSQL instances state negotiation until system is restored" "NOTICE"

    if ! create_file_local "$LOCK_FILE_DIR" "state_negotiaton.block" "blocking state negotiation"; then
        log_message "Failed to block PostgreSQL instances state negotiation" "FATAL"
        return 1
    else
        return 0
    fi
}

unblock_state_negotiation() {
    log_message "Unblocking PostgreSQL instances state negotiation" "NOTICE"

    if ! remove_file_local "$LOCK_FILE_DIR" "state_negotiaton.block" "unblocking state negotiation"; then
        log_message "Failed to unblock PostgreSQL instances state negotiation" "FATAL"
        return 1
    else
        return 0
    fi
}
# ---------------------------------------------------------------------------------------------------------


await_standalone_master_signal() {
    # Waits for a signal from master, that it's rady for asynchronous replication
    # 0 if successful
    # 1 if failed

    if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then
        return 0
    fi

    local check_cycle_clount=0
    local exit_code
    while true; do
        if [[ -e "$LOCK_FILE_DIR"/master_async.signal ]]; then
            rm --force ${LOCK_FILE_DIR:?}/master_async.signal > /dev/null
            log_message "Master is ready for asynchronous replication" "NOTICE"
            exit_code="0"
            break
        fi

        if (( check_cycle_clount * STATE_VERIFICATION_RETRY_DELAY > STATE_VERIFICATION_TIMEOUT)); then
            log_message "Master wasn't ready for asynchronous replication in time" "FATAL"
            exit_code="1"
            break
        fi

        (( check_cycle_clount++ ))
        sleep "$STATE_VERIFICATION_RETRY_DELAY"
    done

    return "$exit_code"    
}

save_async_replication_start() {
    # logs the time, when asynchronous replication started
    # 0 - if successfull
    # 1 - if failed

    log_message "Saving starting time of synchronous replication" "NOTICE"
    date +"%s" > "$LOCK_FILE_DIR"/async.replication
    if ! sync; then
        log_message "Failed to save starting time of synchronous replication (fsync error)" "FATAL"
        return 1
    fi
    return 0
}

clean_async_replication_start() {
    # deletes signal file with the time, when asynchronous replication started
    # 0 - if successfull
    # 1 - if failed

    log_message "Cleaning starting time of asynchronous replication" "NOTICE"
    rm --force "$LOCK_FILE_DIR"/async.replication
    if ! sync; then
        log_message "Failed to clean starting time of synchronous replication (fsync error)" "FATAL"
        return 1
    fi
    return 0
}


# ------------------------------------------------- Locks -------------------------------------------------
create_file_local() {
    local file_dir=$1
    local file_name=$2
    local action_message=$3

    local exit_code
    command_handler "touch $file_dir/$file_name" "$LOCK_RETRIES" "0" "$action_message" > /dev/null
    exit_code="$?"
    if ! sync; then
        return 1
    else
        return "$exit_code"
    fi
}

remove_file_local() {
    local file_dir=$1
    local file_name=$2
    local action_message=$3

    local exit_code
    command_handler "rm --force $file_dir/$file_name" "$UNLOCK_RETRIES" "0" "$action_message" > /dev/null
    exit_code="$?"
    if ! sync; then
        return 1
    else
        return "$exit_code"
    fi
}

create_file_remote() {
    local file_dir=$1
    local file_name=$2
    local action_message=$3
    local exit_code
    remote_command_handler "touch $file_dir/$file_name" "$LOCK_RETRIES" "0" "$action_message" "$SYSTEM_ADMIN_USER" "$CLUSTER_IP_REMOTE" > /dev/null
    exit_code="$?"
    if ! remote_command_handler "sync" "1" "0" "using fsync" "$SYSTEM_ADMIN_USER" "$CLUSTER_IP_REMOTE" > /dev/null; then
        return 1
    else
        return "$exit_code"
    fi
}

lock_by_infinite_loop() {
    # basically stops execution until manual intervention

    log_message "Script is locked in infinite execution (end process manually)" "FATAL"

    while true; 
    do
        sleep 100
    done
}

lock_local_script_execution() {
    if ! create_file_local "$LOCK_FILE_DIR" "script_in_progress.lock" "locking script execution"; then
        log_message "Failed to lock local scipt execution" "ERROR"
        exit 1
    fi
}

unlock_local_script_execution() {
    if ! remove_file_local "$LOCK_FILE_DIR" "active.lock" "unlocking script execution (active action)"; then
        log_message "Failed to unlock script execution (active action) (remove active.lock manually)" "FATAL"
        exit 1
    fi

    if ! remove_file_local "$LOCK_FILE_DIR" "script_in_progress.lock" "unlocking script execution"; then
        log_message "Failed to unlock script execution (remove script_in_progress.lock manually)" "FATAL"
        exit 1
    fi

    dump_current_log
    log_message "" "END"
}

lock_local_script_execution_fatal() {
    local fatal_error_lock_name=$1

    if ! create_file_local "$LOCK_FILE_DIR" "$fatal_error_lock_name" "locking execution due to fatal error"; then
        log_message "Failed to lock scipt execution after FATAL error (end process manually)" "FATAL"
        lock_by_infinite_loop
    fi
}

lock_script_active_action() {
    if ! create_file_local "$LOCK_FILE_DIR" "active.lock" "locking script execution (active action)"; then
        log_message "Failed to lock local scipt execution (active action)" "FATAL"
        lock_local_script_execution_fatal "faile_to_lock_script_local_execution_by_active_action.fatal"
        unlock_local_script_execution
        exit 1
    fi
}
# --------------------------------------------------------------------------------------------------------------

decision_maker() {
    local local_postgres_status
    local local_postgres_redo
    local remote_postgres_status
    local remote_postgres_redo

    local previous_local_postgres_role

    local pg_status_return
    local pg_status_exit_code
    pg_status_return=$(check_postgresql "localhost" "$PGPORT" "$PG_ADMIN_USER" "$SYSTEM_ADMIN_USER" "$CLUSTER_IP_REMOTE")
    pg_status_exit_code="$?"

    if [[ "$pg_status_exit_code" -ne 0 ]]; then
        lock_local_script_execution_fatal "pg_status_check_failed.fatal"
        unlock_local_script_execution
        exit 1
    else
        local IFS=":"
        read -r local_postgres_status local_postgres_redo remote_postgres_status remote_postgres_redo <<< "$pg_status_return"
    fi

    if [[ "$remote_postgres_status" -ne 3 ]]; then
        # ssh connection is present

        if  (( local_postgres_status * remote_postgres_status != 0 )); then
            # both PostgreSQL nodes are running (no action needed)

            if [[ -e "$LOCK_FILE_DIR"/state_negotiaton.block ]]; then
                if ! unblock_state_negotiation; then
                    lock_local_script_execution_fatal "failed_to_unblock_state_negotiation.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
            fi

            log_message "Saving local PostgreSQL instance replication role" "INFO"
            if [[ "$local_postgres_status" -eq 1 ]]; then
                if ! command_handler "echo \"M\" > $LOCK_FILE_DIR/previous_local_cluster_role" "$LOCK_RETRIES" "0" "saving local PostgreSQL role" > /dev/null; then
                    log_message "Failed to save local PostgreSQL instance replication role" "FATAL"
                    lock_local_script_execution_fatal "save_current_pg_role_failed.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
            else
                if ! command_handler "echo \"R\" > $LOCK_FILE_DIR/previous_local_cluster_role" "$LOCK_RETRIES" "0" "saving local PostgreSQL role" > /dev/null; then
                    log_message "Failed to save local PostgreSQL instance replication role" "FATAL"
                    lock_local_script_execution_fatal "save_current_pg_role_failed.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
            fi

            if ! sync; then
                log_message "Failed to save local PostgreSQL instance replication role (fsync failed)" "FATAL"
                lock_local_script_execution_fatal "save_current_pg_role_failed.fatal"
                unlock_local_script_execution
                exit 1
            fi

            if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]] && (( local_postgres_status == 1)) && (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                # SSH interconnect recovered, both nodes are running and local PostgreSQL is Master
                lock_script_active_action

                log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start using replication slot" "NOTICE"
                if ! create_file_remote "$LOCK_FILE_DIR" "use_replication_slot.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start using replication slot"; then
                    log_message "Failed to signal $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start using replication slot" "FATAL"
                    lock_local_script_execution_fatal "failed_to_signal_replication_slot_use.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
                
                log_message "Removing failed SSH interconnect signal" "NOTICE"
                rm --force "$LOCK_FILE_DIR"/ssh_interconnect.lost > /dev/null

                if ! sync; then
                    log_message "Failed to remove lost SSH interconnect signal (fsync failed)" "FATAL"
                    lock_local_script_execution_fatal "lost_interconnect_signal_removel_failed.fatal"
                    unlock_local_script_execution
                    exit 1
                fi

                log_message "locking local script execution (remote lock)" "NOTICE"
                if ! create_file_local "$LOCK_FILE_DIR" "remote.lock" "locking local script execution (remote lock)"; then
                    log_message "Failed to lock local scipt execution (remote lock)" "FATAL"
                    lock_local_script_execution_fatal "failed_to_lock_local_scipt_execution.fatal"
                    unlock_local_script_execution
                    exit 1
                fi

                unlock_local_script_execution
                exit 0
            fi

            if (( PG_ALLOW_STANDALONE_MASTER == 1 )) && [[ -e "$LOCK_FILE_DIR"/async.replication ]] && (( local_postgres_status == 1 )); then
                local immediate_restoration=0
                local successfull_promotion=0
                
                local replication_state
                check_postgresql_replication_state "1"
                replication_state="$?"
                
                if (( replication_state == 0 )); then
                    # asynchronous replica is in streaming state (able to promote replica to synchronous)
                    lock_script_active_action

                    if ! sync_cought_up_async_replica; then
                        immediate_restoration="1"
                    else
                        successfull_promotion="1"
                    fi
                fi

                if ( (( replication_state == 1 )) || (( immediate_restoration == 1 )) ) && (( successfull_promotion != 1 )); then
                    # asynchronous replica is in catching up state (cheching if it has been catching up for too long)
                    if (( $(date +"%s") - $(cat "$LOCK_FILE_DIR"/async.replication) >= PG_CATCHUP_TIMEOUT * 60 )) || (( immediate_restoration == 1 )); then
                        lock_script_active_action

                        if ! sync_lagging_async_replica; then
                            log_message "Failed to promote asynchronous replica to synchronous" "FATAL"
                            lock_local_script_execution_fatal "promoting_aync_replica_to_sync_failed.fatal"
                            unlock_local_script_execution
                            exit 1
                        else
                            successfull_promotion=1
                        fi
                    fi
                fi

                if (( successfull_promotion == 1 )); then
                    log_message "Asynchronous replica is now synchronous" "SUCCESS"
                fi
            fi

            unlock_local_script_execution
            exit 0
        else
            # at lest one node is down

            if ! [[ -e "$LOCK_FILE_DIR"/state_negotiaton.block ]]; then

                local negotiation_exit_code
                negotiate_state "$local_postgres_status" "$remote_postgres_status"
                negotiation_exit_code="$?"

                if [[ "$negotiation_exit_code" -ne 0 ]]; then
                    
                    if ! clean_shared_state; then
                        lock_local_script_execution_fatal "shared_state_cleanup_failed.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    unlock_local_script_execution
                    exit 0
                fi

                if ! clean_shared_state; then
                    lock_local_script_execution_fatal "shared_state_cleanup_failed.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
            fi

            lock_script_active_action

            if [[ "$local_postgres_status" -ne 0 ]]; then
                # remote node is down

                if [[ "$local_postgres_status" -eq 2 ]]; then
                    # remote master is down (action needed) !!!DONE!!!

                    log_message "Remote master on $CLUSTER_NAME_REMOTE is down, latest checkpoint's REDO location: $remote_postgres_redo" "ERROR"
                    
                    log_message "locking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                    if ! create_file_remote "$LOCK_FILE_DIR" "master_down.lock" "locking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
                        log_message "Failed to lock scipt execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "FATAL"
                        lock_local_script_execution_fatal "failed_to_lock_script_execution_on_remote.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    if (( PG_ALLOW_STANDALONE_MASTER == 1)) && [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
                        log_message "Prohibited to promote asynchronous replica" "FATAL"
                        lock_local_script_execution_fatal "async_replica_promote_prohibited.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    if ! promote_local_replica; then
                        lock_local_script_execution_fatal "failed_to_promote_local_replica.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
                        log_message "Removing failed SSH interconnect signal" "NOTICE"
                        rm --force "$LOCK_FILE_DIR"/ssh_interconnect.lost > /dev/null

                        if ! sync; then
                            log_message "Failed to remove lost SSH interconnect signal (fsync failed)" "ERROR"
                            lock_local_script_execution_fatal "failed_to_remove_lost_interconnect_signal.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi
                    fi

                    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start rewind process" "NOTICE"
                    if ! create_file_remote "$LOCK_FILE_DIR" "rewind.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start rewind process"; then
                        log_message "Failed to signal $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start rewind process" "FATAL"
                        lock_local_script_execution_fatal "failed_to_signal_rewind_on_remote.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    log_message "Unlocking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                    if ! create_file_remote "$LOCK_FILE_DIR" "master_down.unlock" "unlocking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
                        log_message "Failed to unlock script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "FATAL"
                        lock_local_script_execution_fatal "failed_to_unlock_script_execution_on_remote.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                        if ! master_replication_check "0"; then
                            lock_local_script_execution_fatal "asyncronous_replication_on_master_didnt_start.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        if ! save_async_replication_start; then
                            lock_local_script_execution_fatal "failed_to_save_starting_time_of_synchronous_replication.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi
                        
                        log_message "Local replica promoted, asyncronous replication started, system restored" "SUCCESS"
                    else
                        if ! master_replication_check "1"; then
                            lock_local_script_execution_fatal "syncronous_replication_on_master_didnt_start.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        log_message "Local replica promoted, syncronous replication started, system restored" "SUCCESS"
                    fi

                    unlock_local_script_execution
                    exit 0
                else
                    # remote replica is down (action needed) !!!DONE!!!

                    log_message "Remote replica on $CLUSTER_IP_REMOTE is down, latest checkpoint's REDO location: $remote_postgres_redo, awaiting recovery" "ERROR"

                    if (( PG_ALLOW_STANDALONE_MASTER == 1)); then
                        
                        if ! [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
                            if ! reconfigure_master_for_standalone_mode; then
                                lock_local_script_execution_fatal "failed_to_reconfigure_master_for_standalone_mode.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi
                        fi

                        if ! create_file_remote "$LOCK_FILE_DIR" "master_async.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE that master is ready for asynchronous replication"; then
                            log_message "Failed to signal $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE that master is ready for asynchronous replication" "FATAL"
                            lock_local_script_execution_fatal "failed_to_signal_master_readyness_on_remote.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        if ! master_replication_check "0"; then
                            lock_local_script_execution_fatal "asyncronous_replication_on_master_didnt_start.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        if ! save_async_replication_start; then
                            lock_local_script_execution_fatal "failed_to_save_starting_time_of_synchronous_replication.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
                            log_message "Removing failed SSH interconnect signal" "NOTICE"
                            rm --force "$LOCK_FILE_DIR"/ssh_interconnect.lost > /dev/null

                            if ! sync; then
                                log_message "Failed to remove lost SSH interconnect signal (fsync failed)" "ERROR"
                                lock_local_script_execution_fatal "failed_to_remove_lost_interconnect_signal.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi
                        fi
                        
                        log_message "Remote replica recovered, asyncronous replication started, system restored" "SUCCESS"
                    else
                        if ! master_replication_check "1"; then
                            lock_local_script_execution_fatal "syncronous_replication_on_master_didnt_start.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi
                        log_message "Remote replica recovered, syncronous replication started, system restored" "SUCCESS"
                    fi

                    unlock_local_script_execution
                    exit 0
                fi
            else

                if [[ "$remote_postgres_status" -eq 0 ]]; then
                    # both nodes are down

                    check_previous_local_cluster_role
                    previous_local_postgres_role="$?"
                    if [[ "$previous_local_postgres_role" -eq 0 ]]; then
                        lock_local_script_execution_fatal "no_previous_local_cluster_role.fatal"
                        unlock_local_script_execution
                        exit 1
                    fi

                    if [[ "$previous_local_postgres_role" -eq 1 ]]; then
                        # both nodes down, local node was master (action needed) !!!DONE!!!

                        log_message "Both nodes are down, local node was in master role. Latest checkpoint's REDO location local: $local_postgres_redo, remote: $remote_postgres_redo" "ERROR"
                        
                        log_message "locking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                        if ! create_file_remote "$LOCK_FILE_DIR" "remote.lock" "locking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
                            log_message "Failed to lock scipt execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "FATAL"
                            lock_local_script_execution_fatal "failed_to_lock_script_execution_on_remote.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        local master_restart_exit_code
                        local_postgresql_control "start" "master"
                        master_restart_exit_code="$?"

                        if [[ "$master_restart_exit_code" -eq 0 ]]; then
                            # local master successfully restarted

                            log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                            if ! create_file_remote "$LOCK_FILE_DIR" "remote.unlock" "unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
                                log_message "Failed to unlock script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "FATAL"
                                lock_local_script_execution_fatal "failed_to_unlock_script_execution_on_remote.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi

                            if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then 
                                if ! master_replication_check "1"; then
                                    lock_local_script_execution_fatal "syncronous_replication_on_master_didnt_start.fatal"
                                    unlock_local_script_execution
                                    exit 1
                                fi

                                log_message "Master recovered, remote replica recovered, system restored" "SUCCESS"
                            else
                                log_message "Awating script recovery cycle" "WARNING"
                            fi

                            unlock_local_script_execution
                            exit 0
                        fi

                        if [[ "$master_restart_exit_code" -ne 0 ]]; then
                            # local master failed to restart

                            log_message "locking local script execution (master down)" "NOTICE"
                            if ! create_file_local "$LOCK_FILE_DIR" "master_down.lock" "locking local script execution (master down)"; then
                                log_message "Failed to lock local scipt execution (master down)" "FATAL"
                                lock_local_script_execution_fatal "failed_to_lock_local_scipt_execution.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi

                            log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restart old replica" "NOTICE"
                            if ! create_file_remote "$LOCK_FILE_DIR" "restart_replica.signal" "signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restart old replica"; then
                                log_message "Failed to signal $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restart old replica" "FATAL"
                                lock_local_script_execution_fatal "failed_to_signal_old_replica_restart_on_remote.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi

                            log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                            if ! create_file_remote "$LOCK_FILE_DIR" "remote.unlock" "unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE"; then
                                log_message "Failed to unlock script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "FATAL"
                                lock_local_script_execution_fatal "failed_to_unlock_script_execution_on_remote.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi

                            log_message "Remote signaled to restart old replica, awaiting rewind signal" "WARNING"

                            unlock_local_script_execution
                            exit 0
                        fi
                    fi
                    
                    if [[ "$previous_local_postgres_role" -eq 2 ]]; then
                        # both nodes down, local node was replica (no action needed) !!!DONE!!!
                        
                        log_message "Both nodes are down, local node was in replica role. Latest checkpoint's REDO location local: $local_postgres_redo, remote: $remote_postgres_redo" "ERROR"
                        
                        if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then
                            if ! block_state_negotiation; then
                                lock_local_script_execution_fatal "failed_to_block_state_negotiation.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi
                        fi

                        log_message "locking local script execution (remote lock)" "NOTICE"
                        if ! create_file_local "$LOCK_FILE_DIR" "remote.lock" "locking local script execution (remote lock)"; then
                            log_message "Failed to lock local scipt execution (remote lock)" "FATAL"
                            lock_local_script_execution_fatal "failed_to_lock_local_scipt_execution.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        log_message "Awaiting unlock" "WARNING"
                        
                        unlock_local_script_execution
                        exit 0
                    fi
                else
                    # local node is down

                    if [[ "$remote_postgres_status" -eq 1 ]]; then

                        check_previous_local_cluster_role
                        previous_local_postgres_role="$?"
                        if [[ "$previous_local_postgres_role" -eq 0 ]]; then
                            lock_local_script_execution_fatal "no_previous_local_cluster_role.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi

                        if [[ "$previous_local_postgres_role" -eq 2 ]]; then
                            # local replica is down (action needed) !!!DONE!!!

                            log_message "Local replica is down, latest chechpoint's REDO location: $local_postgres_redo" "ERROR"
                            
                            if ! await_standalone_master_signal; then
                                lock_local_script_execution_fatal "master_wasnt_ready_for_synchronous_replication.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi    

                            if ! recover_local_replica; then
                                lock_local_script_execution_fatal "failed_to_restore_local_replica.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi 

                            log_message "Local replica recovered successfully, system restored" "SUCCESS"

                            unlock_local_script_execution
                            exit 0
                        fi
                        
                        if [[ "$previous_local_postgres_role" -eq 1 ]]; then
                            # local master is down (no action needed) !!!DONE!!!

                            log_message "Local master is down, latest chechpoint's REDO location: $local_postgres_redo" "ERROR"

                            log_message "locking local script execution (master down)" "NOTICE"
                            if ! create_file_local "$LOCK_FILE_DIR" "master_down.lock" "locking local script execution (master down)"; then
                                log_message "Failed to lock local scipt execution (master down)" "FATAL"
                                lock_local_script_execution_fatal "failed_to_lock_local_scipt_execution.fatal"
                                unlock_local_script_execution
                                exit 1
                            fi
                            
                            log_message "Awaiting rewind signal" "WARNING"

                            unlock_local_script_execution
                            exit 0
                        fi
                    else
                        # local master is down (no action needed) !!!DONE!!!

                        log_message "Local master is down, latest chechpoint's REDO location: $local_postgres_redo" "ERROR"

                        log_message "locking local script execution (master down)" "NOTICE"
                        if ! create_file_local "$LOCK_FILE_DIR" "master_down.lock" "locking local script execution (master down)"; then
                            log_message "Failed to lock local scipt execution (master down)" "FATAL"
                            lock_local_script_execution_fatal "failed_to_lock_local_scipt_execution.fatal"
                            unlock_local_script_execution
                            exit 1
                        fi
                        
                        log_message "Awaiting rewind signal" "WARNING"

                        unlock_local_script_execution
                        exit 0
                    fi
                fi
            fi
        fi
    else
        # no ssh connection
        lock_script_active_action

        if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
            unlock_local_script_execution
            exit 0
        fi

        if ! create_file_local "$LOCK_FILE_DIR" "ssh_interconnect.lost" "Signaling failed SSH interconnect"; then
            log_message "Failed to signal failed SSH interconnect" "FATAL"
            lock_local_script_execution_fatal "failed_to_signal_lost_ssh_interconnect.fatal"
            unlock_local_script_execution
            exit 1
        fi

        if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then
            log_message "Waiting for SSH connection" "WARNING"
            unlock_local_script_execution
            exit 0
        fi

        log_message "SSH interconnect failed" "WARNING"

        if ! [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
            if (( local_postgres_status == 1)); then
                # local node is Master
                if ! reconfigure_master_for_standalone_mode; then
                    lock_local_script_execution_fatal "failed_to_reconfigure_master_for_standalone_mode.fatal"
                    unlock_local_script_execution
                    exit 1
                fi

                if ! save_async_replication_start; then
                    lock_local_script_execution_fatal "failed_to_save_starting_time_of_synchronous_replication.fatal"
                    unlock_local_script_execution
                    exit 1
                fi
            fi
        fi

        unlock_local_script_execution
        exit 0
    fi
}

# exit normally if another script is running
if [[ -e "$LOCK_FILE_DIR"/script_in_progress.lock ]]; then
    exit 0
fi

# exit with error if encountered FATAL error previousely
if find "$LOCK_FILE_DIR" -type f -name "*.fatal" | grep -q .; then
    exit 1
fi

# if unlock signal is present
if find "$LOCK_FILE_DIR" -type f -name "*.unlock" | grep -q .; then
    lock_local_script_execution
    lock_script_active_action

    lock_name=""
    lock_name=$(basename -- "$(find "$LOCK_FILE_DIR" -type f -name "*.unlock" | head -n 1)" .unlock)

    log_message "Unlock signal found, removing $lock_name.lock" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "$lock_name.lock" "unlocking local script execution ($lock_name.unlock)"; then
        log_message "Failed to unlock script execution ($lock_name.unlock)" "FATAL"
        lock_local_script_execution_fatal "failed_to_unlock_local_script_execution_by_signal.fatal"
        exit 1
    fi

    log_message "Cleaning up after unlocking, removing $lock_name.unlock" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "$lock_name.unlock" "removing $lock_name.unlock"; then
        log_message "Failed to remove $lock_name.unlock" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_unlock_signal.fatal"
        exit 1
    fi

    unlock_local_script_execution
    exit 0
fi

# exit normally if any other lock is present
if find "$LOCK_FILE_DIR" -type f -name "*.lock" | grep -q .; then
    exit 0
fi

# if signaled to reattach old master as replica
if [[ -e "$LOCK_FILE_DIR"/rewind.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "rewind.signal found, starting in master reattaching mode" "NOTICE"

    log_message "Blocking keepalived VIP" "NOTICE"
    if ! create_file_local "$KEEPALIVED_DIR" "no_vip.block" "blocking vip"; then
        log_message "Failed to block keepalived VIP" "FATAL"
        lock_local_script_execution_fatal "failed_to_block_vip.fatal"
        unlock_local_script_execution
        exit 1
    fi

    if ! reattach_master_as_replica; then
        lock_local_script_execution_fatal "failed_to_reattach_old_master_as_replica.fatal"

        log_message "Removing rewind.signal" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "rewind.signal" "removing signal"; then
            log_message "Failed to remove rewind.signal" "FATAL"
            lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
            unlock_local_script_execution
            exit 1
        fi

        log_message "Unlocking local script execution (master down)" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "master_down.lock" "unlocking local script execution (master down)"; then
            log_message "Failed to unlock local scipt execution (master down)" "FATAL"
            lock_local_script_execution_fatal "failed_to_unlock_local_scipt_execution.fatal"
            unlock_local_script_execution
            exit 1
        fi

        log_message "Unblocking keepalived VIP" "NOTICE"
        if ! remove_file_local "$KEEPALIVED_DIR" "no_vip.block" "unblocking vip"; then
            log_message "Failed to unblock keepalived VIP" "FATAL"
            lock_local_script_execution_fatal "failed_to_unblock_vip.fatal"
            unlock_local_script_execution
            exit 1
        fi

        unlock_local_script_execution
        exit 1
    fi

    log_message "Removing rewind.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "rewind.signal" "removing signal"; then
        log_message "Failed to remove rewind.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Unlocking local script execution (master down)" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "master_down.lock" "unlocking local script execution (master down)"; then
        log_message "Failed to unlock local scipt execution (master down)" "FATAL"
        lock_local_script_execution_fatal "failed_to_unlock_local_scipt_execution.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Ublocking keepalived VIP" "NOTICE"
    if ! remove_file_local "$KEEPALIVED_DIR" "no_vip.block" "unblocking vip"; then
        log_message "Failed to unblock keepalived VIP" "FATAL"
        lock_local_script_execution_fatal "failed_to_unblock_vip.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully reattached old master as a replica, system restored" "SUCCESS"

    unlock_local_script_execution
    exit 0
fi

# if signaled to restart replica
if [[ -e "$LOCK_FILE_DIR"/restart_replica.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "restart_replica.signal found, starting in old replica restarting mode" "NOTICE"

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to restart old replica" "FATAL"
        lock_local_script_execution_fatal "failed_to_restart_old_replica.fatal"

        log_message "Removing restart_replica.signal" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "restart_replica.signal" "removing signal"; then
            log_message "Failed to remove restart_replica.signal" "FATAL"
            lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
            unlock_local_script_execution
            exit 1
        fi

        unlock_local_script_execution
        exit 1
    fi

    log_message "Removing restart_replica.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "restart_replica.signal" "removing signal"; then
        log_message "Failed to remove restart_replica.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    if ! block_state_negotiation; then
        lock_local_script_execution_fatal "failed_to_block_state_negotiation.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully restarted old replica, awaiting replica promotion" "WARNING"

    unlock_local_script_execution
    exit 0
fi

if [[ -e "$LOCK_FILE_DIR"/prepare_transition.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "prepare_transition.signal found, preparing async replica to trasition to synchronous" "NOTICE"

    if ! prepare_replica_for_sync_transition; then
        log_message "Failed to prepare async replica to trasition to synchronous" "WARNING"
    fi

    log_message "Removing prepare_transition.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "prepare_transition.signal" "removing signal"; then
        log_message "Failed to remove prepare_transition.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully prepared async replica to trasition to synchronous" "NOTICE"

    unlock_local_script_execution
    exit 0
fi

if [[ -e "$LOCK_FILE_DIR"/restore_catchup.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "restore_catchup.signal found, restoring replica to catch up to master" "NOTICE"

    if ! restore_as_replcia_to_catch_up; then
        log_message "Failed to restore replica to catch up to master" "FATAL"
        lock_local_script_execution_fatal "failed_to_restore_catch_up_replica.fatal"

        log_message "Removing restore_catchup.signal" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "restore_catchup.signal" "removing signal"; then
            log_message "Failed to remove restore_catchup.signal" "FATAL"
            lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
            unlock_local_script_execution
            exit 1
        fi

        unlock_local_script_execution
        exit 1
    fi

    log_message "Removing restore_catchup.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "restore_catchup.signal" "removing signal"; then
        log_message "Failed to remove restore_catchup.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully restored replica to catch up to master" "NOTICE"

    unlock_local_script_execution
    exit 0
fi

if [[ -e "$LOCK_FILE_DIR"/clean_replication_start.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "clean_replication_start.signal found, cleaning local starting time of synchronous replication" "NOTICE"

    if ! clean_async_replication_start; then
        log_message "Failed to clean local starting time of synchronous replication" "FATAL"
        lock_local_script_execution_fatal "failed_to_clean_async_start_time.fatal"

        log_message "Removing clean_replication_start.signal" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "clean_replication_start.signal" "removing signal"; then
            log_message "Failed to remove clean_replication_start.signal" "FATAL"
            lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
            unlock_local_script_execution
            exit 1
        fi

        unlock_local_script_execution
        exit 1
    fi

    log_message "Removing clean_replication_start.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "clean_replication_start.signal" "removing signal"; then
        log_message "Failed to remove clean_replication_start.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully cleaned local starting time of asynchronous replication" "NOTICE"

    unlock_local_script_execution
    exit 0
fi

if [[ -e "$LOCK_FILE_DIR"/use_replication_slot.signal ]]; then

    lock_local_script_execution
    lock_script_active_action

    log_message "use_replication_slot.signal found, reconfiguring replica to use replication slot" "NOTICE"

    if ! reconfigure_replica_for_slot_use; then
        log_message "Failed to reconfigure replica to use replication slot" "FATAL"
        lock_local_script_execution_fatal "replica_failed_to_use_replication_slot.fatal"

        log_message "Removing use_replication_slot.signal" "NOTICE"
        if ! remove_file_local "$LOCK_FILE_DIR" "use_replication_slot.signal" "removing signal"; then
            log_message "Failed to remove use_replication_slot.signal" "FATAL"
            lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
            unlock_local_script_execution
            exit 1
        fi

        unlock_local_script_execution
        exit 0
    fi

    log_message "Removing use_replication_slot.signal" "NOTICE"
    if ! remove_file_local "$LOCK_FILE_DIR" "use_replication_slot.signal" "removing signal"; then
        log_message "Failed to remove use_replication_slot.signal" "FATAL"
        lock_local_script_execution_fatal "failed_to_remove_signal.fatal"
        unlock_local_script_execution
        exit 1
    fi

    log_message "Successfully reconfigured replica to use replication slot" "NOTICE"

    unlock_local_script_execution
    exit 0
fi

# normal execution
lock_local_script_execution
decision_maker
