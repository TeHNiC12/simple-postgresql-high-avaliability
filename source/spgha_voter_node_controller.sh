#!/bin/bash
set +m

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
CLUSTER_NAME_LOCAL="pg_ha_voter"
CLUSTER_NAME_NODE_1="pg_ha_1"
CLUSTER_IP_NODE_1="10.7.2.92"
CLUSTER_NAME_NODE_2="pg_ha_2"
CLUSTER_IP_NODE_2="10.7.2.93"
PGPORT="5432"

#-----------------------------Retries & Timeouts---------------------------#
# retries = 1 - disables retries
# timeout = 0 - disables timeout

SSH_CONNECT_TIMEOUT=1
SSH_TCP_KEEPALIVE="yes"
SSH_SERVER_ALIVE_INTERVAL=1
SSH_SERVER_ALIVE_COUNT_MAX=1

export PGCONNECT_TIMEOUT=1

PG_STATUS_CHECK_RETRIES=3               
PG_STATUS_CHECK_TIMEOUT=6

PG_REPLCATION_CHECK_RETRY_DELAY=5
PG_REPLICATION_CHECK_TIMEOUT=240

PG_BASEBACKUP_RESTORE_TIMEOUT=120

PG_START_TIMEOUT=120

PG_STOP_TIMEOUT=120

PG_REWIND_TIMEOUT=120

STANDALONE_WAIT_TIMEOUT=60

STATE_VERIFICATION_TIMEOUT=120       # timeout for retrieving remote's statuses for verification
                                    # 0 - disables timeout
STATE_VERIFICATION_RETRY_DELAY=5    # delay between state verification attempts

RETRY_DELAY=3                       # time in seconds between command reties

PG_BACKGROUND_CHECK_SLEEP=0.3
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

SYNC_FILE_SUPPORTED=0               # Determines if file should me specified when using sync if applicable
                                    # May not me avaliable on all systems
                                    # 1 - sync specific file
                                    # 0 - sync whole FS
#==========================================================================#

TERMINATE_REQUESTED=0
declare -a MONITORING_PIDS=()

#=================================LOGGING==================================#

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

# Writes a log message to logfile and journald
# Usage: log_message <message> <log level>
# $1: a message to log
# $2: logging level to use (use the capital letter ones)
# Logging levels:
#  -1 - SUCCESS
#   0 - EMERGERNCY - Emergency
#   1 - ALERT - Alert
#   2 - FATAL - Critical
#   3 - ERROR - Error
#   4 - WARNING - Warning
#   5 - NOTICE - Notice
#   6 - INFO - Informational
#   7 - DEBUG - Debug
# Returns:  0 - always
log_message() {
    local message="$1"
    local log_level="$2"

    if (( $# != 2 )); then
        log_message "log_message failed - missing argument" "ERROR"
        return 0
    fi

    local timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S.%3N")
    
    local priority
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

        # if ! [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
        #     timeout -s SIGILL 0.1 echo -e "[Node: $CLUSTER_NAME_LOCAL] [$timestamp] [$log_level]  $message" >> /pgpro-ent-16/pgbackup/"$CLUSTER_NAME_LOCAL"_failover.log ## For debug purprouses only !!!REMOVE!!!
        # fi
    fi

    if (( priority <= JOURNAL_LOG_LEVEL )); then
        if (( LOG_TO_JOURNAL == 1 )); then
            if (( priority == -1 )); then
                priority=5
            fi
            echo -e  "$message" | systemd-cat --priority="$priority" --identifier="pg-ha-controller"
        fi
    fi

    return 0
}

# Dumps current session log into .fatal file
# Usage: dump_current_log
# Returns:  0 - always
dump_current_log(){
    if find "$LOCK_FILE_DIR" -type f -name "*.fatal" | grep -q .; then
        local lock_file_name
        lock_file_name=$(basename -- "$(find "$LOCK_FILE_DIR" -type f -name "*.fatal" | head -n 1)" .fatal)

        command_handler "cat $LOCK_FILE_DIR/current.log.temp > $LOCK_FILE_DIR/$lock_file_name.fatal" "1" "0" "dumping current log" > /dev/null 2>&1
    fi

    file_remove "$LOCK_FILE_DIR" "current.log.temp"

    return 0
}
#==========================================================================#


#=============================COMMAND HANDLERS=============================#

# Execution handler for cmd utilities: supports retries, timeouts and error logging
# Usage: command_handler <command> <No. of retries> <timeout> <short decription for command> <disable logging (optional)>
#   $1: command to execute
#   $2: nomber of retries
#   $3: timeout for the command
#   $4: short message to display in logs
#   $5: 1 disables logging (an optional argument)
# Returns:  0 - if command executed successfully
#           124 - if command timed out
#           command's exit code - if command failed for some other reason
# Output:   result of command execution / error message
# !!!REWRITE!!!     I probably don't need retries, just timeout and error logging
command_handler() {
    local command="$1"
    local retries="$2"          # Number of retries
    local timeout="$3"
    local command_message="$4"
    local disable_logging="0"

    if [[ "$#" -eq 5 ]]; then
        disable_logging="$5"
    fi


    local attempt=1
    local exit_code
    local result

    if (( disable_logging == 0 )); then
        log_message "   $command_message locally" "DEBUG"
    fi

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
            if (( disable_logging == 0 )); then
                log_message "       attempt $attempt/$retries" "DEBUG"
            fi

            if [[ "$timeout" -eq 0 ]]; then
                result=$(bash -c "$command" 2>&1)
                exit_code="$?"
            else
                result=$(timeout "$timeout" bash -c "$command" 2>&1)
                exit_code="$?"
            fi

            if [[ "$exit_code" -eq 0 ]]; then
                if (( disable_logging == 0 )); then
                    log_message "       running command succeded on attempt $attempt" "DEBUG"
                fi
                break
            elif [[ "$exit_code" -eq 124 ]]; then
                if (( disable_logging == 0 )); then
                    log_message "       attempt $attempt of $command_message timed out after $timeout seconds" "WARNING"
                fi
            else
                if (( disable_logging == 0 )); then
                    log_message "       attempt $attempt of $command_message failed with error: \n$result" "WARNING"
                fi
            fi

            if (( attempt < retries )); then
                if (( disable_logging == 0 )); then
                    log_message "       retrying in $RETRY_DELAY s" "DEBUG"
                fi
                sleep "$RETRY_DELAY"
            fi

            ((attempt++))
        done
    fi

    if [[ "$exit_code" -eq 0 ]]; then
        if (( disable_logging == 0 )); then
            log_message "   $command_message locally, succeded" "DEBUG"
        fi
        echo "$result"
        return "$exit_code"
    elif [[ "$exit_code" -eq 124 ]]; then
        if (( disable_logging == 0 )); then
            log_message "   $command_message locally, failed due to timeout" "ERROR"
        fi
        return "$exit_code"
    else
        if (( disable_logging == 0 )); then
            log_message "   $command_message locally, failed due to error: \n$result" "ERROR"
        fi
        echo "$result"
        return "$exit_code"
    fi
}

# Remote execution handler for cmd utilities: supports retries, timeouts and error logging
# Usage: command_handler <command> <No. of retries> <timeout> <short decription for command> <remote host user> <remote host ip> <disable logging (optional)>
#   $1: command to execute
#   $2: nomber of retries
#   $3: timeout for the command
#   $4: short message to display in logs
#   $5: remote system's user
#   $6: remote system's ip
#   $7: 1 disables logging (an optional argument)
# Returns:  0 - if remote command executed successfully
#           124 - if remote command timed out
#           255 - if encountered ssh error
#           command's exit code - if command failed for some other reason
# Output:   result of remote command execution / error message
# !!!REWRITE!!!     I probably don't need retries, just timeout and error logging
remote_command_handler() {
    local command="$1"
    local retries="$2"
    local timeout="$3"
    local command_message="$4"
    local user="$5"
    local host="$6"
    local disable_logging="0"

    if [[ "$#" -eq 7 ]]; then
        disable_logging="$7"
    fi

    local attempt=1
    local exit_code
    local result

    if (( disable_logging == 0 )); then
        log_message "   $command_message remotely on $user@$host" "DEBUG"
    fi

    if [[ "$retries" -eq 1 ]]; then
        if [[ "$timeout" -eq 0 ]]; then
            result=$(ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" -o ServerAliveInterval="$SSH_SERVER_ALIVE_INTERVAL" -o ServerAliveCountMax="$SSH_SERVER_ALIVE_COUNT_MAX" -o TCPKeepAlive="$SSH_TCP_KEEPALIVE" "$user"@"$host" "$command" 2>&1)
            exit_code="$?"
        else
            result=$(timeout "$timeout" ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" -o ServerAliveInterval="$SSH_SERVER_ALIVE_INTERVAL" -o ServerAliveCountMax="$SSH_SERVER_ALIVE_COUNT_MAX" -o TCPKeepAlive="$SSH_TCP_KEEPALIVE" "$user"@"$host" "$command" 2>&1)
            exit_code="$?"
        fi
    else
        while (( attempt <= retries )); do
            if (( disable_logging == 0 )); then
                log_message "       attempt $attempt/$retries" "DEBUG"
            fi

            if [[ "$timeout" -eq 0 ]]; then
                result=$(ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" -o ServerAliveInterval="$SSH_SERVER_ALIVE_INTERVAL" -o ServerAliveCountMax="$SSH_SERVER_ALIVE_COUNT_MAX" -o TCPKeepAlive="$SSH_TCP_KEEPALIVE" "$user"@"$host" "$command" 2>&1)
                exit_code="$?"
            else
                result=$(timeout "$timeout" ssh -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" -o ServerAliveInterval="$SSH_SERVER_ALIVE_INTERVAL" -o ServerAliveCountMax="$SSH_SERVER_ALIVE_COUNT_MAX" -o TCPKeepAlive="$SSH_TCP_KEEPALIVE" "$user"@"$host" "$command" 2>&1)
                exit_code="$?"
            fi

            if [[ "$exit_code" -eq 0 ]]; then
                if (( disable_logging == 0 )); then
                    log_message "       running command succeded on attempt $attempt" "DEBUG"
                fi
                break
            elif [[ "$exit_code" -eq 124 ]]; then
                if (( disable_logging == 0 )); then
                    log_message "       attempt $attempt of $command_message timed out after $timeout seconds" "WARNING"
                fi
            elif [[ "$exit_code" -eq 255 ]]; then
                if (( disable_logging == 0 )); then
                    log_message "       attempt $attempt of $command_message failed with SSH error: \n$result" "WARNING"
                fi
            else
                if (( disable_logging == 0 )); then
                    log_message "       attempt $attempt of $command_message failed with error: \n$result" "WARNING"
                fi
            fi

            if (( attempt < retries )); then
                if (( disable_logging == 0 )); then
                    log_message "       retrying in $RETRY_DELAY s" "DEBUG"
                fi
                sleep "$RETRY_DELAY"
            fi

            ((attempt++))
        done
    fi

    if [[ "$exit_code" -eq 0 ]]; then
        if (( disable_logging == 0 )); then
            log_message "   $command_message remotely on $user@$host, succeded" "DEBUG"
        fi
        echo "$result"
        return "$exit_code"
    elif [[ "$exit_code" -eq 124 ]]; then
        if (( disable_logging == 0 )); then
            log_message "   $command_message remotely on $user@$host, failed due to timeout" "ERROR"
        fi
        return "$exit_code"
    elif [[ "$exit_code" -eq 255 ]]; then
        if (( disable_logging == 0 )); then
            log_message "   $command_message remotely on $user@$host, failed due to SSH error: \n$result" "ERROR"
        fi
        echo "$result"
        return "$exit_code"
    else
        if (( disable_logging == 0 )); then
            log_message "   $command_message remotely on $user@$host, failed due to error: \n$result" "ERROR"
        fi
        echo "$result"
        return "$exit_code"
    fi
}
#==========================================================================#


#=============================FILE SYSTEM UTILS============================#

# Creates a file and syncs it's FS cache
# Usage: file_create <directory> <file name>
#   $1: file directory
#   $2: file name
# Returns:  0 - if file was successfully created and synched
#           1 - if failed to create a file
file_create() {
    local file_dir="$1"
    local file_name="$2"

    if (( $# != 2 )); then
        log_message "file_create failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Creating file $file_dir/$file_name" "DEBUG"

    if ! [[ -e "$file_dir" ]]; then
        log_message "file_create failed - the directory $file_dir doesn't exist" "ERROR"
        return 1
    fi

    if ! command_handler "touch $file_dir/$file_name" "1" "0" "creating file with touch" > /dev/null 2>&1; then
        log_message "file_create failed - unable to touch $file_dir/$file_name" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! sync "$file_dir"/"$file_name" > /dev/null 2>&1; then
            log_message "file_create failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    else
        if ! sync > /dev/null 2>&1; then
            log_message "file_create failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully created file $file_dir/$file_name" "DEBUG"
    return 0
}

# Creates a file on remote host and syncs it's FS cache
# Usage: file_create_remote <directory> <file name> <remote host's ip>
#   $1: file directory
#   $2: file name
#   $3: remote system's ip
# Returns:  0 - if file was successfully created and synched
#           1 - if failed to create a file
file_create_remote() {
    local file_dir="$1"
    local file_name="$2"
    local remote_host_ip="$3"

    if (( $# != 3 )); then
        log_message "file_create_remote failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Creating file $file_dir/$file_name on $remote_host_ip" "DEBUG"

    if ! remote_command_handler "[[ -e \"$file_dir\" ]]" "1" "0" "checking directory presence" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
        log_message "file_create_remote failed - the directory $file_dir doesn't exist on $remote_host_ip" "ERROR"
        return 1
    fi

    if ! remote_command_handler "touch $file_dir/$file_name" "1" "0" "creating file with touch" "$SYSTEM_ADMIN_USER" "$remote_host_ip" > /dev/null 2>&1; then
        log_message "file_create_remote failed - unable to touch $file_dir/$file_name on $remote_host_ip" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! remote_command_handler "sync $file_dir/$file_name" "1" "0" "synching FS cache" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
            log_message "file_create_remote failed - unable to sync FS cache on $remote_host_ip" "ERROR"
            return 1
        fi
    else
        if ! remote_command_handler "sync" "1" "0" "synching FS cache" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
            log_message "file_create_remote failed - unable to sync FS cache on $remote_host_ip" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully created file $file_dir/$file_name on $remote_host_ip" "DEBUG"
    return 0
}

# Removes a file and syncs FS cache
# Usage: file_remove <directory> <file name>
#   $1: file directory
#   $2: file name
# Returns:  0 - if file was successfully removed and FS synched
#           1 - if failed to remove a file
file_remove() {
    local file_dir="$1"
    local file_name="$2"

   if (( $# != 2 )); then
        log_message "file_remove failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Removing file $file_dir/$file_name" "DEBUG"

    if ! command_handler "rm --force $file_dir/$file_name" "1" "0" "removing file with rm" > /dev/null 2>&1; then
        log_message "file_remove failed - unable to rm $file_dir/$file_name" "ERROR"
        return 1
    fi

    if ! sync > /dev/null 2>&1; then
        log_message "file_remove failed - unable to sync FS cache" "ERROR"
        return 1
    fi

    log_message "Successfully removed file $file_dir/$file_name" "DEBUG"
    return 0
}

# Rewrites a file with one string and syncs it's FS cache
# Usage: file_rewrite <directory> <file name> <string to write>
#   $1: file directory
#   $2: file name
#   $3: string to write to the file
# Returns:  0 - if file was successfully rewritten and synched
#           1 - if failed to rewrite a file
file_rewrite() {
    local file_dir="$1"
    local file_name="$2"
    local string_to_write="$3"

    if (( $# != 3 )); then
        log_message "file_rewrite failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Rewriting file $file_dir/$file_name" "DEBUG"

    if ! [[ -e "$file_dir" ]]; then
        log_message "file_rewrite failed - the directory $file_dir doesn't exist" "ERROR"
        return 1
    fi

    if ! command_handler "echo \"$string_to_write\" > $file_dir/$file_name" "1" "0" "writing to file" > /dev/null 2>&1; then
        log_message "file_rewrite failed - unable write to $file_dir/$file_name" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! sync "$file_dir"/"$file_name" > /dev/null 2>&1; then
            log_message "file_rewrite failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    else
        if ! sync > /dev/null 2>&1; then
            log_message "file_rewrite failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully rewritten file $file_dir/$file_name" "DEBUG"
    return 0
}

# Rewrites a file with one string on remote host and syncs it's FS cache
# Usage: file_rewrite_remote <directory> <file name> <string to write> <remote host's ip>
#   $1: file directory
#   $2: file name
#   $3: string to write to the file
#   $4: remote system's ip
# Returns:  0 - if file was successfully rewritten and synched
#           1 - if failed to rewrite a file  
file_rewrite_remote() {
    local file_dir="$1"
    local file_name="$2"
    local string_to_write="$3"
    local remote_host_ip="$4"

    if (( $# != 4 )); then
        log_message "file_rewrite_remote failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Rewriting file $file_dir/$file_name on $remote_host_ip" "DEBUG"

    if ! remote_command_handler "[[ -e \"$file_dir\" ]]" "1" "0" "checking directory presence" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
        log_message "file_rewrite_remote failed - the directory $file_dir doesn't exist on $remote_host_ip" "ERROR"
        return 1
    fi

    if ! remote_command_handler "echo \"$string_to_write\" > $file_dir/$file_name" "1" "0" "writing to file" "$SYSTEM_ADMIN_USER" "$remote_host_ip" > /dev/null 2>&1; then
        log_message "file_rewrite_remote failed - unable write to $file_dir/$file_name" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! remote_command_handler "sync $file_dir/$file_name" "1" "0" "synching FS cache" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
            log_message "file_rewrite_remote failed - unable to sync FS cache on $remote_host_ip" "ERROR"
            return 1
        fi
    else
        if ! remote_command_handler "sync" "1" "0" "synching FS cache" "$SYSTEM_ADMIN_USER" "$remote_host_ip" "1" > /dev/null 2>&1; then
            log_message "file_rewrite_remote failed - unable to sync FS cache on $remote_host_ip" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully rewritten file $file_dir/$file_name on $remote_host_ip" "DEBUG"
    return 0
}

# Appends a string to a file and syncs it's FS cache
# Usage: file_append <directory> <file name> <string to append>
#   $1: file directory
#   $2: file name
#   $3: string to appemd to the file
# Returns:  0 - if file was successfully appended and synched
#           1 - if failed to append to a file        
file_append() {
    local file_dir="$1"
    local file_name="$2"
    local string_to_append="$3"

    if (( $# != 3 )); then
        log_message "file_append failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Appending file $file_dir/$file_name" "DEBUG"

    if ! [[ -e "$file_dir/$file_name" ]]; then
        log_message "file_append failed - the file $file_dir/$file_name doesn't exist" "ERROR"
        return 1
    fi

    if ! command_handler "echo \"$string_to_append\" >> $file_dir/$file_name" "1" "0" "writing to file" > /dev/null 2>&1; then
        log_message "file_append failed - unable write to $file_dir/$file_name" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! sync "$file_dir"/"$file_name" > /dev/null 2>&1; then
            log_message "file_append failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    else
        if ! sync > /dev/null 2>&1; then
            log_message "file_append failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully appended to a file $file_dir/$file_name" "DEBUG"
    return 0
}

# Modifies access rights and owners of a file and syncs it's FS cache
# Usage: file_modify_meta <directory> <file name> <access rights> <owner> <group>
#   $1: file directory
#   $2: file name
#   $3: access rights
#   $4: onwer
#   $5: group
# Returns:  0 - if file was successfully modified and synched
#           1 - if failed to modify a file
file_modify_meta() {
    local file_dir="$1"
    local file_name="$2"
    local access_rights="$3"
    local owner="$4"
    local group="$5"

    if (( $# != 5 )); then
        log_message "file_modify_meta failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Modifying file $file_dir/$file_name to $access_rights $owner:$group" "DEBUG"

    if ! [[ -e "$file_dir/$file_name" ]]; then
        log_message "file_modify_meta failed - the file $file_dir/$file_name doesn't exist" "ERROR"
        return 1
    fi

    if ! command_handler "chmod $access_rights $file_dir/$file_name" "1" "0" "modifying file with chmod" > /dev/null 2>&1; then
        log_message "file_modify_meta failed - unable to chmod $file_dir/$file_name" "ERROR"
        return 1
    fi

    if ! command_handler "chown $owner:$group $file_dir/$file_name" "1" "0" "modifying file with chown" > /dev/null 2>&1; then
        log_message "file_modify_meta failed - unable to chown $file_dir/$file_name" "ERROR"
        return 1
    fi

    if (( SYNC_FILE_SUPPORTED == 1)); then
        if ! sync "$file_dir"/"$file_name" > /dev/null 2>&1; then
            log_message "file_modify_meta failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    else
        if ! sync > /dev/null 2>&1; then
            log_message "file_modify_meta failed - unable to sync FS cache" "ERROR"
            return 1
        fi
    fi

    log_message "Successfully modified file $file_dir/$file_name to $access_rights $owner:$group" "DEBUG"
    return 0
}

# Monitors a directory for a file to be created
# Usage: file_monitor <directory> <file name> <timeout>
#   $1: file directory
#   $2: file name
#   $3: file wait timeout in seconds (0 disables timeout)
# Returns:  0 - file was created
#           1 - some error occured during monitoring
#           124 - file wasn't created during timeout period
file_monitor() {
    local file_dir="$1"
    local file_name="$2"
    local timeout="$3"

    if (( $# != 3 )); then
        log_message "file_monitor failed - missing argument" "ERROR"
        return 1
    fi

    if ! [[ -e "$file_dir" ]]; then
        log_message "file_monitor failed - target directory $file_dir doesn't exist" "ERROR"
        return 1
    fi

    log_message "Starting background monitoring for FS event 'create' in directory $file_dir" "DEBUG"
    if (( timeout == 0 )); then
        inotifywait --event create "$file_dir" > /dev/null 2>&1 &
    else
        inotifywait --timeout "$timeout" --event create "$file_dir" > /dev/null 2>&1 &
    fi

    local background_monitor_pid="$!"

    if [[ -e "$file_dir"/"$file_name" ]]; then
        log_message "Target file $file_dir/$file_name was created" "DEBUG"
        kill -s KILL "$background_monitor_pid" > /dev/null 2>&1
        return 0
    fi

    wait "$background_monitor_pid"
    local exit_code="$?"
    
    if (( exit_code == 0 )); then
        if [[ -e "$file_dir"/"$file_name" ]]; then
            log_message "Target file $file_dir/$file_name was created" "DEBUG"
            return 0
        else
            log_message "File in target directory $file_dir was created, but not the target file $file_name" "ERROR"
            return 1
        fi
    fi

    if (( exit_code == 1 )); then
        log_message "An error occured in inotifywait utility" "ERROR"
        return 1
    fi

    log_message "Target file $file_dir/$file_name wasn't created within the timeout period" "ERROR"
    return 124
}

# Removes a directory and syncs FS cache
# Usage: directory_remove <directory>
#   $1: file directory
# Returns:  0 - if directory was successfully removed and FS synched
#           1 - if failed to remove a directory
directory_remove() {
    local directory_path="$1"

    if (( $# != 1 )); then
        log_message "directory_remove failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Removing directory $directory_path" "DEBUG"

    if ! command_handler "rm --recursive --force $directory_path" "1" "0" "removing directory with rm" > /dev/null 2>&1; then
        log_message "directory_remove failed - unable to rm $directory_path" "ERROR"
        return 1
    fi

    if ! sync > /dev/null 2>&1; then
        log_message "directory_remove failed - unable to sync FS cache" "ERROR"
        return 1
    fi

    log_message "Successfully removed directory $directory_path" "DEBUG"
    return 0
}

# Archives local PostgreSQL instances logs to a separate folder before destructive actions
# Usage: archive_local_postgresql_logs
# Returns:  0 - if PostgreSQL instances logs were archived successfylly
#           1 - if failed to archive logs
archive_local_postgresql_logs() {
    if (( PG_LOG_ACHIVING == 0 )); then
        return 0
    fi

    log_message "Archiving local PostgreSQL logs" "NOTICE"

    if ! command_handler "tar -czvf $PG_LOG_ARCIVE_DIR/pg_logs_$(date +"%Y-%m-%d_%H:%M:%S_%3N").tar.gz -C $PGDATA/ $PGLOG/" "1" "0" "archiving logs" > /dev/null 2>&1; then
        log_message "archive_local_postgresql_logs failed - unable to archive PostgreSQL log directory" "WARNING"
        return 1
    fi

    log_message "Succesfully archived PostgreSQL log directory to:  $PG_LOG_ARCIVE_DIR/pg_logs_$(date +"%Y-%m-%d_%H:%M:%S_%3N").tar.gz" "NOTICE"
    return 0
}
#==========================================================================#


#==============================LOCKS & SIGNALS=============================#

# Stops script execution by traping it in an infinite loop
# Usage: lock_by_infinite_loop
lock_by_infinite_loop() {
    log_message "Script is locked in infinite execution (end process manually)" "FATAL"

    while true;
    do
        sleep 1000
    done
}

# Removes general script execution lock
# Usage: unlock_local_script_execution
unlock_local_script_execution() {
    if ! file_remove "$LOCK_FILE_DIR" "active.lock"; then
        log_message "Failed to unlock script execution (active action) (remove active.lock manually)" "FATAL"
        exit 1
    fi

    dump_current_log
    log_message "" "END"
}

# Completely stops script execution on FATAL ERROR
# Usage: exit_fatal <fatal lock name> <fatal error message>
#   $1: name of the .fatal lock file (example.fatal)
#   $2: fatal error message
exit_fatal() {
    local fatal_error_lock_name="$1"
    local fatal_error_message="$2"

    log_message "$fatal_error_message" "FATAL"

    if ! file_create "$LOCK_FILE_DIR" "$fatal_error_lock_name"; then
        log_message "Failed to lock scipt execution after FATAL error (end process manually)" "FATAL"
        lock_by_infinite_loop
    fi

    unlock_local_script_execution
    exit 1
}

# Locks script execution during active recovery actions, so the in case of script restarting it wouldn't retry to repair PostgreSQL again (incorrectly)
# Usage: lock_script_active_action
lock_script_active_action() {
    if ! file_create "$LOCK_FILE_DIR" "active.lock"; then
        exit_fatal "failed_to_lock_script_local_execution_by_active_action.fatal" "Failed to lock local scipt execution (active action)"
    fi
}

# Creates a signal file on remote host
# Usage: signal_to_remote <signal name> <remote host ip>
#   $1: name of the signal file (example.signal)
#   $2: remote system's ip
signal_to_remote() {
    local signal_name="$1"
    local remote_host_ip="$2"

    if ! file_create_remote "$LOCK_FILE_DIR" "$signal_name" "$remote_host_ip"; then
        exit_fatal "failed_to_signal_to_remote.fatal" "Failed to send $signal_name signal to $remote_host_ip"
    fi
}

# Locks script execution with custom lock
# Usage: lock_script_custom <lock name>
#   $1: name of the lock file
lock_script_custom() {
    local lock_name="$1"

    if ! file_create "$LOCK_FILE_DIR" "$lock_name"; then
        exit_fatal "failed_to_lock_script_local_execution_by_$lock_name.fatal" "Failed to lock local scipt execution ($lock_name)"
    fi
}

# Unlocks script execution, removing a custom lock
# Usage: unlock_script_custom <lock name>
#   $1: name of the lock file
unlock_script_custom() {
    local lock_name="$1"

    if ! file_remove "$LOCK_FILE_DIR" "$lock_name"; then
        log_message "Failed to unlock script execution ($lock_name) (remove $lock_name manually)" "FATAL"
        exit 1
    fi
}

# Logs the time, when asynchronous replication started
# Usage: save_async_replication_start
# Returns:  0 - if operation successull
#           1 - if operation failed
save_async_replication_start() {
    log_message "Saving starting time of asynchronous replication" "NOTICE"

    if ! file_rewrite "$LOCK_FILE_DIR" "async.replication" "$(date +"%s")"; then
        log_message "Failed to save starting time of asynchronous replication" "FATAL"
        return 1
    fi 

    return 0
}

# Deletes signal file with the time, when asynchronous replication started
# Usage: clean_async_replication_start
# Returns:  0 - if operation successull
#           1 - if operation failed
clean_async_replication_start() {
    log_message "Cleaning starting time of asynchronous replication" "NOTICE"

    if ! file_remove "$LOCK_FILE_DIR" "async.replication"; then
        log_message "Failed to clean starting time of asynchronous replication" "FATAL"
        return 1
    fi

    return 0
}

# Saves the status of local PostgreSQL instance to a file
# Usage: save_local_postgres_status
save_local_postgres_status() {
    local local_postgres_status="$1"

    if (( $# != 1 )); then
        exit_fatal "save_current_pg_role_failed.fatal" "save_local_postgres_status failed - missing argument"
    fi

    log_message "Saving local PostgreSQL instance replication role" "INFO"

    if [[ "$local_postgres_status" -eq 1 ]]; then
        if ! file_rewrite "$LOCK_FILE_DIR" "previous_local_cluster_role" "M"; then
            exit_fatal "save_current_pg_role_failed.fatal" "Failed to save local PostgreSQL instance replication role"
        fi
    else
        if ! file_rewrite "$LOCK_FILE_DIR" "previous_local_cluster_role" "R"; then
            exit_fatal "save_current_pg_role_failed.fatal" "Failed to save local PostgreSQL instance replication role"
        fi
    fi
}
#==========================================================================#


#===============================SCRIPT UTILS===============================#

# Kills leftover background processes
on_stop_cleanup() {
    TERMINATE_REQUESTED=1

    for pid in "${MONITORING_PIDS[@]}"; do
        kill -s SIGTERM "$pid" > /dev/null 2>&1
    done
    
    return 0
}
#==========================================================================#


#=============================SIGNAL HANDELLING============================#

# Removes cpecific lock on recieving unlock signal
# Usage: unlock_script_by_signal 
unlock_script_by_signal() {
    lock_script_active_action

    lock_name=""
    lock_name=$(basename -- "$(find "$LOCK_FILE_DIR" -type f -name "*.unlock" | head -n 1)" .unlock)

    log_message "Unlock signal found, removing $lock_name.lock" "NOTICE"
    unlock_script_custom "$lock_name.lock"

    log_message "Cleaning up after unlocking, removing $lock_name.unlock" "NOTICE"
    if ! file_remove "$LOCK_FILE_DIR" "$lock_name.unlock"; then
        exit_fatal "failed_to_remove_unlock_signal.fatal" "Failed to remove $lock_name.unlock"
    fi

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_rewind_signal() {
    lock_script_active_action

    log_message "rewind.signal found, starting in master reattaching mode" "NOTICE"

    log_message "Blocking keepalived VIP" "NOTICE"
    if ! file_create "$KEEPALIVED_DIR" "no_vip.block"; then
        exit_fatal "failed_to_block_vip.fatal" "Failed to block keepalived VIP"
    fi

    if ! reattach_master_as_replica; then
        log_message "Removing rewind.signal" "NOTICE"
        unlock_script_custom "rewind.signal"

        log_message "Unlocking local script execution (master down)" "NOTICE"
        unlock_script_custom "master_down.lock"

        log_message "Unblocking keepalived VIP" "NOTICE"
        unlock_script_custom "no_vip.block"

        exit_fatal "failed_to_reattach_old_master_as_replica.fatal" "Failed to reattach old master as replica"
    fi

    log_message "Removing rewind.signal" "NOTICE"
    unlock_script_custom "rewind.signal"

    log_message "Unlocking local script execution (master down)" "NOTICE"
    unlock_script_custom "master_down.lock"

    log_message "Unblocking keepalived VIP" "NOTICE"
    if ! file_remove "$KEEPALIVED_DIR" "no_vip.block"; then
        exit_fatal "failed_to_unblock_keepalived_vip" "Failed to remove block from keepalived VIP"
    fi

    log_message "Successfully reattached old master as a replica, system restored" "SUCCESS"

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_restart_replica_signal() {
    lock_script_active_action

    log_message "restart_replica.signal found, starting in old replica restarting mode" "NOTICE"

    if ! local_postgresql_control "start" "replica"; then
        log_message "Removing restart_replica.signal" "NOTICE"
        unlock_script_custom "restart_replica.signal"

        exit_fatal "failed_to_restart_old_replica.fatal" "Failed to restart old replica"
    fi

    log_message "Removing restart_replica.signal" "NOTICE"
    unlock_script_custom "restart_replica.signal"

    if ! block_state_negotiation; then
        exit_fatal "failed_to_block_state_negotiation.fatal" "Failed to block state negotiation"
    fi

    log_message "Successfully restarted old replica, awaiting replica promotion" "WARNING"

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_prepare_transition_signal() {
    lock_script_active_action

    log_message "prepare_transition.signal found, preparing async replica to trasition to synchronous" "NOTICE"

    if ! prepare_replica_for_sync_transition; then
        log_message "Failed to prepare async replica to trasition to synchronous" "WARNING"
    fi

    log_message "Removing prepare_transition.signal" "NOTICE"
    unlock_script_custom "prepare_transition.signal"

    log_message "Successfully prepared async replica to trasition to synchronous" "NOTICE"

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_restore_catchup_signal() {
    lock_script_active_action

    log_message "restore_catchup.signal found, restoring replica to catch up to master" "NOTICE"

    if ! restore_as_replcia_to_catch_up; then
        

        log_message "Removing restore_catchup.signal" "NOTICE"
        unlock_script_custom "restore_catchup.signal"

        exit_fatal "failed_to_restore_catch_up_replica.fatal" "Failed to restore replica to catch up to master"
    fi

    log_message "Removing restore_catchup.signal" "NOTICE"
    unlock_script_custom "restore_catchup.signal"

    log_message "Successfully restored replica to catch up to master" "NOTICE"

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_clean_replicatino_start_signal() {
    lock_script_active_action

    log_message "clean_replication_start.signal found, cleaning local starting time of synchronous replication" "NOTICE"

    if ! clean_async_replication_start; then
        log_message "Removing clean_replication_start.signal" "NOTICE"
        unlock_script_custom "clean_replication_start.signal"

        exit_fatal "failed_to_clean_async_start_time.fatal" "Failed to clean local starting time of synchronous replication"
    fi

    log_message "Removing clean_replication_start.signal" "NOTICE"
    unlock_script_custom "clean_replication_start.signal"

    log_message "Successfully cleaned local starting time of asynchronous replication" "SUCCESS"

    unlock_local_script_execution
    return 0
}

# A set of actions to handle rewind.signal
# Exits on fatal error
react_to_use_replication_slot_signal() {
    lock_script_active_action

    log_message "use_replication_slot.signal found, reconfiguring replica to use replication slot" "NOTICE"

    if ! reconfigure_replica_for_slot_use; then
        

        log_message "Removing use_replication_slot.signal" "NOTICE"
        unlock_script_custom "use_replication_slot.signal"

        exit_fatal "replica_failed_to_use_replication_slot.fatal" "Failed to reconfigure replica to use replication slot"
    fi

    log_message "Removing use_replication_slot.signal" "NOTICE"
    unlock_script_custom "use_replication_slot.signal"

    log_message "Successfully reconfigured replica to use replication slot" "NOTICE"

    unlock_local_script_execution
    return 0
}

# Processes locks and signals found in $LOCK_FILE_DIR
# Usage: check_locks_and_signals
# Returns:  0 - normal signal, continue execution
#           1 - restart execution loop
#           2 - start monitoring file dir
check_locks_and_signals() {

    if find "$LOCK_FILE_DIR" -type f -name "*.fatal" | grep -q .; then
        # exit with error if previousely script failed fatallty
        exit 1
    fi

    if find "$LOCK_FILE_DIR" -type f -name "*.unlock" | grep -q .; then
        # if signal to unlock is present
        unlock_script_by_signal
        return 1
    fi

    if find "$LOCK_FILE_DIR" -type f -name "*.lock" | grep -q .; then
        # if there are custom locks blocking execution, start monitoring lock directory
        return 2
    fi

    if [[ -e "$LOCK_FILE_DIR"/system.restored ]]; then
        # if cluster node 1 requests permission to go standalone master

        log_message "System restored, removing leftover locks" "NOTICE"

        unlock_script_custom "permission.granted"

        unlock_script_custom "system.restored"

        return 2
    fi

    if [[ -e "$LOCK_FILE_DIR"/"$CLUSTER_NAME_NODE_1".standalone ]]; then
        # if cluster node 1 requests permission to go standalone master

        log_message "Cluster node 1 requests permission to go standalone master" "NOTICE"

        if [[ -e "$LOCK_FILE_DIR"/permission.granted ]]; then
            log_message "Permission for node 2 was already granted, denying" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "0" "$CLUSTER_IP_NODE_1"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_1"
            fi
            unlock_script_custom "permission.granted"
        else
            log_message "Granting node 1 permission to go standalone" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "1" "$CLUSTER_IP_NODE_1"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_1"
            fi
            lock_script_custom "permission.granted"
        fi

        unlock_script_custom "$CLUSTER_NAME_NODE_1.standalone"

        return 2
    fi

    if [[ -e "$LOCK_FILE_DIR"/"$CLUSTER_NAME_NODE_2".standalone ]]; then
        # if cluster node 2 requests permission to go standalone master

        log_message "Cluster node 2 requests permission to go standalone master" "NOTICE"

        if [[ -e "$LOCK_FILE_DIR"/permission.granted ]]; then
            log_message "Permission for node 1 was already granted, denying" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "0" "$CLUSTER_IP_NODE_2"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_2"
            fi
            unlock_script_custom "permission.granted"
        else
            log_message "Granting node 2 permission to go standalone" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "1" "$CLUSTER_IP_NODE_2"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_2"
            fi
            lock_script_custom "permission.granted"
        fi

        unlock_script_custom "$CLUSTER_NAME_NODE_2.standalone"

        return 2
    fi

    if [[ -e "$LOCK_FILE_DIR"/"$CLUSTER_NAME_NODE_1".promote ]]; then
        # if cluster node 1 requests permission to promote replica

        log_message "Cluster node 1 requests permission to promote replica" "NOTICE"

        if [[ -e "$LOCK_FILE_DIR"/permission.granted ]]; then
            log_message "Permission for node 2 was already granted, denying" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "0" "$CLUSTER_IP_NODE_1"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_1"
            fi
            unlock_script_custom "permission.granted"
        else
            log_message "Granting node 1 permission to promote replica" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "1" "$CLUSTER_IP_NODE_1"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_1"
            fi
            lock_script_custom "permission.granted"
        fi

        unlock_script_custom "$CLUSTER_NAME_NODE_1.promote"

        return 2
    fi

    if [[ -e "$LOCK_FILE_DIR"/"$CLUSTER_NAME_NODE_2".promote ]]; then
        # if cluster node 2 requests permission to promote replica

        log_message "Cluster node 2 requests permission to promote replica" "NOTICE"

        if [[ -e "$LOCK_FILE_DIR"/permission.granted ]]; then
            log_message "Permission for node 1 was already granted, denying" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "0" "$CLUSTER_IP_NODE_2"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_2"
            fi
            unlock_script_custom "permission.granted"
        else
            log_message "Granting node 2 permission to promote replica" "NOTICE"
            if ! file_rewrite_remote "$LOCK_FILE_DIR" "voter.response" "1" "$CLUSTER_IP_NODE_2"; then
                exit_fatal "failed_to_send_voter_response.fatal" "Failed send voter response to $CLUSTER_NAME_NODE_2"
            fi
            lock_script_custom "permission.granted"
        fi

        unlock_script_custom "$CLUSTER_NAME_NODE_2.promote"

        return 2
    fi

    return 0
}
#==========================================================================#


#===================================MAIN===================================#

main() {
    file_remove "$LOCK_FILE_DIR" "permission.granted"

    while true;
    do
        local locks_and_signals_exit_code
        check_locks_and_signals
        locks_and_signals_exit_code="$?"
        if (( locks_and_signals_exit_code == 1 )); then
            true
        fi 

        if ((locks_and_signals_exit_code == 2 )); then
            inotifywait --event create "$LOCK_FILE_DIR" > /dev/null 2>&1 &
            local background_monitor_pid="$!"
            MONITORING_PIDS+=( "$background_monitor_pid" )

            check_locks_and_signals
            if (( $? == 2 )); then
                wait -n
            fi

            kill -s KILL "$background_monitor_pid" > /dev/null 2>&1
            if (( TERMINATE_REQUESTED == 1 )); then 
                exit 0
            fi
        fi

        if (( locks_and_signals_exit_code == 0 )); then
            inotifywait --event create "$LOCK_FILE_DIR" > /dev/null 2>&1 &
            local background_file_monitor_pid="$!"
            MONITORING_PIDS+=( "$background_file_monitor_pid" )

            wait -n
            
            kill -s SIGTERM "$background_file_monitor_pid" > /dev/null 2>&1
            if (( TERMINATE_REQUESTED == 1 )); then 
                exit 0
            fi
        fi
    done
}

trap on_stop_cleanup TERM INT EXIT

main

#=================================TEMPLATE=================================#
#==========================================================================#
