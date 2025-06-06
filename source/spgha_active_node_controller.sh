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
CLUSTER_NAME_LOCAL="pg_ha_1"
CLUSTER_NAME_REMOTE="pg_ha_2"
CLUSTER_IP_REMOTE="10.7.2.93"
CLUSTER_IP_VOTER="10.7.2.91"
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

VOTE_TIMEOUT=10 
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

USE_VOTER_NODE=1
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


#===========================POSTGRES HEALTHCHECK===========================#

# Checks health of local PostgreSQL instance
# Usage: check_local_postgres <postgresql host> <postgresql port> <postgresql admin user>
#   $1: ip of the postgresql host (localhost is expected)
#   $2: port on which PostgreSQL is run (usually 5432)
#   $3: PostgreSQL user that would perform the healthcheck (a database with the same name must exist)
# Returns:  1 - if PostgreSQL is running and is in Master role
#           2 - if PostgreSQL is running and is in Slave role
#           0 - if PostgreSQL isn't running and outputs latest checkpoint's REDO location
#           10 - if encountered other error
# !!!REWRITE!!!
check_local_postgres() {
    local host="$1"
    local port="$2"
    local user="$3"

    log_message "Checking local PostgreSQL status" "INFO"

    command_handler "$PG_ISREADY -h $host -p $port -U $user" "1" "$PG_STATUS_CHECK_TIMEOUT" "cheking if PostgreSQL is running" > /dev/null 2>&1
    local pg_is_ready_exit_code="$?"
    if [[ "$pg_is_ready_exit_code" -eq 0 ]]; then                       # PostgreSQL is ready to accept connections
        local is_slave
        local is_slave_exit_code
        is_slave=$(command_handler "$PSQL -h $host -p $port -U $user -tAc \"SELECT pg_is_in_recovery();\"" "1" "$PG_STATUS_CHECK_TIMEOUT" "checking PostgreSQL replication role") # !!!FIX!!! here there  must be a database with the same name as the $user
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

# Checks health of remote PostgreSQL instance
# Usage: check_remote_postgres <postgresql host> <postgresql port> <postgresql admin user> <remote systen admin user> <remote host ip>
#   $1: ip of the postgresql host (localhost is expected)
#   $2: port on which PostgreSQL is run (usually 5432)
#   $3: PostgreSQL user that would perform the healthcheck (a database with the same name must exist)
#   $4: user with which ssh connection would be established
#   $5: remote host's ip
# Returns:  1 - if PostgreSQL is running and is in Master role
#           2 - if PostgreSQL is running and is in Slave role
#           0 - if PostgreSQL isn't running and outputs latest checkpoint's REDO location
#           3 - if encountered ssh error
#           10 - if encountered other error
# !!!REWRITE!!!
check_remote_postgres() {   
    local host="$1"
    local port="$2"
    local user="$3"
    local ssh_user="$4"
    local ssh_host="$5"

    log_message "Checking remote PostgreSQL status on $ssh_user@$ssh_host" "INFO"

    remote_command_handler "$PG_ISREADY -h $host -p $port -U $user" "1" "$PG_STATUS_CHECK_TIMEOUT" "cheking if PostgreSQL is running" "$ssh_user" "$ssh_host" > /dev/null 2>&1
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

# Checks health of both PostgreSQL instances
# Usage: check_postgresql <postgresql host> <postgresql port> <postgresql admin user> <remote systen admin user> <remote host ip>
#   $1: ip of the postgresql host (localhost is expected)
#   $2: port on which PostgreSQL is run (usually 5432)
#   $3: PostgreSQL user that would perform the healthcheck (a database with the same name must exist)
#   $4: user with which ssh connection would be established
#   $5: remote host's ip
# Returns:  0 - if PostgreSQL replication pair state check succeds
#           1 - if fails to aquire PostgreSQL replication pair state
# Output:   both PostgreSQL instance resplication roles (1 - master 2 - replica 0 - not running) and if instance isn't running, it's latest checkpoint's REDO location.
# Format:   "local_instance_status:remote_instance_status:local_REDO:remote_REDO"
# !!!REWRITE!!!
check_postgresql() {
    # returns 0 and outputs both PostgreSQL instances replication roles (and if PostgreSQL instance is down, it's latest checkpoint's REDO location)
    # returns 1 if fails to aquire PostgreSQL replication pair status

    local host="$1"
    local port="$2"
    local user="$3"
    local ssh_user="$4"
    local ssh_host="$5"

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

# Opens and maintains connection to PostgreSQL, while continuosely polling it
# Usage: check_postgresql <postgresql host>
#   $1: ip of the postgresql host
check_postgresql_fault() {
    local host=$1

    coproc PSQL_MON {
        "$PSQL" -h "$host" -p "$PGPORT" -U "$PG_ADMIN_USER" -At
    }

    # Ensure cleanup on script exit
    trap 'kill $PSQL_MON_PID 2>/dev/null' EXIT

    while true; do
        echo "SELECT now();" >&"${PSQL_MON[1]}" || break
        read -r -t "$PG_BACKGROUND_CHECK_SLEEP" -u "${PSQL_MON[0]}" _ || break
        sleep "$PG_BACKGROUND_CHECK_SLEEP"
    done
}

# Uses psql to check standby's replication sync state
# Usage: check_postgresql_replication_sync_state <is master local>
#   $1: 1 - if master is running locally, 0 - if master is running remotely
# Returns:  0 - if replica is synchronous
#           1 - if replica is asynchronous
#           5 - if no replication is happening
#           10 - if fails to check
check_postgresql_replication_sync_state() {
    local master_is_local="$1"

    if (( $# != 1 )); then
        log_message "check_postgresql_replication_sync_state failed - missing argument" "ERROR"
        return 10
    fi

    log_message "Checking replication sync state" "DEBUG"

    local host
    if (( master_is_local == 1 )); then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi

    local replication_status
    local exit_code
    replication_status=$(command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT sync_state FROM pg_stat_replication\"" "1" "0" "fetching replication sync state")
    exit_code="$?"

    if (( exit_code == 0 )); then
        if [[ "$replication_status" = "sync" ]]; then
            log_message "Replica is synchronous" "DEBUG"
            return 0
        elif [[ "$replication_status" = "async" ]]; then
            log_message "Replica is asynchronous" "DEBUG"
            return 1
        else
            return 5
        fi
    fi

    log_message "Failed to check replication sync state" "ERROR"
    return 10
}

# Uses psql to check standby's replication state
# Usage: check_postgresql_replication_state <is master local>
#   $1: 1 - if master is running locally, 0 - if master is running remotely
# Returns:  0 - if replica is streaming
#           1 - if replica is catchup
#           5 - if replica in some other state
#           10 - if fails to check
check_postgresql_replication_state() {
    local master_is_local="$1"

    if (( $# != 1 )); then
        log_message "check_postgresql_replication_state failed - missing argument" "ERROR"
        return 10
    fi

    log_message "Checking replication state" "DEBUG"

    local host
    if (( master_is_local == 1 )); then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi

    local replication_state
    local exit_code
    replication_state=$(command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT state FROM pg_stat_replication\"" "1" "0" "fetching replication state")
    exit_code="$?"

    if (( exit_code == 0 )); then
        if [[ "$replication_state" = "streaming" ]]; then
            log_message "Replica is streaming" "DEBUG"
            return 0
        elif [[ "$replication_state" = "catchup" ]]; then
            log_message "Replica is catching up" "DEBUG"
            return 1
        else
            return 5
        fi
    fi

    log_message "Failed to check replication state" "ERROR"
    return 10
}

# Retrieves previous local PostgreSQL role from file
# Usage: check_previous_local_cluster_role
# Returns:  0 - if previous role file not found
#           1 - if previous local PostgreSQL role was master
#           2 - if previous local PostgreSQL role was replica
check_previous_local_cluster_role() {
    log_message "Checking previous local PostgreSQL replication role" "NOTICE"

    local previous_role
    local exit_code
    previous_role=$(command_handler "cat $LOCK_FILE_DIR/previous_local_cluster_role" "1" "0" "checking previous replication role")
    exit_code="$?"

    if (( exit_code != 0 )); then
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

# Checks if replication slot exisis and if it's in use
# Usage: check_replication_slot
# Returns:  0 - if replication slot is free
#           1 - if replication slot does not exist
#           2 - if replication slot is in use
#           124 - if check failed on timeout
check_replication_slot() {
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

# Checks for synchronous or asynchronous replication presence on master
# Usage: master_replication_check <replication type>
#   $1: if 0 - would check async replication, if 1 - would check sync replication
# Returns:  0 - if replication on master started
#           1 - if error occured
#           124 - if replication check timed out
master_replication_check() {
    local check_for_sync="$1"

    if (( $# != 1 )); then
        log_message "master_replication_check failed - missing argument" "ERROR"
        return 1
    fi

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

# Checks for replication presence on replica
# Usage: slave_replication_check
# Returns:  0 - if replication on replica started
#           124 - if replication check timed out
slave_replication_check() {
    local wal_receiver_status=""

    log_message "Waiting for wal receiver to start streaming on replica..." "NOTICE"

    local start_time
    start_time=$(date +%s)
    local exit_code

    while true; do
        wal_receiver_status=$(command_handler "$PSQL -h localhost -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT status from pg_stat_wal_receiver\"" "1" "0" "cheching PostgreSQL replica wal receiver status")

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
#==========================================================================#


#=============================POSTGRESQL UTILS=============================#

# Uses pg_ctl to start, stop, restart, reload configuration and promote local PostgreSQL
# Usage: local_postgresql_control <action> <postgresql role>
#   $1: action to perform:
#       start - starts local PostgreSQL instance
#       stop - stops local PostgreSQL instance
#       restart - restarts local PostgreSQL instance
#       reload - reloads local PostgreSQL instance's configuration
#       promote - promotes local PostgreSQL replica to master
#   $2: PostgreSQL role master/replica to correctly write logs messages
# Returns:  0 - if operation successfull
#           1 - if operation failed
local_postgresql_control() { 
    local command_option="$1"
    local pg_role="$2"

    if (( $# != 2 )); then
        log_message "local_postgresql_control failed - missing argument" "ERROR"
        return 1
    fi

    if [[ "$command_option" = "start" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null start" "1" "$PG_START_TIMEOUT" "starting PostgreSQL" > /dev/null 2>&1; then
            log_message "Failed to start local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully started local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "stop" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null stop" "1" "$PG_STOP_TIMEOUT" "stopping PostgreSQL" > /dev/null 2>&1; then
            log_message "Failed to stop local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully stopped local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "restart" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null restart" "1" "0" "restarting PostgreSQL" > /dev/null 2>&1; then
            log_message "Failed to restart local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully restarted local PostgreSQL $pg_role instance" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "reload" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null reload" "1" "0" "reloading PostgreSQL config" > /dev/null 2>&1; then
            log_message "Failed to reload local PostgreSQL $pg_role instance's config" "ERROR"
            return 1
        else
            log_message "Successfully reloaded local PostgreSQL $pg_role instance's config" "NOTICE"
            return 0
        fi
    elif [[ "$command_option" = "promote" ]]; then
        if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_CTL -D $PGDATA -l /dev/null promote" "1" "0" "promoting PostgreSQL" > /dev/null 2>&1; then
            log_message "Failed to promote local PostgreSQL $pg_role instance" "ERROR"
            return 1
        else
            log_message "Successfully promoted local PostgreSQL $pg_role instance to new master" "NOTICE"
            return 0
        fi
    else
        log_message "local_postgresql_control failed - provided option is incorrect" "ERROR"
        return 1
    fi
}
#==========================================================================#


#=========================POSTGRESQL CONFIGURATION=========================#

# Creates configuration for PostgreSQL master instace
# Usage: configure_local_master <attach synchronous replica>
#   $1: how to attach replica - if 1 - synchronous, if 0 - asynchronous
# Returns:  0 - if config was generated successfully
#           1 - if config failed to generate
configure_local_master() {
    local attach_synchronous_replica="$1"

    if (( $# != 1 )); then
        log_message "configure_local_master failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Generating postgresql.auto.conf for master" "NOTICE"
    
    log_message "Cleaning current postgresql.auto.conf" "NOTICE"
    if ! file_remove "$PGDATA" "postgresql.auto.conf"; then
        log_message "Failed to clean current postgersql.auto.conf" "ERROR"
        return 1
    fi

    if (( PG_ALLOW_STANDALONE_MASTER == 0 )) || (( attach_synchronous_replica == 1 )) ; then
        log_message "Adding synchronous replica to configuration" "NOTICE"
        if ! file_rewrite "$PGDATA" "postgresql.auto.conf" "synchronous_standby_names = '$CLUSTER_NAME_REMOTE'"; then
            log_message "Failed to add synchronous replica to configuration" "ERROR"
        fi
    else
        if ! file_create "$PGDATA" "postgresql.auto.conf"; then
            log_message "Failed to crate new psotgresql.auto.conf file" "ERROR"
        fi
    fi

    if ! file_modify_meta "$PGDATA" "postgresql.auto.conf" "600" "postgres" "postgres"; then
        log_message "Failed to modify configuration file access rights" "ERROR"
    fi

    if (( PG_USE_RECOVERY_CONF == 1 )); then
        log_message "Removing old recovery.done config file" "NOTICE"
        file_remove "$PGDATA" "recovery.done"
    else
        log_message "Removing unnecessary standby.signal" "NOTICE"
        file_remove "$PGDATA" "standby.signal"
    fi

    log_message "Successfully generated postgresql.auto.conf" "NOTICE"
    return 0
}

# Creates configuration for PostgreSQL replica instace
# Usage: configure_local_replica <enable replication slot>
#   $1: if 1 - configure replica using replication slot, if 0 - configure replica not using replication slot
# Returns:  0 - if config was generated successfully
#           1 - if config failed to generate
configure_local_replica() {
    local enable_replication_slot="$1"

    if (( $# != 1 )); then
        log_message "configure_local_replica failed - missing argument" "ERROR"
        return 1
    fi

    if [[ "$PG_USE_RECOVERY_CONF" -eq 1 ]]; then
        # configure with recovery.conf
        
        # creating postgresql.auto.conf
        log_message "Generating postgresql.auto.conf for replica" "NOTICE"

        log_message "Cleaning current postgresql.auto.conf" "NOTICE"
        if ! file_remove "$PGDATA" "postgresql.auto.conf"; then
            log_message "Failed to clean current postgersql.auto.conf" "ERROR"
            return 1
        fi

        if ! file_create "$PGDATA" "postgresql.auto.conf"; then
            log_message "Failed to crate new psotgresql.auto.conf file" "ERROR"
            return 1
        fi

        if ! file_modify_meta "$PGDATA" "postgresql.auto.conf" "600" "postgres" "postgres"; then
            log_message "Failed to modify configuration file access rights" "ERROR"
            return 1
        fi

        # creating recovery.conf
        log_message "Generating recovery.conf for replica" "NOTICE"
    
        log_message "Cleaning current recovery.conf" "NOTICE"
        if ! file_remove "$PGDATA" "recovery.conf"; then
            log_message "Failed to clean current recovery.conf" "ERROR"
            return 1
        fi

        local writes_exit_code=0

        file_rewrite "$PGDATA" "recovery.conf" "standby_mode = 'on'"
        (( writes_exit_code += $? ))
        file_append "$PGDATA" "recovery.conf" "recovery_target_timeline = 'latest'"
        (( writes_exit_code += $? ))
        file_append "$PGDATA" "recovery.conf" "recovery_target_action = 'pause'"
        (( writes_exit_code += $? ))
        file_append "$PGDATA" "recovery.conf" "primary_conninfo = 'user=$PG_REPLICATION_USER host=$CLUSTER_IP_REMOTE port=$PGPORT application_name=$CLUSTER_NAME_LOCAL'"
        (( writes_exit_code += $? ))

        if (( enable_replication_slot == 1 )) ; then
            log_message "Adding replication slot to configuration" "NOTICE"
            file_append "$PGDATA" "recovery.conf" "primary_slot_name = '$PG_REPLICATION_SLOT_NAME'"
            (( writes_exit_code += $? ))
        fi

        if (( writes_exit_code > 0 )); then
            log_message "Failed to write configuration to recovery.conf" "ERROR"
            return 1
        fi

        if ! file_modify_meta "$PGDATA" "recovery.conf" "600" "postgres" "postgres"; then
            log_message "Failed to modify configuration file access rights" "ERROR"
            return 1
        fi

        log_message "Successfully generated PostgreSQL replica configuration" "NOTICE"
        return 0
    else
        # configure with standby.signal

        # creating postgresql.auto.conf
        log_message "Generating postgresql.auto.conf for replica" "NOTICE"
    
        log_message "Cleaning current postgresql.auto.conf" "NOTICE"
        if ! file_remove "$PGDATA" "postgresql.auto.conf"; then
            log_message "Failed to clean current postgersql.auto.conf" "ERROR"
            return 1
        fi

        local writes_exit_code=0

        file_rewrite "$PGDATA" "postgresql.auto.conf" "recovery_target_timeline = 'latest'"
        (( writes_exit_code += $? ))
        file_append "$PGDATA" "postgresql.auto.conf" "recovery_target_action = 'pause'"
        (( writes_exit_code += $? ))
        file_append "$PGDATA" "postgresql.auto.conf" "primary_conninfo = 'user=$PG_REPLICATION_USER host=$CLUSTER_IP_REMOTE port=$PGPORT application_name=$CLUSTER_NAME_LOCAL'"
        (( writes_exit_code += $? ))

        if (( enable_replication_slot == 1 )) ; then
            log_message "Adding replication slot to configuration" "NOTICE"
            file_append "$PGDATA" "postgresql.auto.conf" "primary_slot_name = '$PG_REPLICATION_SLOT_NAME'"
            (( writes_exit_code += $? ))
        fi

        if (( writes_exit_code > 0 )); then
            log_message "Failed to write configuration to postgresql.auto.conf" "ERROR"
            return 1
        fi

        if ! file_modify_meta "$PGDATA" "postgresql.auto.conf" "600" "postgres" "postgres"; then
            log_message "Failed to modify configuration file access rights" "ERROR"
            return 1
        fi

        log_message "Adding standby.signal" "NOTICE"
        if ! file_create "$PGDATA" "standby.signal"; then
            log_message "Failed to crate standby.signal file" "ERROR"
            return 1
        fi
        
        if ! file_modify_meta "$PGDATA" "standby.signal" "600" "postgres" "postgres"; then
            log_message "Failed to modify configuration file access rights" "ERROR"
            return 1
        fi

        log_message "Successfully generated PostgreSQL replica configuration" "NOTICE"
        return 0
    fi
}

# Uses psql to create or drop replication slot
# Usage: manipulate_replication_slot <is master local> <create/drop replication slot>
#   $1: 1 - if master is running locally, 0 - if master is running remotely
#   $2: if 1 - creates replication slot, if 0 - droppes replication slot
# Returns:  0 - if slot was created/dropped successfully
#           1 - if operation failed
manipulate_replication_slot() {
    local master_is_local="$1"
    local create_replication_slot="$2"

    if (( $# != 2 )); then
        log_message "manipulate_replication_slot failed - missing argument" "ERROR"
        return 1
    fi

    local host

    if [[ "$master_is_local" -eq 1 ]]; then
        host="localhost"
    else
        host=$CLUSTER_IP_REMOTE
    fi
    
    if (( create_replication_slot == 1 )); then
        log_message "Creating replication slot" "NOTICE"
        if ! command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT * FROM pg_create_physical_replication_slot('$PG_REPLICATION_SLOT_NAME', true)\"" "1" "0" "creating replication slot" > /dev/null 2>&1; then
            log_message "Failed to create replication slot" "ERROR"
            return 1
        fi
    else
        log_message "Dropping replication slot" "NOTICE"
        if ! command_handler "$PSQL -h $host -p $PGPORT -U $PG_ADMIN_USER -tAc \"SELECT * FROM pg_drop_replication_slot('$PG_REPLICATION_SLOT_NAME')\"" "1" "0" "dropping replication slot" > /dev/null 2>&1; then
            log_message "Failed to drop replication slot" "ERROR"
            return 1
        fi
    fi

    return 0
}
#==========================================================================#


#===========================REPLICA MANIPULATION===========================#

# Restores local replica from immediate backup (e.g. pg_basebackup)
# Usage: restore_as_replica <not use replication slot>
#   $1: if 1 - don't use replication slot, if 0 - use replication slot if applicable
# Returns:  0 - if successfully restored local replica from immediate backup
#           1 - if operation failed
restore_as_replica() {
    local restore_without_replication_slot="$1"

    if (( $# != 1 )); then
        log_message "restore_as_replica failed - missing argument" "ERROR"
        return 1
    fi
    
    log_message "Restoring local PostgreSQL with pg_basebackup as replica" "NOTICE"

    #!!!ENSURE_SHUTDOWN!!!
    
    if ! archive_local_postgresql_logs; then
        log_message "Failed to restore local PostgreSQL as replica: unable to archive previous PostgreSQL logs" "ERROR"
        return 1
    fi

    log_message "Cleaning current pg_data and pg_wal directories" "NOTICE"

    if ! directory_remove "$PGWAL/*"; then
        log_message "Failed to restore local PostgreSQL as replica: unable to cleanup previous WAL directory" "ERROR"
        return 1
    fi

    if ! directory_remove "$PGDATA/*"; then
        log_message "Failed to restore local PostgreSQL as replica: unable to cleanup previous DATA directory" "ERROR"
        return 1
    fi

    log_message "Restoring local Postgresql data directory with pg_basebackup" "NOTICE"

    if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_BASEBACKUP -D $PGDATA --waldir $PGWAL -U $PG_REPLICATION_USER -h $CLUSTER_IP_REMOTE -R" "1" "$PG_BASEBACKUP_RESTORE_TIMEOUT" "restoring with pg_basebackup" > /dev/null 2>&1; then
        log_message "Failed to restore local PostgreSQL as replica: pg_basebackup failed" "ERROR"
        return 1
    fi

    log_message "Local PostgreSQL data directory successfully restored from current master" "NOTICE"

    local use_replication_slot

    if (( restore_without_replication_slot == 0 )) && (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        use_replication_slot=1
    else
        use_replication_slot=0
    fi

    if ! configure_local_replica "$use_replication_slot"; then
        log_message "Failed to restore local PostgreSQL as replica: unable to configure PostgreSQL instance" "ERROR"
    return 1
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to restore local PostgreSQL as replica: PostgreSQL instance didn't start" "ERROR"
        return 1
    fi

    log_message "Local PostgreSQL successfully restored as replica" "NOTICE"
    return 0
}

# Restores local replica by reconfiguting and restarting it
# Usage: replica_attempt_restart
# Returns:  0 - if successfully restored local replica by restarting it
#           1 - if operation failed
replica_attempt_restart() {
    log_message "Attemting to restart local replica to restore it" "NOTICE"

    #!!!ENSURE_SHUTDOWN!!!

    local use_replication_slot
    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        use_replication_slot=1
    else
        use_replication_slot=0
    fi

    if ! configure_local_replica "$use_replication_slot"; then
        log_message "Replica restart attempt failed: unable to reconfigure replica" "ERROR"
        return 1
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Replica restart attempt failed: PostgreSQL instance didn't start" "ERROR"
        return 1
    fi
    
    log_message "Replica successfully restarted with correct configuration" "NOTICE"
    return 0
}

# Attempts to recover local replica by restarting it or restoring from immediate backup
# Usage: recover_local_replica
# Returns:  0 - if successfully recovered local replica
#           1 - if operation failed
recover_local_replica() {
    log_message "Starting recovery process for local replica" "NOTICE"

    if replica_attempt_restart; then
        if slave_replication_check; then
            if ((  PG_ALLOW_STANDALONE_MASTER == 1 )); then
                if save_async_replication_start; then
                    log_message "Recovery attempt succeded: replica reattached after restart" "NOTICE"
                    return 0
                fi
            else
                log_message "Recovery attempt succeded: replica reattached after restart" "NOTICE"
                return 0
            fi
        fi
    fi

    log_message "Restarting local replica attempt failed" "WARNING"    

    if restore_as_replica "0"; then
        if slave_replication_check; then
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

# Reconfigures local replica to stop using replciation slot
# Usage: prepare_replica_for_sync_transition
# Returns:  0 - if successfully reconfigures local replica
#           1 - if operation failed
prepare_replica_for_sync_transition() {
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

    if ! slave_replication_check; then
        log_message "Failed to prepare local replica for sync transition: replication check failed" "ERROR"
        return 1
    fi

    log_message "Local replica is ready for sync transition" "NOTICE"
    return 0
}

# Restores replica with immediate backup to catch up to master
# Usage: restore_as_replcia_to_catch_up
# Returns:  0 - if successfully retores replica for catch-up
#           1 - if operation failed
restore_as_replcia_to_catch_up() {
    log_message "Restoring local PostgreSQL as replica to catch up to master" "NOTICE"

    local_postgresql_control "stop" "replica" > /dev/null 2>&1

    if ! restore_as_replica "1"; then
        log_message "Failed to restore local PostgreSQL as replica to catch up to master: failed to restore local Postgresql as replica" "ERROR"
        return 1
    fi

    if ! slave_replication_check; then
        log_message "Failed to restore local PostgreSQL as replica to catch up to master: replication check failed" "ERROR"
        return 1
    fi

    log_message "Local replica is ready for sync transition" "NOTICE"
    return 0
}

# Promotes local replica to new master
# Usage: promote_local_replica
# Returns:  0 - if successfully promoted local replica
#           1 - if operation failed
promote_local_replica() {
    log_message "Promoting local replica" "NOTICE"

    if ! local_postgresql_control "promote" "replica"; then
        log_message "Failed to promote local replica: pg_ctl promote failed" "FATAL"
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

    if ! local_postgresql_control "reload" "master"; then
        log_message "Failed to promote local replica: failed to reload new config" "FATAL"
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

# Reconfigures replica to use replication slot
# Usage: reconfigure_replica_for_slot_use
# Returns:  0 - if successfully reconfigured local replica
#           1 - if operation failed
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
    if ! file_remove "$LOCK_FILE_DIR" "ssh_interconnect.lost"; then
        return 1
    fi

    log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
    if ! file_create_remote "$LOCK_FILE_DIR" "remote.unlock" "$CLUSTER_IP_REMOTE"; then
        log_message "Failed to unlock script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "ERROR"
        return 1
    fi

    return 0
}

# Promotes cought up async replica to synchronous
# Usage: sync_cought_up_async_replica
# Returns:  0 - if operation successfull
#           1 - if operation failed
sync_cought_up_async_replica() {
    log_message "Attempting to promote asynchronous replica to synchronous" "NOTICE"

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to prepare replica for sync transition" "NOTICE"
    if ! file_create_remote "$LOCK_FILE_DIR" "prepare_transition.signal" "$CLUSTER_IP_REMOTE"; then
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
    if ! file_create_remote "$LOCK_FILE_DIR" "clean_replication_start.signal" "$CLUSTER_IP_REMOTE"; then
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

# Promotes lagging async replica to synchronous by catching it up with pg_basebackup
# Usage: sync_cought_up_async_replica
# Returns:  0 - if operation successfull
#           1 - if operation failed
sync_lagging_async_replica() {
    log_message "Attempting to promote lagging asynchronous replica to synchronous by catching up with pg_basebackup" "NOTICE"

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restore replica to catch up to master" "NOTICE"
    if ! file_create_remote "$LOCK_FILE_DIR" "restore_catchup.signal" "$CLUSTER_IP_REMOTE"; then
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
    if ! file_create_remote "$LOCK_FILE_DIR" "clean_replication_start.signal" "$CLUSTER_IP_REMOTE"; then
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
#==========================================================================#


#============================MASTER MANIPULATION===========================#

# Rewinds previous master
# Usage: rewind_local_master
# Returns:  0 - if successfully rewound previous master
#           1 - if operation failed
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
    if ! command_handler "sudo -u $SYSTEM_POSTGRES_USER $PG_REWIND --target-pgdata=$PGDATA --source-server=\"host=$CLUSTER_IP_REMOTE port=$PGPORT user=$PG_ADMIN_USER\"" "1" "$PG_REWIND_TIMEOUT" "rewinding PostgreSQL" > /dev/null 2>&1; then
        log_message "Failed to rewind previous master: pg_rewind failed" "ERROR"
        return 1
    fi
    log_message "Successfully rewound previous master" "NOTICE"

    local use_replication_slot
    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        use_replication_slot=1
    else
        use_replication_slot=0
    fi

    if ! configure_local_replica "$use_replication_slot"; then
        log_message "Failed to rewind previous master: unable to configure instance" "ERROR"
        return 1
    fi

    if ! local_postgresql_control "start" "replica"; then
        log_message "Failed to rewind previous master: failed to start replica instance" "ERROR"
        return 1
    fi

    log_message "Old master successfully rewound and started as replica" "NOTICE"
    return 0
}

# Reattaches previous master as replica using pg_rewind / restoration from immediate backup 
# Usage: reattach_master_as_replica
# Returns:  0 - if successfully reattached old master as replica
#           1 - if operation failed
reattach_master_as_replica() {
    log_message "Reattaching old master as replica" "NOTICE"

    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
        log_message "Removing failed SSH interconnect signal" "NOTICE"
        unlock_script_custom "ssh_interconnect.lost"
    fi

    if rewind_local_master; then 
        if slave_replication_check; then
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
        if slave_replication_check; then
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

# Disables synchronous configuration on master and creates a replication slot 
# Usage: reconfigure_master_for_standalone_mode
# Returns:  0 - if operation successull
#           1 - if operation failed
reconfigure_master_for_standalone_mode() {
    log_message "Reconfiguring master for standalone mode" "NOTICE"

    if ! configure_local_master "0"; then
        log_message "Reconfiguring master for standalone mode: unable to write configuration" "FATAL"
        return 1
    fi

    if ! local_postgresql_control "reload" "master"; then
        log_message "Reconfiguring master for standalone mode: master failed to read new configuration" "FATAL"
        return 1
    fi

    if ! manipulate_replication_slot "1" "1"; then
        log_message "Reconfiguring master for standalone mode: failed to create a replication slot" "FATAL"
        return 1
    fi

    return 0
}

# Enables synchronous configuration on master and removes the replication slot
# Usage: reconfigure_master_for_synchronous_mode <drop replication slot>
#   $1: if 1 - droppes replication slot, if 0 - doesn't attemp to drop replication slot
# Returns:  0 - if operation successull
#           1 - if operation failed
reconfigure_master_for_synchronous_mode() {
    local drop_replication_slot="$1"

    if (( $# != 1 )); then
        log_message "reconfigure_master_for_synchronous_mode failed - missing argument" "ERROR"
        return 1
    fi

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

    if ! local_postgresql_control "reload" "master"; then
        log_message "Reconfiguring master for standsynchronousalone mode: master failed to read new configuration" "FATAL"
        return 1
    fi

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


#=============================STATE NEGOTIATION============================#

# Creates a file on remote active node with current detected state of PostgreSQL instances
# Usage: share_state_with_remote <local PostgreSQL state> <remote PostgreSQL state>
#   $1: local PostgreSQL instance's status
#   $2: remote PostgreSQL instance's status
#   status values:
#       0 - PostgreSQL instance is not running
#       1 - PostgreSQL instance is in master role
#       2 - PostgreSQL instance is in replica role
# Returns:  0 - if successfully shared remote state
#           1 - if failed to share state
share_state_with_remote() {
    local local_postgres_status="$1"
    local remote_postgres_status="$2"

    if (( $# != 2 )); then
        log_message "share_state_with_remote failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Sending local detected state to remote active node" "NOTICE"

    if ! file_rewrite_remote "$LOCK_FILE_DIR" "remote_pg.status" "$remote_postgres_status:$local_postgres_status" "$CLUSTER_IP_REMOTE"; then
        log_message "Failed to send local state to remote active node" "ERROR"
        return 1
    fi

    log_message "Successfully sent local state to remote active node" "DEBUG"
    return 0
}

# Reads state file, shared by remote and compares it with state detected locally
# Usage: share_state_with_remote <local PostgreSQL state> <remote PostgreSQL state>
#   $1: local PostgreSQL instance's status
#   $2: remote PostgreSQL instance's status
#   status values:
#       0 - PostgreSQL instance is not running
#       1 - PostgreSQL instance is in master role
#       2 - PostgreSQL instance is in replica role
# Returns:  0 - if local and remote states match
#           1 - failed to process shared state
#           10 - if local and remote states don't match
read_remote_state_and_compare() {
    local local_postgres_status="$1"
    local remote_postgres_status="$2"

    if (( $# != 2 )); then
        log_message "read_remote_state_and_compare failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Reading state, shared by remote, and comparing to local state" "DEBUG"

    read_remote_state_result=$(command_handler "cat $LOCK_FILE_DIR/remote_pg.status" "1" "0" "reading remote's statuses")
    exit_code="$?"

    if (( exit_code != 0 )); then
        log_message "Failed to read state, shared by remote" "ERROR"
        return 1
    fi

    local local_postgres_status_verification=-1
    local remote_postgres_status_verification=-1

    local IFS=":"
    read -r local_postgres_status_verification remote_postgres_status_verification <<< "$read_remote_state_result"

    if (( local_postgres_status == local_postgres_status_verification )); then
        if (( remote_postgres_status == remote_postgres_status_verification )); then
            log_message "Local PostgreSQL state and remote's state are in sync" "NOTICE"
            return 0
        fi
    fi

    log_message "Local PostgreSQL state and remote's state are different" "WARNING"
    return 10
}

# Removes local leftover files used by state negotiation
# Usage: clean_shared_state
# Returns:  0 - if successfull
#           1 - if failed
clean_shared_state() {
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

    if file_remove "$LOCK_FILE_DIR" "remote_pg.status" && file_remove "$LOCK_FILE_DIR" "status.expired"; then
        return 0
    else
        log_message "Failed clean up remote PostgreSQL state (clean shared state manually)" "FATAL"
        return 1
    fi
}

# Handles the process of comparing state of PostgreSQL instances between local and remote scripts
# Usage: new_negotiate_state <local PostgreSQL state> <remote PostgreSQL state>
#   $1: local PostgreSQL instance's status
#   $2: remote PostgreSQL instance's status
#   status values:
#       0 - PostgreSQL instance is not running
#       1 - PostgreSQL instance is in master role
#       2 - PostgreSQL instance is in replica role
# Returns:  0 - if local and remote states match
#           1 - if state negotiation failed
negotiate_state(){
    local local_postgres_status="$1"
    local remote_postgres_status="$2"

    if (( $# != 2 )); then
        log_message "negotiate_state failed - missing argument" "ERROR"
        return 1
    fi

    log_message "Negotiating PostgreSQL instances state with remote" "NOTICE"

    if [[ -e "$LOCK_FILE_DIR"/status.expired ]]; then
        log_message "Cleaning expired shared state" "NOTICE"
        if ! clean_shared_state; then
            exit_fatal "shared_state_cleanup_failed.fatal" "Unable to cleanup expired remote state"
        fi
    fi

    if ! share_state_with_remote "$local_postgres_status" "$remote_postgres_status"; then
        log_message "State negotiation failed: unable to share state" "ERROR"
        return 1
    fi

    file_monitor "$LOCK_FILE_DIR" "remote_pg.status" "$STATE_VERIFICATION_TIMEOUT"
    local monitor_exit_code="$?"

    if (( monitor_exit_code == 0 )); then
        if read_remote_state_and_compare "$local_postgres_status" "$remote_postgres_status"; then
            if ! clean_shared_state; then
                exit_fatal "shared_state_cleanup_failed.fatal" "Unable to cleanup shared remote state"
            fi
            return 0
        fi
    fi

    if (( monitor_exit_code == 124 )); then
        log_message "Signaling remote that shared state is expired" "NOTICE"
        signal_to_remote "status.expired" "$CLUSTER_IP_REMOTE"
    fi

    log_message "State negotiation failed" "ERROR"
    if ! clean_shared_state; then
        exit_fatal "shared_state_cleanup_failed.fatal" "Unable to cleanup shared remote state"
    fi
    return 1
}

# Creates a signal, that blocks state negotiation
# Usage: block_state_negotiation
# Returns:  0 - if successfull
#           1 - if failed
block_state_negotiation() {
    log_message "Blocking PostgreSQL instances state negotiation until system is restored" "NOTICE"
     
    if ! file_create "$LOCK_FILE_DIR" "state_negotiaton.block"; then
        log_message "Failed to block PostgreSQL instances state negotiation" "FATAL"
        return 1
    else
        return 0
    fi
}

# Removes the signal, that blocks state negotiation
# Usage: unblock_state_negotiation
# Returns:  0 - if successfull
#           1 - if failed
unblock_state_negotiation() {
    log_message "Unblocking PostgreSQL instances state negotiation" "NOTICE"

    if ! file_remove "$LOCK_FILE_DIR" "state_negotiaton.block"; then
        log_message "Failed to unblock PostgreSQL instances state negotiation" "FATAL"
        return 1
    else
        return 0
    fi
}
#==========================================================================#


#===================================VOTING=================================#

approve_with_voter() {
    local action_to_approve="$1"

    if (( $# != 1 )); then
        log_message "approve_with_voter failed - missing argument" "ERROR"
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/voter_response.invalid ]]; then
        unlock_script_custom "voter.response"
        unlock_script_custom "voter_response.invalid"
    fi

    if ! file_create_remote "$LOCK_FILE_DIR" "$action_to_approve" "$CLUSTER_IP_VOTER"; then
        log_message "Failed to send approval request to voter" "WARNING"
        return 1
    fi

    if ! file_monitor "$LOCK_FILE_DIR" "voter.response" "$VOTE_TIMEOUT"; then
        log_message "Voter didn't respond in time" "WARNING"
        lock_script_custom "voter_response.invalid"
        return 1
    fi

    local voter_response
    voter_response=$(cat "$LOCK_FILE_DIR"/voter.response)

    if (( voter_response == 0 )); then
        log_message "Voter didn't approve action" "NOTICE"
        unlock_script_custom "voter.response"
        return 1
    else
        log_message "Voter approved action" "NOTICE"
        unlock_script_custom "voter.response"
        return 0
    fi
}
#==========================================================================#


#================================MAIN LOGIC================================#

# A set of actions aimed to handle fault of local master with SSH interconnect present
# Exits on fatal error
handle_ssh_local_master_down() {
    log_message "Local master is down" "ERROR"

    log_message "locking local script execution (master down)" "NOTICE"
    lock_script_custom "master_down.lock"
    
    log_message "Awaiting rewind signal" "WARNING"

    unlock_local_script_execution
    return 0
}

# A set of actions aimed to handle fault of local replica with SSH interconnect present
# Exits on fatal error
handle_ssh_local_replica_down() {
    log_message "Local replica is down, latest chechpoint's REDO location: $local_postgres_redo" "ERROR"

    if ! file_monitor "$LOCK_FILE_DIR" "master_async.signal" "$STANDALONE_WAIT_TIMEOUT"; then
        exit_fatal "master_wasnt_ready_for_synchronous_replication.fatal" "Master wasn't ready for asynchronous replication in time"
    else
        log_message "Master is ready for asynchronous replication" "NOTICE"
    fi

    unlock_script_custom "master_async.signal" 

    if ! recover_local_replica; then
        exit_fatal "failed_to_restore_local_replica.fatal" "Failed to recover local replica"
    fi

    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
        log_message "Removing failed SSH interconnect signal" "NOTICE"
        unlock_script_custom "ssh_interconnect.lost"
    fi

    log_message "Local replica recovered successfully, system restored" "SUCCESS"


    unlock_local_script_execution
    return 0
}

# A set of actions aimed to handle fault of remote master with SSH interconnect present
# Exits on fatal error
handle_ssh_remote_master_down() {
    log_message "Remote master on $CLUSTER_NAME_REMOTE is down" "ERROR"
                    
    log_message "locking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
    signal_to_remote "master_down.lock" "$CLUSTER_IP_REMOTE"


    if (( PG_ALLOW_STANDALONE_MASTER == 1)) && [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
        exit_fatal "async_replica_promote_prohibited.fatal" "Prohibited to promote asynchronous replica"
    fi

    if ! promote_local_replica; then
        exit_fatal "failed_to_promote_local_replica.fatal" "Failed to promote local replica"
    fi

    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
        log_message "Removing failed SSH interconnect signal" "NOTICE"

        if ! file_remove "$LOCK_FILE_DIR" "ssh_interconnect.lost"; then
            exit_fatal "failed_to_remove_lost_interconnect_signal.fatal" "Failed to remove lost SSH interconnect signal"
        fi
    fi

    log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start rewind process" "NOTICE"
    signal_to_remote "rewind.signal" "$CLUSTER_IP_REMOTE"

    log_message "Unlocking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
    signal_to_remote "master_down.unlock" "$CLUSTER_IP_REMOTE"

    if (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
        if ! master_replication_check "0"; then
            exit_fatal "asyncronous_replication_on_master_didnt_start.fatal" "Asynchronous replication failed to start"
        fi

        if ! save_async_replication_start; then
            exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
        fi
        
        log_message "Local replica promoted, asyncronous replication started, system restored" "SUCCESS"
    else
        if ! master_replication_check "1"; then
            exit_fatal "syncronous_replication_on_master_didnt_start.fatal" "Synchronous replication failed to start"
        fi

        log_message "Local replica promoted, syncronous replication started, system restored" "SUCCESS"
    fi

    unlock_local_script_execution
    return 0
}

# A set of actions aimed to handle fault of remote replica with SSH interconnect present
# Exits on fatal error
handle_ssh_remote_replica_down() {
    log_message "Remote replica on $CLUSTER_IP_REMOTE is down, awaiting recovery" "ERROR"

    if (( PG_ALLOW_STANDALONE_MASTER == 1)); then
        
        if ! [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
            if ! reconfigure_master_for_standalone_mode; then
                exit_fatal "failed_to_reconfigure_master_for_standalone_mode.fatal" "Failed to reconfigure master for standalone operation"
            fi
        fi

        log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE that master is ready for async replication" "NOTICE"
        signal_to_remote "master_async.signal" "$CLUSTER_IP_REMOTE"


        if ! master_replication_check "0"; then
            exit_fatal "asyncronous_replication_on_master_didnt_start.fatal" "Asynchronous replication failed to start"
        fi

        if ! save_async_replication_start; then
            exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
        fi

        if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
            log_message "Removing failed SSH interconnect signal" "NOTICE"
            unlock_script_custom "ssh_interconnect.lost"
        fi
        
        log_message "Remote replica recovered, asyncronous replication started, system restored" "SUCCESS"
    else
        if ! master_replication_check "1"; then
            exit_fatal "syncronous_replication_on_master_didnt_start.fatal" "Synchronous replication failed to start"
        fi
        log_message "Remote replica recovered, syncronous replication started, system restored" "SUCCESS"
    fi

    unlock_local_script_execution
    return 0
}

# A set of actions aimed to handle fault of both PostgreSQL instances while local was master with SSH interconnect present
# Exits on fatal error
handle_ssh_both_down_local_master() {
    log_message "Both nodes are down, local node was in master role." "ERROR"

    log_message "locking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
    signal_to_remote "remote.lock" "$CLUSTER_IP_REMOTE"

    local master_restart_exit_code
    local_postgresql_control "start" "master"
    master_restart_exit_code="$?"

    if [[ "$master_restart_exit_code" -eq 0 ]]; then
        # local master successfully restarted

        log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
        signal_to_remote "remote.unlock" "$CLUSTER_IP_REMOTE"

        if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then 
            if ! master_replication_check "1"; then
                exit_fatal "syncronous_replication_on_master_didnt_start.fatal" "Synchronous replication failed to start"
            fi

            log_message "Master recovered, remote replica recovered, system restored" "SUCCESS"
        else
            log_message "Awating script recovery cycle" "WARNING"
        fi

        unlock_local_script_execution
        return 0
    fi

    if [[ "$master_restart_exit_code" -ne 0 ]]; then
        # local master failed to restart

        log_message "locking local script execution (master down)" "NOTICE"
        lock_script_custom "master_down.lock"

        log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to restart old replica" "NOTICE"
        signal_to_remote "restart_replica.signal" "$CLUSTER_IP_REMOTE"

        log_message "Unlocking script execution on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
        signal_to_remote "remote.unlock" "$CLUSTER_IP_REMOTE"

        log_message "Remote signaled to restart old replica, awaiting rewind signal" "WARNING"

        unlock_local_script_execution
        return 0
    fi
}

# A set of actions aimed to handle fault of both PostgreSQL instances while local was replica with SSH interconnect present
# Exits on fatal error
handle_ssh_both_down_local_replica() {
    log_message "Both nodes are down, local node was in replica role." "ERROR"
    
    if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then
        if ! block_state_negotiation; then
            exit_fatal "failed_to_block_state_negotiation.fatal" "Failed to block state negotiation"
        fi
    fi

    log_message "locking local script execution (remote lock)" "NOTICE"
    lock_script_custom "remote.lock"

    log_message "Awaiting unlock" "WARNING"
    
    unlock_local_script_execution
    return 0
}

# A set of actions aimed to handle loss of SSH interconnect
# Exits on fatal error
handle_no_ssh() {
    local local_postgres_status="$1"

    lock_script_active_action

    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]]; then
        unlock_local_script_execution
        return 0
    fi

    lock_script_custom "ssh_interconnect.lost"

    if (( PG_ALLOW_STANDALONE_MASTER == 0 )); then
        log_message "Waiting for SSH connection" "WARNING"
        unlock_local_script_execution
        return 0
    fi

    log_message "SSH interconnect failed" "WARNING"

    if (( USE_VOTER_NODE == 0 )); then
        if ! [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
            if (( local_postgres_status == 1)); then
                # local node is Master
                if ! reconfigure_master_for_standalone_mode; then
                    exit_fatal "failed_to_reconfigure_master_for_standalone_mode.fatal" "Failed to reconfigure master for standalone operation"
                fi

                if ! save_async_replication_start; then
                    exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
                fi
            fi
        fi

        unlock_local_script_execution
        return 0
    fi

    if (( USE_VOTER_NODE == 1 )); then
        if (( local_postgres_status == 1 )); then
            # local node is master

            log_message "Blocking keepalived VIP" "NOTICE"
            if ! file_create "$KEEPALIVED_DIR" "no_vip.block"; then
                exit_fatal "failed_to_block_vip.fatal" "Failed to block keepalived VIP"
            fi

            log_message "Asking voter to allow standalone master" "NOTICE"
            if approve_with_voter "$CLUSTER_NAME_LOCAL.standalone"; then
                log_message "Unblocking keepalived VIP" "NOTICE"
                if ! file_remove "$KEEPALIVED_DIR" "no_vip.block"; then
                    exit_fatal "failed_to_unblock_keepalived_vip" "Failed to remove block from keepalived VIP"
                fi

                if ! reconfigure_master_for_standalone_mode; then
                    exit_fatal "failed_to_reconfigure_master_for_standalone_mode.fatal" "Failed to reconfigure master for standalone operation"
                fi

                if ! save_async_replication_start; then
                    exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
                fi
            else
                if ! local_postgresql_control "stop" "master"; then
                    log_message "Keepalived would remain blocked to prevent spit brain. Remove $KEEPALIVED_DIR/no_vip.block manualy" "ERROR"
                    exit_fatal "failed_to_stop_local_master.fatal" "Failed to stop local PostgreSQL master during SSH interconnect failiure"
                fi

                log_message "locking local script execution (master down)" "NOTICE"
                lock_script_custom "master_down.lock"
                
                log_message "Unblocking keepalived VIP" "NOTICE"
                if ! file_remove "$KEEPALIVED_DIR" "no_vip.block"; then
                    exit_fatal "failed_to_unblock_keepalived_vip" "Failed to remove block from keepalived VIP"
                fi
            fi
        fi

        if (( local_postgres_status == 2 )); then
            # local node is replica

            log_message "Asking voter to allow replica promotion" "NOTICE"
            if approve_with_voter "$CLUSTER_NAME_LOCAL.promote"; then
                if [[ -e "$LOCK_FILE_DIR"/async.replication ]]; then
                    exit_fatal "async_replica_promote_prohibited.fatal" "Prohibited to promote asynchronous replica"
                fi

                if ! promote_local_replica; then
                    exit_fatal "failed_to_promote_local_replica.fatal" "Failed to promote local replica"
                fi

                if ! lock_script_custom "local.promoted"; then
                    exit_fatal "failed_to_mark_promotion.fatal" "Failed to mark promotion of local replica"
                fi

                if ! block_state_negotiation; then
                    exit_fatal "failed_to_block_state_negotiation.fatal" "Failed to block state negotiation"
                fi

                if ! save_async_replication_start; then
                    exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
                fi
            else
                if ! local_postgresql_control "stop" "replica"; then
                    exit_fatal "failed_to_stop_local_replica.fatal" "Failed to stop local PostgreSQL replica during SSH interconnect failiure"
                fi

                if ! save_async_replication_start; then
                    exit_fatal "failed_to_save_starting_time_of_asynchronous_replication.fatal" "Failed to log start of async replication"
                fi
            fi
        fi

        unlock_local_script_execution
        return 0
    fi
}

# Main PostgreSQL failiure handling logic
decision_maker() {
    local local_postgres_status="$1"
    local local_postgres_redo="$2"
    local remote_postgres_status="$3"
    local remote_postgres_redo="$4"

    local previous_local_postgres_role

    if [[ "$remote_postgres_status" -ne 3 ]]; then
        # ssh connection is present

        if  (( local_postgres_status * remote_postgres_status != 0 )); then
            # both PostgreSQL nodes are running (no action needed)

            if [[ -e "$LOCK_FILE_DIR"/state_negotiaton.block ]]; then
                if ! unblock_state_negotiation; then
                    exit_fatal "failed_to_unblock_state_negotiation.fatal" "Failed to unblock state negotiation"
                fi
            fi

            save_local_postgres_status "$local_postgres_status"
            

            if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]] && (( local_postgres_status == 1)) && (( PG_ALLOW_STANDALONE_MASTER == 1 )); then
                # SSH interconnect recovered, both nodes are running and local PostgreSQL is Master
                lock_script_active_action

                log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start using replication slot" "NOTICE"
                signal_to_remote "use_replication_slot.signal" "$CLUSTER_IP_REMOTE"
                
                log_message "Removing failed SSH interconnect signal" "NOTICE"
                if ! file_remove "$LOCK_FILE_DIR" "ssh_interconnect.lost"; then
                    exit_fatal "lost_interconnect_signal_removel_failed.fatal" "Failed to remove lost SSH interconnect signal"
                fi

                log_message "locking local script execution (remote lock)" "NOTICE"
                lock_script_custom "remote.lock"

                unlock_local_script_execution
                return 0
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
                            exit_fatal "promoting_aync_replica_to_sync_failed.fatal" "Failed to promote asynchronous replica to synchronous"
                        else
                            successfull_promotion=1
                        fi
                    fi
                fi

                if (( successfull_promotion == 1 )); then
                    log_message "Asynchronous replica is now synchronous" "SUCCESS"
                    log_message "Signaling voter to clean up after successfull recovery" "NOTICE"
                    signal_to_remote "system.restored" "$CLUSTER_IP_VOTER"
                fi
            fi

            unlock_local_script_execution
            return 0
        else
            # at lest one node is down

            if ! [[ -e "$LOCK_FILE_DIR"/state_negotiaton.block ]]; then

                local negotiation_exit_code
                negotiate_state "$local_postgres_status" "$remote_postgres_status"
                negotiation_exit_code="$?"

                if [[ "$negotiation_exit_code" -ne 0 ]]; then
                    unlock_local_script_execution
                    return 0
                fi
            fi

            lock_script_active_action

            if [[ "$local_postgres_status" -ne 0 ]]; then
                # remote node is down

                if [[ "$local_postgres_status" -eq 2 ]]; then
                    # remote master is down (action needed)
                    handle_ssh_remote_master_down
                else
                    if [[ -e "$LOCK_FILE_DIR"/ssh_interconnect.lost ]] && [[ -e "$LOCK_FILE_DIR"/local.promoted ]]; then
                        # Previous remote master needs rewinding after loosing network and local replica being promoted to new master
                        log_message "Removing failed SSH interconnect signal" "NOTICE"

                        if ! file_remove "$LOCK_FILE_DIR" "ssh_interconnect.lost"; then
                            exit_fatal "failed_to_remove_lost_interconnect_signal.fatal" "Failed to remove lost SSH interconnect signal"
                        fi

                        log_message "Signaling $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE to start rewind process" "NOTICE"
                        signal_to_remote "rewind.signal" "$CLUSTER_IP_REMOTE"

                        log_message "Unlocking script execution (master down) on $SYSTEM_ADMIN_USER@$CLUSTER_NAME_REMOTE" "NOTICE"
                        signal_to_remote "master_down.unlock" "$CLUSTER_IP_REMOTE"
                        
                        if ! master_replication_check "0"; then
                            exit_fatal "asyncronous_replication_on_master_didnt_start.fatal" "Asynchronous replication failed to start"
                        fi

                        unlock_script_custom "local.promoted"

                        unlock_local_script_execution
                        return 0
                    fi
                    

                    # remote replica is down (action needed)
                    handle_ssh_remote_replica_down
                fi
            else

                if [[ "$remote_postgres_status" -eq 0 ]]; then
                    # both nodes are down

                    check_previous_local_cluster_role
                    previous_local_postgres_role="$?"
                    if [[ "$previous_local_postgres_role" -eq 0 ]]; then
                        exit_fatal "no_previous_local_cluster_role.fatal" "Failed to retrieve previous local cluster role"
                    fi

                    if [[ "$previous_local_postgres_role" -eq 1 ]]; then
                        # both nodes down, local node was master (action needed)
                        handle_ssh_both_down_local_master
                    fi
                    
                    if [[ "$previous_local_postgres_role" -eq 2 ]]; then
                        # both nodes down, local node was replica (no action needed)
                        handle_ssh_both_down_local_replica
                    fi
                else
                    # local node is down

                    if [[ "$remote_postgres_status" -eq 1 ]]; then

                        check_previous_local_cluster_role
                        previous_local_postgres_role="$?"
                        if [[ "$previous_local_postgres_role" -eq 0 ]]; then
                            exit_fatal "no_previous_local_cluster_role.fatal" "Failed to retrieve previous local cluster role"
                        fi

                        if [[ "$previous_local_postgres_role" -eq 2 ]]; then
                            # local replica is down (action needed)
                            handle_ssh_local_replica_down
                        fi
                        
                        if [[ "$previous_local_postgres_role" -eq 1 ]]; then
                            # local master is down (no action needed)
                            handle_ssh_local_master_down
                        fi
                    else
                        # local master is down (no action needed)
                        handle_ssh_local_master_down
                    fi
                fi
            fi
        fi
    else
        # no ssh connection
        handle_no_ssh "$local_postgres_status"
    fi
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

    if [[ -e "$LOCK_FILE_DIR"/rewind.signal ]]; then
        # if signaled to reattach old master as replica
        react_to_rewind_signal
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/restart_replica.signal ]]; then
        # if signaled to restart replica
        react_to_restart_replica_signal
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/prepare_transition.signal ]]; then
        # if signaled to prepare replica to become synchronous
        react_to_prepare_transition_signal
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/restore_catchup.signal ]]; then
        # if signaled to restore replica from pg_basebackup to catch up to master
        react_to_restore_catchup_signal
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/clean_replication_start.signal ]]; then
        # if signaled to clean start time of syncronous replication
        react_to_clean_replicatino_start_signal
        return 1
    fi

    if [[ -e "$LOCK_FILE_DIR"/use_replication_slot.signal ]]; then
        # if signaled to use replication slot
        react_to_use_replication_slot_signal
        return 1
    fi

    return 0
}
#==========================================================================#


#===================================MAIN===================================#

main() {
    #file_remove "$LOCK_FILE_DIR" "remote_pg.status"
    #file_remove "$LOCK_FILE_DIR" "status.expired"

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
                wait "$background_monitor_pid"
            fi

            kill -s KILL "$background_monitor_pid" > /dev/null 2>&1

            if (( TERMINATE_REQUESTED == 1 )); then 
                exit 0
            fi
        fi

        if (( locks_and_signals_exit_code == 0 )); then

            local local_postgres_status
            local local_postgres_redo
            local remote_postgres_status
            local remote_postgres_redo

            local pg_status_return
            local pg_status_exit_code
            pg_status_return=$(check_postgresql "localhost" "$PGPORT" "$PG_ADMIN_USER" "$SYSTEM_ADMIN_USER" "$CLUSTER_IP_REMOTE")
            pg_status_exit_code="$?"

            if [[ "$pg_status_exit_code" -ne 0 ]]; then
                exit_fatal "pg_status_check_failed.fatal" "Failed to get PostgreSQL statuses"
            else
                local IFS=":"
                read -r local_postgres_status local_postgres_redo remote_postgres_status remote_postgres_redo <<< "$pg_status_return"
            fi

            if (( remote_postgres_status != 3 )) && (( local_postgres_status * remote_postgres_status != 0 )); then
                decision_maker "$local_postgres_status" "$local_postgres_redo" "$remote_postgres_status" "$remote_postgres_redo"

                inotifywait --event create "$LOCK_FILE_DIR" > /dev/null 2>&1 &
                local background_file_monitor_pid="$!"
                MONITORING_PIDS+=( "$background_file_monitor_pid" )

                check_postgresql_fault "localhost" > /dev/null 2>&1 &
                local local_postgresql_monitor_pid="$!"
                MONITORING_PIDS+=( "$local_postgresql_monitor_pid" )

                check_postgresql_fault "$CLUSTER_IP_REMOTE" > /dev/null 2>&1 &
                local remote_postgresql_monitor_pid="$!"
                MONITORING_PIDS+=( "$remote_postgresql_monitor_pid" )

                wait -n
                kill -s SIGTERM "$background_file_monitor_pid" > /dev/null 2>&1
                kill -s SIGTERM "$local_postgresql_monitor_pid" > /dev/null 2>&1
                kill -s SIGTERM "$remote_postgresql_monitor_pid" > /dev/null 2>&1

                if (( TERMINATE_REQUESTED == 1 )); then 
                    exit 0
                fi
            else
                decision_maker "$local_postgres_status" "$local_postgres_redo" "$remote_postgres_status" "$remote_postgres_redo"
            fi
        fi
    done
}

trap on_stop_cleanup TERM INT EXIT

main

#=================================TEMPLATE=================================#
#==========================================================================#
