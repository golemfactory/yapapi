#!/bin/bash

SCRIPT_NAME=$0
LC_NUMERIC=C

DEFAULT_TIME="0"
DEFAULT_STDOUT_RATE="0"
DEFAULT_STDERR_RATE="0"
DEFAULT_EXIT_CODE="0"
DEFAULT_INTERVAL="0.25"

function usage() {
    echo "Usage:"
    echo "  $SCRIPT_NAME [-t time] [-o rate] [-e rate] [-c code] [-f path(,size)] [-i interval]"
    echo "  -h,--help      display help message"
    echo "  -t,--time      running time                   [default: 0]"
    echo "  -o,--stdout    stdout output rate (B/s)       [default: 0]"
    echo "  -e,--stderr    stderr output rate (B/s)       [default: 0]"
    echo "  -c,--code      exit code                      [default: 0]"
    echo "  -i,--interval  sleep interval in s            [default: 0.25]"
    echo "  -f,--file      generate output file (opt. with N random bytes)"
    exit 201
}

function read_tuple() {
    declare -n result=$2
    local f=$(echo $(echo $1 | cut -d',' -f1))
    local s=$(echo $(echo $1 | cut -d',' -f2 -s))
    result=($f $s)
}

function read_uint() {
    declare -n result=$2
    if [[ $1 =~ ^[0-9]+$ ]]; then
        result=$(num $1)
    else
        >&2 echo "invalid unsigned integer"
        exit 202;
    fi
}

function num() {
    [[ -z "$1" ]] && echo "0" || echo "$1"
}

function max() {
    local a=$(num $1)
    local b=$(num $2)
    (( $a > $b )) && echo "$a" || echo "$b"
}

function round() {
    local n=$(num $1)
    local f=$(echo "scale=1; $n + 0.5" | bc)
    echo ${f%.*}
}

function random_str() {
    tr -dc A-Za-z0-9 < /dev/urandom | head -c $1
}

function timestamp() {
    if [[ -z "$1" ]]; then
        date +"%s"
    else
        local now=$(date +"%s")
        echo $(( $now + $1 ))
    fi
}

function main() {
    local p_time=$DEFAULT_TIME
    local p_stdout=$DEFAULT_STDOUT_RATE
    local p_stderr=$DEFAULT_STDERR_RATE
    local p_code=$DEFAULT_EXIT_CODE
    local p_interval=$DEFAULT_INTERVAL
    local p_file

    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -h|--help) usage; exit 0 ;;
            -t|--time) read_uint $2 p_time; shift; shift ;;
            -o|--stdout) read_uint $2 p_stdout; shift; shift ;;
            -e|--stderr) read_uint $2 p_stderr; shift; shift ;;
            -c|--code) read_uint $2 p_code; shift; shift ;;
            -i|--interval) p_interval=$2; shift; shift ;;
            -f|--file) read_tuple $2 p_file; shift; shift ;;
            -*) usage ;;
        esac
    done

    local deadline=$( timestamp $p_time )
    local stdout_csz=$( round $(echo "scale=4; ${p_stdout} * $p_interval" | bc) )
    local stderr_csz=$( round $(echo "scale=4; ${p_stderr} * $p_interval" | bc) )

    if [[ ! -z $p_file ]]; then
        mkdir -p $(dirname "${p_file[0]}")
        echo -n $( random_str $(num ${p_file[1]}) ) > "${p_file[0]}"
    fi

    while true; do
        if [[ $stdout_csz -gt 0 ]]; then
            echo -n $(random_str $stdout_csz)
        fi
        if [[ $stderr_csz -gt 0 ]]; then
            >&2 echo -n $(random_str $stderr_csz)
        fi
        if [[ $(timestamp) -ge $deadline ]]; then
            break
        fi
        sleep $p_interval
    done

    exit $p_code
}

main "$@"
