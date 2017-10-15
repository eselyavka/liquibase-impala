#!/bin/bash

set -Cvx

if [[ -n "$2" ]] ; then
    export PATH="$2:${PATH}"
fi

function run_impala() {
    echo "runnig impala"
    impala-shell -i localhost -V -q "DROP DATABASE IF EXISTS impala_test CASCADE; CREATE DATABASE impala_test;" || exit 1
    liquibase --logLevel=debug update || exit 1
    liquibase --logLevel=debug tag 'testTag' || exit 1
    liquibase --logLevel=debug rollback 'start' || exit 1
}

function run_hive() {
    echo "running hive"
    beeline -u jdbc:hive2://localhost:10000/default -n "$(whoami)" -e "DROP DATABASE IF EXISTS hive_test CASCADE; CREATE DATABASE hive_test;" || exit 1
    liquibase --logLevel=debug --defaultsFile=<(sed s/@@USER@@/"$(whoami)"/g liquibase-hive.properties) update || exit 1
    liquibase --logLevel=debug --defaultsFile=<(sed s/@@USER@@/"$(whoami)"/g liquibase-hive.properties) tag 'testTag' || exit 1
    liquibase --logLevel=debug --defaultsFile=<(sed s/@@USER@@/"$(whoami)"/g liquibase-hive.properties) rollback 'start' || exit 1
}

function main() {
    case "$1" in
        hive*)
            run_hive
            ;;
        impala*)
            run_impala
            ;;
        both*)
            run_hive
            run_impala
            ;;
        *)
            echo "Unsupported argument [hive, impala, both]"
            exit 1
            ;;
    esac
}

main "$1"

exit 0
