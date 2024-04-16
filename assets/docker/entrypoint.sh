#!/bin/sh

DATA="/data"

if [ -z $1 ]; then
    echo "!ERR: no dataset name provided!"
    exit 1
fi

DS="$1"
LOG="${DATA}/${DS}/${DS}.log"

echo "BEGIN[`date`] ${*}" >> ${LOG}

if [ ! -d ${DATA}/${DS} ]; then
    echo "!ERR: dataset directory doesn't exist!" 2>&1 | tee -a ${LOG}
    exit 2
fi

if [ ! -d ${DATA}/${DS}/RDF ]; then
    mkdir -p ${DATA}/${DS}/RDF 2>&1 | tee -a ${LOG}
fi

if [ ! -f ${DATA}/${DS}/RDF/${DS}.hdt ]; then
    echo "?INF: HDT doesn't exist yet" 2>&1 | tee -a ${LOG}

    if [ ! -f ${DATA}/${DS}/RDF/${DS}.nq ]; then
        echo "?INF: NQ doesn't exist yet" 2>&1 | tee -a ${LOG}

        if [ ! -d ${DATA}/${DS}/CSV ]; then
            echo "!ERR: dataset CSV directory doesn't exist!" 2>&1 | tee -a ${LOG}
            exit 3
        fi

        # turn CSV into NQ
        echo "?INF: generate NQ"  2>&1 | tee -a ${LOG}
        python3 /app/convert-to-RDF.py ${DATA}/${DS}/CSV ${DATA}/${DS}/RDF 2>&1 | tee -a ${LOG}
        ERR="$?"
        if [ ${ERR} -ne 0 ]; then
            exit ${ERR}
        fi

        cat ${DATA}/${DS}/RDF/*.nq > ${DATA}/${DS}/RDF/${DS}.nq
    fi

    # turn NQ into HDT
    echo "?INF: generate HDT"  2>&1 | tee -a ${LOG}
    java -jar burgerLinker-0.0.1-SNAPSHOT-jar-with-dependencies.jar --inputData ${DATA}/${DS}/RDF/${DS}.nq --outputDir ${DATA}/${DS}/RDF --function convertToHDT 2>&1 | tee -a ${LOG}
    ERR="${?}"
    if [ ${ERR} -ne 0 ]; then
        exit ${ERR}
    fi
fi

# get rid of $DS in $1
shift

java -jar burgerLinker-0.0.1-SNAPSHOT-jar-with-dependencies.jar --inputData ${DATA}/${DS}/RDF/${DS}.hdt --outputDir ${DATA}/${DS} $* 2>&1 | tee -a ${LOG}
ERR="${?}"
echo " END [`date`] ${*}" >> ${LOG}

exit $ERR