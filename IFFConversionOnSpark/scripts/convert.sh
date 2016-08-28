#!/usr/bin/env bash
usage()
{
  echo "convert.sh <account-date> <config-file-path> <metadata-file-path> <iff-file-input-path> <dat-file-output-path>"
}
init()
{
	CONFIG_FILE="`pwd`/config.sh"
	source ${CONFIG_FILE}
}
run()
{
COMMAND="${SPARK_HOME}/bin/spark-submit \
====--class \"com.boc.iff.IFFConvertApp\" \
====--master ${SPARK_MASTER} \
====--executor-memory ${EXECUTOR_MEMORY} \
====${JAR_FILE} \
====--account-date \"${ACCOUNT_DATE}\" \
====--config-file-path \"${CONFIG_FILE_PATH}\" \
====--metadata-file-path \"${METADATA_FILE_PATH}\" \
====--iff-file-input-path \"${IFF_FILE_INPUT_PATH}\" \
====--dat-file-output-path \"${DAT_FILE_OUTPUT_PATH}\" \
====--log-detail \
====--repartition 2"
COMMAND=`echo "${COMMAND}"|sed 's/=//g'`
echo "${COMMAND}"
echo "${COMMAND}"|bash
}
main()
{
	init
	run
}
if [ $# -ne 5 ]
then
	usage
	exit 1
fi
ACCOUNT_DATE=$1
CONFIG_FILE_PATH=$2
METADATA_FILE_PATH=$3
IFF_FILE_INPUT_PATH=$4
DAT_FILE_OUTPUT_PATH=$5
main
