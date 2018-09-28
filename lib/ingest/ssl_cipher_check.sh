#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf

# Set props for the initial container
hadoop fs -get /user/dev/blacklistJDK/disabledAlgorithms.security
hadoop fs -get /user/dev/oozie/share/lib/sqoop/cipher-check.jar

export HADOOP_OPTS="-Dmapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION -Djava.security.properties=disabledAlgorithms.security"
echo "Hadoop Opts = $HADOOP_OPTS"

echo "Runinng cipher-check ....."
java $HADOOP_OPTS -jar ./cipher-check.jar

if [ $? -eq 0 ]
then
  echo "No weak ciphers enabled."
else
  echo "Weak ciphers detected, aborting import." >&2
  exit 1
fi
