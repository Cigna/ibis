# !/bin/bash

# Prerequisites:
# 1. Python 2.7 with PIP
# 2. IBIS dependencies (refer IBIS's requirements.pip)
#
#
# This setup.sh to be executed once when installing IBIS for the first time.
# This script does the following:
#   1. Creates required directories in the local file system and in the HDFS as well.
#   2. Install python's libraries required for IBIS
#   3. Calls DDL statements to create a Hive database and with required 
#      audit and transactional tables.


execute_ddl() {
	# Execute DDL statements
	echo "Executing DDLs..."

	# Calls config_env.sh to get Hive JDBC URL and Kerberos Pricipal 
	source ./lib/ingest/$env/config_env.sh
	
	beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -f './resources/ibis.hql' 
}

verify_hadoop_installation() {
	hadoop version
	rc=$?
	if [ $rc -ne 0 ]; then echo "Error: Hadoop not found!"; exit $rc; fi
}

validate_python_version() {
	echo "Checking Python version..."
	unsupported_version=false
	supported_major_ver="2"

	pyver=`python -c 'import sys; print sys.version_info[:]'`
	echo "Current Python version: ${pyver}"
	IFS=','
	pyver=(${pyver:1:${#pyver}-2})
	major_ver=${pyver[0]}
	minor_ver=${pyver[1]}
	micro_ver=${pyver[2]}
	unset IFS

	if [ "$major_ver" -eq "$supported_major_ver" ]; then
		if [ $minor_ver -lt 7 ]; then
			unsupported_version=true
		fi
	else
		unsupported_version=true
	fi

	if $unsupported_version; then
		echo "Unsupported Python version. Require 2.7.x"
		exit 1
	fi
}

check_prerequisites() {
	echo "Checking prerequisites..."
	verify_hadoop_installation
	validate_python_version	

}

install_py_libs() {
	python -m pip install -r ./requirements.pip
	if [ $? -ne 0 ]; then 
		echo "Error: Couldn't able to install IBIS dependencies via PIP!"
		exit 1
	fi
}

make_unix_dir() {
	mkdir -p $1
}

make_hdfs_dir() {
	hadoop fs -mkdir -p $1
}

property_parser() {
	echo "Parsing properties..."
	# Calls python utility to parse the IBIS properties
	properties=(`python ./config_parser.py $prop_file Directories`)
	echo "List of IBIS properties: ${properties[@]}"

	# Iterates all properties
	for property in "${properties[@]}"
	do
		IFS='='
		set -- $property

		if [[ $1 == *'hdfs'* ]]; then
			# Creates HDFS directory
			make_hdfs_dir $2
		else
			# Creates local directory
			make_unix_dir $2
		fi
	done

}

main() {
	# Validates the prerequisites
	check_prerequisites

	# Installs the required Python libraries
	install_py_libs
	
	# Prompts user for environment
	echo
	read -p "Specify the environment (dev/int/pvs/prod): " env
	prop_file="./resources/$env.properties"
	echo "Property File: $prop_file"

	# Creates local and HDFS directories as mentioned in the properties
	if [ -f "$prop_file" ]; then
		# Parses IBIS properties to create required directories
		property_parser
	else
		echo "Error: Property file is not found!"
		exit 1
	fi

	# Executes DDL statements
	execute_ddl
 
	
}

main