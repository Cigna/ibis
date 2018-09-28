# !/bin/bash

# Prerequisites:
# 1. Unix like environment (Unix, Linux, Mac)
# 2. Hadoop eco-systems
# 3. Python 2.7
#
#
# This setup.sh to be executed once when installing IBIS for the first time.
# This script does the following:
#	1. Validates the Operating System. Fails if it's not a Unix like 
#	   environment (linx, AIX, Mac)
#   2. Checks Hadoop installation.
#	3. Checks the installed Python version. Fails if version is not as 2.7  
#   4. Creates the required directories in the local file system and 
#      in the HDFS as well.
#   5. Installs Python's libraries required for IBIS
#   6. Calls DDL statements to create a Hive database and with required 
#      audit and transactional tables.


validate_os() {
	unameOut="$(uname -s)"

	case "${unameOut}" in
	    Linux*)     os=Linux;;
	    Darwin*)    os=Mac;;
	    GNU*)       os=GNU;;
	    FreeBSD*)   os=FreeBSD;;
	    AIX*)       os=AIX;;
	    *)          os="Unknown"
	esac

	echo "Operating System: $os"

	if [ os == "Unknown" ]; then
		echo "Error: Unsupported Operating System! Require a Unix like environment (Linux, Mac)"
		exit 1
	fi
}

execute_ddl() {
	# Execute DDL statements
	echo "Executing DDLs..."

	# Calls config_env.sh to get Hive JDBC URL and Kerberos Pricipal 
	source ./lib/ingest/$env/config_env.sh
	
	beeline /etc/hive/beeline.properties -u ${hive2_jdbc_url}\;principal=hive/${KERBEROS_PRINCIPAL} --hiveconf mapred.job.queue.name=$queueName --silent=true --showHeader=false --outputformat=csv2 -f './resources/ibis.hql'

	if [ $? -ne 0 ]; then 
		echo "Error: Not able to execute the DDL statements required for IBIS!"
		exit $rc
	fi
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
	validate_os
	verify_hadoop_installation
	validate_python_version	
	sudo yum groupinstall -y "development tools"
	sudo yum install krb5-devel gcc zlib-devel gcc-c++ python-devel cyrus-sasl-devel openssl openssl-devel libffi-devel bzip2-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel snappy-devel expat-devel patch -y --skip-broken
	sudo  yum --nogpgcheck -y install unzip sudo vim wget which tar gzip graphviz python-setuptools python-setuptools-devel shadow-utils git net-tools libXtst.x86_64
}

install_py_libs() {
	sudo pip install -r ./requirements.pip
	
	if [ $? -ne 0 ]; then 
		echo "Error: Couldn't install IBIS dependencies via PIP!"
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
	read -p "Specify the environment (dev/int/prod): " env
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
 
 	exit 0	
}

main