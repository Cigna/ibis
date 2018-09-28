#!/bin/bash

ibis_home=$1
args=$2
cur_dir=`pwd`
echo 'Argument 1 : ibis_home: ' $ibis_home
echo 'Argument 2 : Execution option: ' $args
export ibis_home=$ibis_home
export cur_dir=$cur_dir

print_args(){
        echo '========================ATTENTION===================================='
        echo 'Argument 1 : Needs to be IBIS home directory path'
		echo 'Argument 2 : Below are the acceptable argument values'
		echo '			   Blank to have Tests, Code check and Build process to execute'
        echo '             skip-all-test - Skip all validation and create Egg'
        echo '             skip-code-check - Skip code style check'
        echo '             skip-build - Run all validations and skip build process'
        echo '====================================================================='
}

setup_env() {
	# install Requirements 
	# pip install -r $ibis_home/requirements.pip
	
	echo "*************"
	pip show impyla
	echo
	pip show thrift
	echo "*************"
	
	pip install bandit
	
	mkdir -p ./test_logs
	
	touch ./test_logs/sqoop.log
	touch ./test_logs/action_node.log
	touch ./test_logs/inventory.log
}

run_unit_tests() {
	
	echo -e "<i><b>Test bold</b></i>"
	# Run test cases and capture Coverage Report
	nosetests $ibis_home/ibis_test_suite.py --with-xunit
    let EXIT_STATUS_PY=$?
	if [ "$EXIT_STATUS_PY" -gt 0 ]
	then
		echo ' test cases failed'
		exit 1
	fi
	coverage run --branch --source="ibis" --omit="*test*,ibis/features/*,ibis/setup.py,ibis/__main__.py,*__init__.py*,ibis/import_version.py,ibis/ibis/settings.py,ibis/ibis/utilities/run_parallel.py,ibis/ibis/utilities/gitlab.py,ibis/ibis/ingest/import_prep.py" $ibis_home/ibis_test_suite.py
	coverage report
	coverage xml
	coverage html -d coverage-report
	rm -rf $cur_dir/test_logs
	rm -rf $cur_dir/test_workflows-git
	rm -rf $cur_dir/test_files
	
	mv $ibis_home/test_logs $cur_dir/
	mv $ibis_home/test_workflows-git $cur_dir/
	mv $ibis_home/test_files $cur_dir/
}

check_code_quality() {

	# Run codestyle and pylint to capture Code style violations
	$( pycodestyle $ibis_home/ibis | tee  pep8.out ) ||  echo 'find issues' | exit -1
	pepper8 -o pep8_report.html pep8.out
	
	pylint --rcfile=.pylintrc --output-format=parseable $ibis_home/ibis $ibis_home/lib $ibis_home/features | tee pylint.out
}

check_security(){
	# Run Bandit for security check
	cd $ibis_home
	cd ..
	ibis_pre_dir=`pwd`
	bandit -f html -o bandit_results.html -r $ibis_pre_dir/ibis -lll
	cd $cur_dir
}

build_egg(){

	#Build the egg
	
	cd $ibis_home
	python2.7 $ibis_home/setup.py bdist_egg
    let EXIT_STATUS_PY=$?
	if [ "$EXIT_STATUS_PY" -gt 0 ]
	then
		echo ' build failed'
		exit 1
	fi
	rm -rf $cur_dir/dist
	rm -rf $cur_dir/opensource_ibis.egg-info
	rm -rf $cur_dir/build
	mv $ibis_home/dist $cur_dir
	mv $ibis_home/opensource_ibis.egg-info $cur_dir
	mv $ibis_home/build $cur_dir
	cp requirements.pip $cur_dir/dist
	cp ibis-shell $cur_dir/dist
	cp ibis_version.sh $cur_dir/dist
	cp -R ./lib $cur_dir/dist/lib

	tar -czvf $cur_dir/ibis.tar.gz -C $cur_dir/dist .
	cd $cur_dir

}

main() {

    if [ -e "$ibis_home/setup.py" ]; then
        setup_env
    else
        echo 'Argument 1 is not a IBIS direcotry ' $ibis_home
		print_args
        exit 1
    fi
	
	if [ "${args}" == "" ]; then
		run_unit_tests
		check_code_quality
		check_security
		build_egg
	elif [ "${args}" == "skip-all-test" ]; then
		build_egg
	elif [ "${args}" == "skip-code-check" ]; then
		run_unit_tests
		build_egg
	elif [ "${args}" == "skip-build" ]; then
		run_unit_tests
		check_code_quality
		check_security
	else
		print_args
		exit 1
	fi

}

main

# WRITE CODE ABOVE THIS LINE
# WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING WARNING
