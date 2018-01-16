#!/bin/bash

# install Requirements
pip install -r ./ibis/requirements.pip

echo "*************"
pip show impyla
echo 
pip show thrift
echo "*************"

pip install coverage
pip install pylint==1.6.4
pip install nose -I
pip install bandit

mkdir -p ./test_logs

touch ./test_logs/sqoop.log
touch ./test_logs/action_node.log
touch ./test_logs/inventory.log

echo -e "<i><b>Test bold</b></i>"

# Run test cases and capture Coverage Report
nosetests ./ibis/ibis_test_suite.py --with-xunit

coverage run --branch --source="ibis" --omit="*test*,ibis/features/*,ibis/setup.py,ibis/__main__.py,*__init__.py*,ibis/import_version.py,ibis/ibis/settings.py,ibis/ibis/utilities/run_parallel.py,ibis/ibis/utilities/gitlab.py,ibis/ibis/ingest/import_prep.py" ./ibis/ibis_test_suite.py
coverage report
coverage xml
coverage html -d coverage-report


# Run codestyle and pylint to capture Code style violations
pip install pycodestyle
$( pycodestyle ibis | tee  pep8.out ) ||  echo 'find issues' | exit -1
pepper8 -o pep8_report.html pep8.out

cd ibis

pylint --rcfile=.pylintrc --output-format=parseable ibis lib features | tee pylint.out

cd ..


# Run Bandit for security check
bandit -f html -o bandit_results.html -r ibis -lll


#Build the egg
cd ibis
ls
pwd
python2.7 setup.py bdist_egg

cp requirements.pip ./dist
cp ibis-shell ./dist/
cp ibis_version.sh ./dist/
cp -R ./lib ./dist/lib

tar -czvf ibis.tar.gz -C ./dist .

