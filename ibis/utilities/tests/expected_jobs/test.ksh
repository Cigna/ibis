# Script to run Oozie workflows through ESP
# Returns success / failure on completion

echo "
Welcome to...
    ******************
    ******************
    **** I B I S *****
    ******************
    ******************
"

log_trace () {
    echo "$(date) [TRACE] : $@" >&2
}

log_debug () {
    echo "$(date) [DEBUG] : $@" >&2
}

log_info () {
    echo "$(date) [INFO]  : $@" >&2
}

log_warn () {
    echo "$(date) [WARN]  : $@" >&2
}

log_error () {
    echo "$(date) [ERROR] : $@" >&2
}

log_fatal () {
    echo "$(date) [FATAL] : $@" >&2
}

kinit_oozie() {

    kinit -S krbtgt/fake.kerberos@fake.kerberos fake_username@fake.kerberos -k -t ~/fake.keytab

    if [ $? -ne 0 ]; then
        log_error "Kinit failed"
        exit 1
    else
        log_info "Kinited as fake_username"
    fi
}

klist_status() {
    klist_status=$(klist)
    exitCode=$?
    log_info "klist_status - klist output: ${klist_status}"
    echo $exitCode
}

oozie_setup() {
    # run the Oozie workflow
    OOZIE=http://fake.dev.oozie:25007/oozie
    CONFIG="/test/save/location/test_workflows/test_workflow_job.properties"
}

run_oozie_job() {

    OOZIE_OUTPUT=$(oozie -Doozie.auth.token.cache=false job -oozie ${OOZIE} -DREDUCER_SLEEP_TIME=15 -config ${CONFIG} -run)

    # output when a job starts is "job: 00001000001000"
    if echo ${OOZIE_OUTPUT} | grep -q "job:"; then
        log_info "Workflow has started as job ID ${OOZIE_OUTPUT}"
        JOBID=`echo -e ${OOZIE_OUTPUT} | awk -F ': ' '{print $2}'`;
    else
        log_fatal "The job has not started correctly. ${OOZIE_OUTPUT}"
        exit 1
    fi
}

poll_every_5() {
    ALL_OOZIE_STATUS=("SUCCEEDED" "FAILED" "KILLED" "PREP" "DONEWITHERROR" "PAUSED" "PAUSEDWITHERROR" "PREMATER" "PREPPAUSED" "PREPSUSPENDED" "RUNNINGWITHERROR" "SUSPENDED" "SUSPENDEDWITHERROR")

    STATUS=$(oozie job -oozie $OOZIE -info $JOBID | grep -e "Status        :" | awk -F ' : ' '{print $2}')

    # Make sure that the workflow is in one of the known Oozie status's
    while [[ "${ALL_OOZIE_STATUS[@]}" =~ "${STATUS}" ]]; do
        sleep 5
        # Check if the job status is one of the known job status
        if [[ "${ALL_OOZIE_STATUS[@]}" =~ " ${STATUS} " ]]; then
               log_error "Workflow is ${STATUS} status which is not a valid status, requires manual intervention"
               exit 3
        fi
        # I don't know what "Premater" means either, but it's documented here:
        # http://www.slideshare.net/mislam77/oozie-summit-2011
        NONE_COMPLETED_STATUS=("DONEWITHERROR" "PAUSED" "PAUSEDWITHERROR" "PREMATER" "PREPPAUSED" "PREPSUSPENDED" "RUNNINGWITHERROR" "SUSPENDED" "SUSPENDEDWITHERROR")
        PREP_COUNTER=1

        STATUS=$(oozie job -oozie $OOZIE -info $JOBID | grep -e "Status        :" | awk -F ' : ' '{print $2}')

        if [ "$STATUS" == "PREP" ]; then
            log_info "Job starting up. It has been in this step for ${PREP_COUNTER} run-throughs"
            let PREP_COUNTER=PREP_COUNTER+1

            if [ $PREP_COUNTER -gt 10 ]; then
                log_error "Job has been in prep step for ${PREP_COUNTER} rounds. Manual intervention required."
                exit 4
            fi
        fi

        # Check if job is still running or not
        if [ "${STATUS}" != "RUNNING" ]; then

           # If job is not running, check if it's in a complete-type step
           if [ "${STATUS}" == "SUCCEEDED" ]; then
                   log_info "Workflow ${STATUS}"
                   exit 0
           fi

           if [ "${STATUS}" == "FAILED" ]; then
                   log_info "Workflow ${STATUS}"
                   exit 1
           fi

           if [ "${STATUS}" == "KILLED" ]; then
                   log_info "Workflow ${STATUS}"
                   exit 2
           fi

           if [[ "${NONE_COMPLETED_STATUS[@]}" =~ " ${STATUS} " ]]; then
               log_error "Workflow is ${STATUS}, requires manual intervention"
               exit 3
           fi
        fi

        # Check if valid kerberos ticket exist
        klistExitCode=$(klist_status)
        if [ $klistExitCode -gt 0 ]; then
            echo '##################################'
            echo 'Kinit again!'
            kinit_oozie
            klistExitCode=$(klist_status)
            if [ $klistExitCode -gt 0 ]; then
                echo 'klist failed even after kinit again! So exiting.'
                exit 3
            fi
            echo '##################################'
        fi

    done
}

main() {
    echo "Running the workflow..."
    kinit_oozie
    oozie_setup
    run_oozie_job
    poll_every_5
}

main
