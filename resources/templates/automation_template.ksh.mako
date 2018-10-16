##############################################################
# Script to run Oozie workflows through ESP
# Returns success / failure on completion
##############################################################

echo "
Welcome to...
    ******************
    ******************
    **** I B I S *****
    ******************
    ******************
"

log_trace () {
    echo "<%text>$</%text>(date) [TRACE] : <%text>$</%text>@" >&2
}

log_debug () {
    echo "<%text>$</%text>(date) [DEBUG] : <%text>$</%text>@" >&2
}

log_info () {
    echo "<%text>$</%text>(date) [INFO]  : <%text>$</%text>@" >&2
}

log_warn () {
    echo "<%text>$</%text>(date) [WARN]  : <%text>$</%text>@" >&2
}

log_error () {
    echo "<%text>$</%text>(date) [ERROR] : <%text>$</%text>@" >&2
}

log_fatal () {
    echo "<%text>$</%text>(date) [FATAL] : <%text>$</%text>@" >&2
}

kinit_oozie() {

    kinit -S krbtgt/${kerberos_realm}@${kerberos_realm} fake_username@${kerberos_realm} -k -t ~/fake.keytab

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
    log_info "klist_status - klist output: <%text>$</%text>{klist_status}"
    echo $exitCode
}

oozie_setup() {
    # run the Oozie workflow
    OOZIE=${oozie_url}
    CONFIG="${saved_loc}${job_name}_job.properties"
}

run_oozie_job() {

    OOZIE_OUTPUT=<%text>$</%text>(oozie -Doozie.auth.token.cache=false job -oozie <%text>$</%text>{OOZIE} -DREDUCER_SLEEP_TIME=15 -config <%text>$</%text>{CONFIG} -run)

    # output when a job starts is "job: 00001000001000"
    if echo <%text>$</%text>{OOZIE_OUTPUT} | grep -q "job:"; then
        log_info "Workflow has started as job ID <%text>$</%text>{OOZIE_OUTPUT}"
        JOBID=`echo -e <%text>$</%text>{OOZIE_OUTPUT} | awk -F ': ' '{print <%text>$</%text>2}'`;
    else
        log_fatal "The job has not started correctly. <%text>$</%text>{OOZIE_OUTPUT}"
        exit 1
    fi
}

poll_every_5() {
    ALL_OOZIE_STATUS=("SUCCEEDED" "FAILED" "KILLED" "PREP" "DONEWITHERROR" "PAUSED" "PAUSEDWITHERROR" "PREMATER" "PREPPAUSED" "PREPSUSPENDED" "RUNNINGWITHERROR" "SUSPENDED" "SUSPENDEDWITHERROR")

    STATUS=<%text>$</%text>(oozie job -oozie <%text>$</%text>OOZIE -info <%text>$</%text>JOBID | grep -e "Status        :" | awk -F ' : ' '{print <%text>$</%text>2}')

    # Make sure that the workflow is in one of the known Oozie status's
    while [[ "<%text>$</%text>{ALL_OOZIE_STATUS[@]}" =~ "<%text>$</%text>{STATUS}" ]]; do
        ### Poll oozie every 5 seconds for job status and exit when job is done
        sleep 5
        # Check if the job status is one of the known job status
        if [[ "<%text>$</%text>{ALL_OOZIE_STATUS[@]}" =~ " <%text>$</%text>{STATUS} " ]]; then
               log_error "Workflow is <%text>$</%text>{STATUS} status which is not a valid status, requires manual intervention"
               exit 3
        fi
        # I don't know what "Premater" means either, but it's documented here:
        # http://www.slideshare.net/mislam77/oozie-summit-2011
        NONE_COMPLETED_STATUS=("DONEWITHERROR" "PAUSED" "PAUSEDWITHERROR" "PREMATER" "PREPPAUSED" "PREPSUSPENDED" "RUNNINGWITHERROR" "SUSPENDED" "SUSPENDEDWITHERROR")
        PREP_COUNTER=1

        STATUS=<%text>$</%text>(oozie job -oozie <%text>$</%text>OOZIE -info <%text>$</%text>JOBID | grep -e "Status        :" | awk -F ' : ' '{print <%text>$</%text>2}')

        if [ "<%text>$</%text>STATUS" == "PREP" ]; then
            log_info "Job starting up. It has been in this step for <%text>$</%text>{PREP_COUNTER} run-throughs"
            let PREP_COUNTER=PREP_COUNTER+1

            if [ <%text>$</%text>PREP_COUNTER -gt 10 ]; then
                log_error "Job has been in prep step for <%text>$</%text>{PREP_COUNTER} rounds. Manual intervention required."
                exit 4
            fi
        fi

        # Check if job is still running or not
        if [ "<%text>$</%text>{STATUS}" != "RUNNING" ]; then

           # If job is not running, check if it's in a complete-type step
           if [ "<%text>$</%text>{STATUS}" == "SUCCEEDED" ]; then
                   log_info "Workflow <%text>$</%text>{STATUS}"
                   exit 0
           fi

           if [ "<%text>$</%text>{STATUS}" == "FAILED" ]; then
                   log_info "Workflow <%text>$</%text>{STATUS}"
                   exit 1
           fi

           if [ "<%text>$</%text>{STATUS}" == "KILLED" ]; then
                   log_info "Workflow <%text>$</%text>{STATUS}"
                   exit 2
           fi

           if [[ "<%text>$</%text>{NONE_COMPLETED_STATUS[@]}" =~ " <%text>$</%text>{STATUS} " ]]; then
               log_error "Workflow is <%text>$</%text>{STATUS}, requires manual intervention"
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