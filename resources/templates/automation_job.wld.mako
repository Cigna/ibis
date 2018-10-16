IF %ESPEVENT = 'ESPM2D.ADHOC' THEN JUMPTO ADHOC
APPL ${appl_id} POST_OLDEST WAIT JOB_ANCESTOR_WAIT
ADHOC:
INVOKE 'TTAT.U44.TEST.APPLLIB(<%text>$</%text>DEFAULT)'

<%text>$</%text>CKAGENT AGENT(${host_name_prefix})
IF %${host_name_prefix}_RES_AVAIL = '0' THEN QUIT
AGENT ${host_name_prefix}
RESOURCE ADD(2,THR_AGENT_${host_name_prefix})
RESOURCE ADD(2,THR_FAKE_MAINT)


LINUX_JOB ${first_wld_job.job_name}

   AGENT ${host_name_prefix}

   RUN ANYDAY

   SCRIPTNAME /opt/app/esp/${first_wld_job.script_name}

   USER fake_username

   RELEASE (${first_wld_job.next_job_name})

ENDJOB



APPLEND APPLEND1.${appl_id}

ENDJOB



APPLSTART APPLSTART1.${appl_id}

   RELEASE (${first_wld_job.job_name})

ENDJOB


% for wld_job in remaining_wld_jobs:
LINUX_JOB ${wld_job.job_name}

   AGENT ${host_name_prefix}

   RUN ANYDAY

   SCRIPTNAME /opt/app/esp/${wld_job.script_name}

   USER fake_username

   RELEASE (${wld_job.next_job_name})

ENDJOB

% endfor

