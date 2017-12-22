input {
  teamName = "${input_obj.team_name}"
  frequencyRequested = "${input_obj.frequency_requested}"
  envToRunIn = "${input_obj.env_to_run_in}"
  jobType = "${input_obj.job_type}"
  dayOrDateToRun = "${input_obj.day_or_date_to_run}"
  timeToRun = "${input_obj.time_to_run}"
  agent = "${input_obj.agent}"
  userId = "${input_obj.user_id}"
  espGroup = "${input_obj.esp_group}"
  appl = "${input_obj.appl}"

  jobs = [
    % for job in ${input_obj.jobs}:
    {
      jobName = "${job.job_name}"
      scriptLocation = "${job.script_location}"
      goesToNext = ["${job.goes_to_next}"]
    }
    % endfor
  ]
}