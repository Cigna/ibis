    <action name="oozie_cb_ok">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>oozie_cb.sh</exec>
            <env-var>workflowName=${workflowName}</env-var>
            <env-var>HADOOP_CONF_DIR=/etc/hadoop/conf</env-var>
            <env-var>hdfs_ingest_path=${hdfs_ingest_path}</env-var>
            <file>${hdfs_ingest_path}oozie_cb.sh#oozie_cb.sh</file>
        </shell>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    <action name="oozie_cb_fail">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>oozie_cb.sh</exec>
            <env-var>workflowName=${workflowName}</env-var>
            <env-var>HADOOP_CONF_DIR=/etc/hadoop/conf</env-var>
            <env-var>hdfs_ingest_path=${hdfs_ingest_path}</env-var>
            <file>${hdfs_ingest_path}oozie_cb.sh#oozie_cb.sh</file>
        </shell>
        <ok to="kill"/>
        <error to="kill"/>
    </action>
