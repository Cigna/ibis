    <action name="oozie_cb_ok">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>export_cb.sh</exec>
            <env-var>workflowName=${workflowName}</env-var>
            <env-var>HADOOP_CONF_DIR=/etc/hadoop/conf</env-var>
            <env-var>hdfs_export_path=${hdfs_export_path}</env-var>
            <file>${hdfs_export_path}export_cb.sh#export_cb.sh</file>
        </shell>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    <action name="oozie_cb_fail">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>export_cb.sh</exec>
            <env-var>workflowName=${workflowName}</env-var>
            <env-var>HADOOP_CONF_DIR=/etc/hadoop/conf</env-var>
            <env-var>hdfs_export_path=${hdfs_export_path}</env-var>
            <file>${hdfs_export_path}export_cb.sh#export_cb.sh</file>
        </shell>
        <ok to="kill"/>
        <error to="kill"/>
    </action>
    <kill name="kill">
        <message>Action failed, error message[<%text>$</%text>{wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
