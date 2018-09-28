<workflow-app name="${workflowName}" xmlns="uri:oozie:workflow:0.4">
    <global>
        <job-tracker><%text>$</%text>{jobTracker}</job-tracker>
        <name-node><%text>$</%text>{nameNode}</name-node>
        <configuration>
            <property>
                <name>mapred.job.queue.name</name>
                <value><%text>$</%text>{queueName}</value>
            </property>
        </configuration>
    </global>
    <credentials>
        <credential name="hive2" type="hive2">
            <property>
                <name>hive2.jdbc.url</name>
                <value><%text>$</%text>{hive2_jdbc_url}</value>
            </property>
            <property>
                <name>hive2.server.principal</name>
                <value><%text>$</%text>{hive2_principal}</value>
            </property>
            <property>
                <name>jdbc-url</name>
                <value><%text>$</%text>{hive2_jdbc_url}</value>
            </property>
        </credential>
    </credentials>