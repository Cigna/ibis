    <start to="import_prep_${it_table_obj.clean_db_table_name}"/>
    <action name="import_prep_${it_table_obj.clean_db_table_name}">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>incr_import_prep.sh</exec>
            <env-var>source_database_name=${it_table_obj.database}</env-var>
            <env-var>source_table_name=${it_table_obj.table_name}</env-var>
            <env-var>db_env=${it_table_obj.db_env}</env-var>
            <env-var>target_dir=${it_table_obj.target_dir}</env-var>
            <env-var>hive2_jdbc_url=<%text>$</%text>{hive2_jdbc_url}</env-var>
            <env-var>queueName=<%text>$</%text>{queueName}</env-var>
            <env-var>incremental_mode=${incremental_mode}</env-var>
            <env-var>it_table=${it_table}</env-var>
            <env-var>it_table_host=${it_table_host}</env-var>
            <env-var>hdfs_ingest_path=${hdfs_ingest_path}</env-var>
            <file>${hdfs_ingest_path}incr_import_prep.sh</file>
            <capture-output/>
        </shell>
        <ok to="incr_import_${it_table_obj.clean_db_table_name}"/>
        <error to="${error_to}"/>
    </action>
    ${sqoop_xml}
    <decision name="check_if_empty_${it_table_obj.clean_db_table_name}">
        <switch>
            <case to="incr_post_import_${it_table_obj.clean_db_table_name}">
                <%text>$</%text>{fs:exists('/user/data/ingest/${it_table_obj.target_dir}/part-m-00000.avro')}
            </case>
            <default to="oozie_cb_ok"/>
        </switch>
    </decision>
    <action name="incr_post_import_${it_table_obj.clean_db_table_name}">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <exec>incr_post_import.sh</exec>
            <env-var>target_dir=${it_table_obj.target_dir}</env-var>
            <env-var>hive2_jdbc_url=<%text>$</%text>{hive2_jdbc_url}</env-var>
            <env-var>hdfs_ingest_path=${hdfs_ingest_path}</env-var>
           <file>${hdfs_ingest_path}incr_post_import.sh</file>
        </shell>
        <ok to="incr_create_table_${it_table_obj.clean_db_table_name}"/>
        <error to="${error_to}"/>
    </action>
    <action cred="hive2" name="incr_create_table_${it_table_obj.clean_db_table_name}">
        <hive2 xmlns="uri:oozie:hive2-action:0.1">
            <jdbc-url><%text>$</%text>{hive2_jdbc_url}</jdbc-url>
            <script>/user/data/${it_table_obj.target_dir}/gen/incr_create_table.hql</script>
        </hive2>
        <ok to="incr_quality_assurance_${it_table_obj.clean_db_table_name}"/>
        <error to="${error_to}"/>
    </action>
    ${qa_xml}
    <action cred="hive2" name="incr_merge_partition_${it_table_obj.clean_db_table_name}">
        <hive2 xmlns="uri:oozie:hive2-action:0.1">
            <jdbc-url><%text>$</%text>{hive2_jdbc_url}</jdbc-url>
            <script>/user/data/${it_table_obj.target_dir}/gen/incr_merge_partition.hql</script>
        </hive2>
        <ok to="${merge_ok_to}"/>
        <error to="${error_to}"/>
    </action>
    ${views_xml}