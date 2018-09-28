<%namespace name="hive2" file="hive.xml.mako" />
    <action cred="hive2" name="${node.get_name()}">
        <hive2 xmlns="uri:oozie:hive2-action:0.1">
            % if node.get_job_tracker():
            <job-tracker>${node.get_job_tracker()}</job-tracker>
            % endif
            % if node.get_name_node():
            <name-node>${node.get_name_node()}</name-node>
            % endif
            % if node.get_delete() or node.get_mkdir():
            <prepare>
                % for delete in node.get_delete():
                <delete path = "${delete}"/>
                % endfor
                % for mkdir in node.get_mkdir():
                <mkdir path = "${mkdir}"/>
                % endfor
            </prepare>
            % endif
            % if node.get_job_xml():
                % for job_xml in node.get_job_xml():
            <job-xml>${job_xml}</job-xml>
                % endfor
            % endif
             % if node.get_config():
            <configuration>
                % for config in node.get_config():
                    % for key, value in config.iteritems():
                <property>
                    <name>${key}</name>
                    <value>${value}</value>
                </property>
                    % endfor
                % endfor
            </configuration>
            % endif
            <jdbc-url><%text>$</%text>{hive2_jdbc_url}</jdbc-url>
            <script>${node.get_script()}</script>
            % if node.get_param():
                % for param in node.get_param():
                <param>${param}</param>
                % endfor
            % endif
            % if node.get_file():
            % for fle in node.get_file():
            <file>${fle}</file>
            % endfor
            % endif
            % if node.get_archive():
                % for archive in node.get_archive():
            <archive>${archive}</archive>
                % endfor
            % endif
        </hive2>
        <ok to="${node.get_ok()}"/>
        <error to="${node.get_error()}"/>
    </action>