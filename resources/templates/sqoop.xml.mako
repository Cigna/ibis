<%namespace name="shell" file="shell.xml.mako" />
    <action name="${node.get_name()}">
        <sqoop xmlns="uri:oozie:sqoop-action:0.4">
            % if node.get_job_tracker():
            <job-tracker>${node.get_job_tracker()}</job-tracker>
            % endif
            % if node.get_name_node():
            <name-node>${node.get_name_node()}</name-node>
            % endif
            % if node.get_delete() or node.get_mkdir():
            <prepare>
                % if node.get_delete():
                    % for path in node.get_delete():
                    <delete path="${path}"/>
                    % endfor
                    % for path in node.get_mkdir():
                    <mkdir path="${path}"/>
                    % endfor
                % endif
            </prepare>
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
            <arg>${node.get_command()}</arg>
            % if node.get_arg():
            % for arg in node.get_arg():
            <arg>${arg}</arg>
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
        </sqoop>
        <ok to="${node.get_ok()}"/>
        <error to="${node.get_error()}"/>
    </action>
