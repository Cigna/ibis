<%namespace name="shell" file="shell.xml.mako" />
    <action name="${node.get_name()}">
        <shell xmlns="uri:oozie:shell-action:0.3">
            % if node.get_delete() or node.get_mkdir():
            <prepare>
                % for path in node.get_delete():
                <delete path="${path}"/>
                % endfor
                % for path in node.get_mkdir():
                <mkdir path="${path}"/>
                % endfor
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
            <exec>${node.get_execute()}</exec>
            % for arg in node.get_arg():
            <argument>${arg}</argument>
            % endfor
            % for env in node.get_env_var():
            <env-var>${env}</env-var>
            % endfor
            % for fle in node.get_file():
            <file>${fle}</file>
            % endfor
            % for archive in node.get_archive():
            <archive>${archive}</archive>
            % endfor
            % if node.get_capture_output():
            <capture-output/>
            % endif
        </shell>
        <ok to="${node.get_ok()}"/>
        <error to="${node.get_error()}"/>
    </action>