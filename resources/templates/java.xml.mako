<%namespace name="java" file="java.xml.mako" />
    <action name="${node.name}">
        <java>
            % if node.get_delete() or node.get_mkdir():
            <prepare>
                % for delete in node.get_delete():
                <delete path="${delete}"/>
                % endfor
                % for mkdir in node.get_mkdir():
                <mkdir path="${mkdir}"/>
                % endfor
            </prepare>
            % endif
            % if node.get_job_xml():
            <job-xml>${node.get_job_xml()}</job-xml>
            % endif
            % if node.get_configuration():
            <configuration>
                % for config in node.get_configuration():
                <property>
                    <name>${config['name']}</name>
                    <value>${config['value']}</value>
                </property>
                % endfor
            </configuration>
            % endif
            <main-class>${node.main_class}</main-class>
            % if node.get_java_opts():
            <java-opts>${node.get_java_opts()}</java-opts>
            % endif
            % for arg in node.get_args():
            <arg>${arg}</arg>
            % endfor
            % for file in node.get_files():
            <file>${file}</file>
            % endfor
            % for archive in node.get_archives():
            <archive>${archive}</archive>
            % endfor
            % if node.get_capture_output():
            <capture-output/>
            % endif
        </java>
        <ok to="${node.ok}"/>
        <error to="kill"/>
    </action>