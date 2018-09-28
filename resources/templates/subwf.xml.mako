<%namespace name="fake_open_subwf" file="subwf.xml.mako" />
<action name="${node.name}">
    <sub-workflow>
        <app-path>${node.get_path()}</app-path>
        <propagate-configuration />
        <configuration>
            <property>
                <name>job_reg_key</name>
                <value>${"${wf:actionData('JobRegisterStart')['Job_Reg_Key']}" | n}</value>
            </property>
        </configuration>
    </sub-workflow>
    <ok to="${node.ok}"/>
    <error to="${node.error}"/>
</action>