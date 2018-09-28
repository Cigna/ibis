<%namespace name="ssh" file="ssh.xml.mako" />
    <action name="${node.name}">
        <ssh xmlns="uri:oozie:ssh-action:0.2">
            % if node.get_user() and node.get_host():
            <host>${node.get_user()}@${node.get_host()}</host>
            % endif
            <command>${node.execute}</command>
            % if node.get_args():
            % for arg in node.get_args():
            <argument>${arg}</argument>
            % endfor
            % endif
            % if node.get_capture_output():
            <capture-output/>
            % endif
        </ssh>
        <ok to="${node.ok}"/>
        <error to="${node.error}"/>
    </action>