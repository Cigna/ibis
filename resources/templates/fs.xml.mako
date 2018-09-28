<%namespace name="fs" file="fs.xml.mako" />
    <action name="${node.name}">
        <fs>
            % if node.get_delete():
            % for delete in node.get_delete():
            <delete path="${delete}"/>
            % endfor
            % endif
            % if node.get_mkdir():
            % for mkdir in node.get_mkdir():
            <mkdir path="${mkdir}"/>
             % endfor
            % endif
            % if node.get_move():
            % for move in node.get_move():
            <move source="${move["source"]}" target="${move["target"]}"/>
            % endfor
            % endif
            % if node.get_chmod():
            % for chmod in node.get_chmod():
            <chmod path="${chmod["path"]}" permissions="${chmod["permissions"]}" dir-files="${chmod["dir-files"]}"/>
            % endfor
            % endif
        </fs>
        <ok to="${node.ok}"/>
        <error to="kill"/>
    </action>