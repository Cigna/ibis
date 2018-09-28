<%namespace name="fork" file="fork.xml.mako" />
    <fork name="${node.name}">
    % for name in node.get_to_nodes():
        <path start="${name}"/>
    % endfor
    </fork>