<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<configuration>
    % for prop, val in wf_props.iteritems():
    <property>
        <name>${prop}</name>
        <value>${val}</value>
    </property>
    % endfor
</configuration>
