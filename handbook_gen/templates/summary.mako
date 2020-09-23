% for node in summary.children.values():
${render_node(node, 0)}
% endfor
<%def name="render_node(node, level)">
<% indent = '' %>\
% for i in range(level):
<% indent += '  ' %>\
% endfor
${indent}* [${node.name}](${node.filepath})\
% for child in node.children.values():
${render_node(child, level + 1)}\
% endfor
</%def>