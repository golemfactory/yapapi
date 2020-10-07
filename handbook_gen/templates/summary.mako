% for node in summary.children.values():
${render_node(node, 0)}\
% endfor
<%def name="render_node(node, level)">
<% indent = '' %>\
% for i in range(level):
<% indent += '  ' %>\
% endfor
% if node.filepath:
${indent}* [${node.name}](${node.filepath})\
% else:
${indent}* ${node.name}\
% endif
% for child in node.children.values():
${render_node(child, level + 1)}\
% endfor
</%def>
