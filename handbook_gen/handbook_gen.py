"""Prepare documentation for Yagna Handbook

uses `Mako` and `Markdown` which are already included as portray/pydocs dependencies

The documentation is prepared from:
    * the projects's own `docs`
    * the automatically generated source reference (output of `pydocs`)
    * `mkdocs` configuration included in `pyproject.toml` (consistency with `portray`)

"""

import argparse
import markdown
import os
import toml

from collections import OrderedDict
from pathlib import Path
from mako import template as mako_template

def get_md_files(md_root, package_name):
    root = md_root / package_name
    return sorted([
        (dirname, sorted([f for f in files if f.endswith('.md')]), )
        for dirname, _, files in os.walk(root)
    ])


def get_module_title(file_path):
    with open(file_path, "r", encoding="utf-8") as md_file:
        text = md_file.read()

    md = markdown.Markdown()

    lines = text.split("\n")
    etree = md.parser.parseDocument(lines)
    root = etree.getroot()

    h1 = root.find(f"./h1")
    return h1.text.lstrip("Module ") if h1 is not None else ''


def get_pyproject():
    pyproject_path = Path(__file__).parents[1] / "pyproject.toml"
    with open(pyproject_path) as f:
        pyproject = toml.loads(f.read())

    return pyproject


def get_portray_config():
    return get_pyproject()["tool"]["portray"]


class SummaryNode:
    def __init__(self, name: str = None, filepath: str = None):
        self.name = name
        self.filepath = filepath
        self.children = OrderedDict()

    def __str__(self):
        children = ", ".join([str(v) for k, v in self.children.items()])
        return f"<name={self.name}, filepath={self.filepath}, " \
               f"children=[{children}]>"


def build_summary_tree(md_root, package_name):
    summary = SummaryNode()

    def process_file(dirname, filename):
        file_path = Path(dirname) / filename
        title = get_module_title(file_path)
        module_path = title.split('.')
        # print(Path(dirname).relative_to(md_root), title, filename, module_path[0])

        summary_node = summary
        for segment in module_path:
            summary_node = summary_node.children.setdefault(segment, SummaryNode())

        summary_node.name = title
        summary_node.filepath = str(Path(dirname).relative_to(md_root) / filename)

    for dirname, files in get_md_files(md_root, package_name):
        for filename in files:
            process_file(dirname, filename)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare Markdown docs")
    parser.add_argument("--md-dir")
    parser.add_argument("--template")
    parser.add_argument("--docs-dir")

    args = parser.parse_args()

    md_root = Path(args.md_dir) if args.md_dir else Path(__file__).parents[1] / "md"
    summary_template = Path(args.template) \
        if args.template else Path(__file__).parents[0] / "templates/summary.mako"

    summary = build_summary_tree(md_root, get_pyproject()['tool']['poetry']['name'])
    print(
        mako_template.Template(filename=str(summary_template)).render(summary=summary)
    )
