#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Convert an exported Hugo docs tree to Docusaurus sources."""

import argparse
import html
import json
import posixpath
import re
from pathlib import Path


def frontmatter(text):
    match = re.match(r"---\s*\n(.*?)\n---\n", text, re.S)
    fields = {}
    aliases = []
    for line in match[1].splitlines():
        scalar = re.match(r"^(\w+):\s*(.*?)\s*$", line)
        if scalar:
            fields[scalar[1]] = scalar[2].strip('"\'')
        alias = re.match(r"\s*-\s*(/.*?)\s*$", line)
        if alias:
            aliases.append(alias[1])
    return fields, aliases, text[match.end():]


def target_path(relative):
    return relative.replace("_index.md", "index.md").replace("versions.md", "versions.mdx")


def route(relative):
    return "/" + re.sub(r"(?:_index)?\.md$", "", relative).rstrip("/")


def strip_heading_ids(body):
    lines = body.splitlines(keepends=True)
    fence = None
    for index, line in enumerate(lines):
        marker = re.match(r"^\s*(`{3,})([^`]*)$|^\s*(~{3,})([^~]*)$", line.rstrip())
        if marker:
            character = (marker[1] or marker[3])[0]
            fence = None if fence == character else character
        atx = re.match(r"^#{1,6}\s+", line)
        setext = index + 1 < len(lines) and re.fullmatch(r"(?:={3,}|-{3,})\s*", lines[index + 1])
        if fence is None and (atx or setext):
            lines[index] = re.sub(r"[ \t]+\{#[^}\n]+\}[ \t]*(?=\n|$)", "", line)
    return "".join(lines)


def convert_body(body, relative, locale):
    body = re.sub(r'<h([1-6]) id="[^"]+">(.*?)</h\1>', lambda match: "#" * int(match[1]) + " " + match[2], body)
    body = body.replace('<a name="Full Changelog" id="003" ></a>', '')
    body = body.replace('<a href="{% link dev/table/types.md %}"></a>', '')
    body = body.replace('mysql-cdc.md/#', 'mysql-cdc.md#')
    body = body.replace('href="./mysql-cdc.md#', 'href="../mysql-cdc/#')
    body = body.replace('href="#a-name-id-003-a"', 'href="#full-changelog"')
    def reference(match):
        path, _, anchor = match[1].partition("#")
        path = path.lstrip("/")
        if not path.endswith(".md"):
            path += ".md"
        path = target_path(path)
        link = posixpath.relpath(path, posixpath.dirname(target_path(relative)) or ".")
        return link + ("#" + anchor if anchor else "")

    # HTML links need deployed URLs rather than Markdown source-file paths.
    body = re.sub(
        r'href="\{\{<\s*ref\s+"([^"]+)"\s*>\}\}"',
        lambda m: 'href="@@BASE_URL@@@@LOCALE_PREFIX@@' + m[1].lstrip("/") + '/"',
        body,
    )
    body = re.sub(r'\{\{<\s*ref\s+"([^"]+)"\s*>\}\}', reference, body)

    def image(match):
        attributes = dict(re.findall(r'(\w+)="([^"]*)"', match[1]))
        width = attributes.get("width", "")
        return (
            '<img src="@@BASE_URL@@' + attributes["src"].lstrip("/")
            + '" alt="' + html.escape(attributes.get("alt", ""), quote=True)
            + '"' + (f' width="{width}"' if width else "") + ' class="doc-image">'
        )

    body = re.sub(r"\{\{<\s*img\s+(.*?)\s*>\}\}", image, body)
    body = re.sub(r"\{\{<\s*param\s+Version\s*>\}\}", "@@VERSION@@", body)

    def artifact(match):
        return (
            "```xml\n<dependency>\n"
            "    <groupId>org.apache.flink</groupId>\n"
            f"    <artifactId>{match[1]}</artifactId>\n"
            "    <version>@@VERSION@@</version>\n"
            "</dependency>\n```"
        )

    body = re.sub(r"\{\{<\s*artifact\s+([\w-]+)\s*>\}\}", artifact, body)
    body = re.sub(r"\{\{<\s*hint\s+(\w+)\s*>\}\}", r":::\1\n", body)
    body = re.sub(r"\{\{<\s*/hint\s*>\}\}", "\n:::", body)
    body = re.sub(r"\{\{<\s*top\s*>\}\}", '', body)
    body = re.sub(r"\{\{<\s*all_versions\s*>\}\}", "<AllVersions />", body)
    if relative == "docs/core-concept/data-pipeline.md":
        body = body.replace("\n# Pipeline Configurations\n", "\n## Pipeline Configurations\n")
        body = body.replace("\n# Pipeline 配置\n", "\n## Pipeline 配置\n")
    if relative == "docs/core-concept/route.md":
        body = body.replace("\n# Route Mode\n", "\n## Route Mode\n")
        body = body.replace("\n# 路由模式\n", "\n## 路由模式\n")
    if locale == "zh":
        body = body.replace('href="#code-contribution-guide"', 'href="#贡献代码指导"')
        body = body.replace('href="#code-review-guide"', 'href="#代码评审指导"')
        body = body.replace('href="#full-changelog"', 'href="#完整的-changelog"')
        for mode in ["exception", "evolve", "tryevolve", "lenient", "ignore"]:
            body = body.replace(f"schema-evolution.md#{mode}-mode", f"schema-evolution.md#{mode}-模式")
        body = body.replace("flink-sources/overview.md#supported-connectors", "flink-sources/overview.md#支持的连接器")
    if "{{<" in body or "{{%" in body:
        raise ValueError(f"Unconverted shortcode in {relative}")
    return body.rstrip() + "\n"


def migrate(source, destination):
    header = (Path(__file__).parents[1] / "site-metadata.js").read_text().split("*/", 1)[0] + "*/\n\n"
    section_fields = {}
    translations = {}
    redirects = [
        {"from": "/docs", "to": "/docs/get-started/introduction"},
        {"from": "/en", "to": "/"},
    ]
    for locale, directory in [("en", "content"), ("zh", "content.zh")]:
        output = destination / ("content" if locale == "en" else "i18n/zh/docusaurus-plugin-content-docs/current")
        for file in sorted((source / directory).rglob("*.md")):
            relative = file.relative_to(source / directory).as_posix()
            fields, aliases, body = frontmatter(file.read_text())
            body = strip_heading_ids(body)
            body = convert_body(body, relative, locale)
            lines = ["---", "title: " + json.dumps(fields["title"], ensure_ascii=False), "slug: " + json.dumps(route(relative))]
            if "weight" in fields:
                lines.append("sidebar_position: " + fields["weight"])
            if fields.get("bookToc") == "false":
                lines.append("hide_table_of_contents: true")
            lines.extend(["hide_title: true", "displayed_sidebar: docs", "---"])
            if relative == "versions.md":
                body = "import AllVersions from '@site/src/components/AllVersions';\n\n" + body
            if relative == "_index.md":
                lines.insert(-1, "pagination_prev: null")
                lines.insert(-1, "pagination_next: null")
            path = output / target_path(relative)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(lines) + "\n" + body)
            if relative.startswith("docs/"):
                if locale == "en":
                    section_fields[relative] = fields
                elif relative.endswith("_index.md"):
                    english = section_fields[relative]["title"]
                    translations[f"sidebar.docs.category.{english}"] = {"message": fields["title"]}
            redirects.extend({"from": alias.rstrip("/"), "to": route(relative)} for alias in aliases)

    def category(directory):
        children = []
        for relative, fields in section_fields.items():
            if posixpath.dirname(relative) == directory and not relative.endswith("_index.md"):
                children.append((int(fields.get("weight", 999)), relative, relative[:-3]))
            elif relative.endswith("/_index.md") and posixpath.dirname(posixpath.dirname(relative)) == directory:
                subdir = posixpath.dirname(relative)
                children.append((int(fields.get("weight", 999)), relative, {
                    "type": "category", "label": fields["title"], "collapsed": True,
                    "items": category(subdir),
                }))
        return [item[2] for item in sorted(children, key=lambda item: (item[0], item[1]))]

    (destination / "sidebars.js").write_text(header + "module.exports = " + json.dumps({"docs": category("docs")}, indent=2) + ";\n")
    unique_redirects = {}
    for redirect in redirects:
        # An old PolarDB-X page mistakenly reused the MongoDB alias.
        unique_redirects.setdefault(redirect["from"], redirect)
    (destination / "redirects.js").write_text(header + "module.exports = " + json.dumps(list(unique_redirects.values()), indent=2) + ";\n")
    (destination / "i18n/zh/docusaurus-plugin-content-docs/current.json").write_text(json.dumps(translations, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Exported Hugo docs directory")
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    migrate(args.source, args.destination)
