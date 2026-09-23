################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
"""Resolve the Calcite return type of a Python UDF from its inline source."""

import ast


def eval_return_type(source):
    """Return the raw return-type annotation string of the top-level ``eval``
    function. Raises ``ValueError`` if it can't be determined.
    """
    for node in ast.parse(source).body:
        if isinstance(node, ast.FunctionDef) and node.name == 'eval':
            if node.returns is None:
                raise ValueError(
                    "Function 'eval' has no return type annotation."
                )
            annotation = _annotation_string(node.returns)
            if annotation is None:
                raise ValueError(
                    "Return type annotation of 'eval' could not be rendered "
                    "(needs Python 3.9+ for non-trivial annotations)."
                )
            return annotation
    raise ValueError(
        "Python UDF source does not define a top-level 'eval' function."
    )


def _annotation_string(annotation):
    if isinstance(annotation, ast.Name):
        return annotation.id
    unparse = getattr(ast, 'unparse', None)
    if unparse is None:
        return None
    try:
        return unparse(annotation)
    except (AttributeError, TypeError):
        return None
