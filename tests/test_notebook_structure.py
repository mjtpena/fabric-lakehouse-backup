"""
Structure checks for the Fabric notebook files. Fabric's import and Git integration rely
on the cell markers, and the pipelines pass parameters by name to the parameters cell.
"""

import ast
import re
import unittest

from notebook_loader import HEADER, NOTEBOOKS, PURE_MARKER, REPO_ROOT, code_cells, metadata_json, read_notebook, split_cells

PIPELINES = {
    "backup": ["pipeline.notebook.lakehouse.backup.yml", "pipeline.notebook.lakehouse.backup.scheduled.yml"],
    "restore": ["pipeline.notebook.lakehouse.restore.yml"],
    "cleanup": ["pipeline.notebook.lakehouse.cleanup.yml"],
}

# Notebook parameters the pipelines pass, as "name" = @{ "value" = ... } or $parametersDict["name"] = @{ ... }
PIPELINE_PARAMETER = re.compile(r'(?:"(\w+)"\s*=\s*@\{\s*"value"|\$parametersDict\["(\w+)"\]\s*=)')


def assigned_names(cell_text):
    """Top-level names assigned in a cell"""
    names = set()
    for node in ast.parse(cell_text).body:
        if isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
    return names


class NotebookStructureTests(unittest.TestCase):
    def test_header_and_cell_markers(self):
        for name in NOTEBOOKS:
            with self.subTest(notebook=name):
                source = read_notebook(name)
                self.assertTrue(source.startswith(HEADER + "\n"), "first line must be the Fabric header")
                blocks = split_cells(source)
                kinds = [kind for kind, _ in blocks]
                self.assertEqual(kinds[1], "metadata", "notebook-level METADATA must follow the header")
                self.assertIn("kernel_info", metadata_json(blocks[1][1]))
                self.assertEqual(kinds.count("parameters"), 1, "exactly one PARAMETERS CELL")

    def test_parameters_cell_is_first_code_cell_and_tagged(self):
        for name in NOTEBOOKS:
            with self.subTest(notebook=name):
                blocks = split_cells(read_notebook(name))
                first_code = next(i for i, (kind, _) in enumerate(blocks) if kind in ("parameters", "code"))
                self.assertEqual(blocks[first_code][0], "parameters")
                kind, text = blocks[first_code + 1]
                self.assertEqual(kind, "metadata", "parameters cell must be followed by its METADATA block")
                self.assertIn("parameters", metadata_json(text).get("tags", []))

    def test_every_metadata_block_is_json(self):
        for name in NOTEBOOKS:
            for kind, text in split_cells(read_notebook(name)):
                if kind == "metadata":
                    with self.subTest(notebook=name):
                        metadata_json(text)

    def test_every_code_cell_compiles(self):
        for name in NOTEBOOKS:
            for index, (kind, text) in enumerate(code_cells(read_notebook(name))):
                with self.subTest(notebook=name, cell=index):
                    compile(text, f"{name} cell {index}", "exec")

    def test_each_notebook_has_a_pure_helper_cell(self):
        for name in NOTEBOOKS:
            with self.subTest(notebook=name):
                self.assertTrue(any(PURE_MARKER in text for _, text in code_cells(read_notebook(name))))

    def test_pipeline_parameters_exist_in_notebook_parameters_cell(self):
        for name, pipelines in PIPELINES.items():
            parameters = assigned_names(code_cells(read_notebook(name))[0][1])
            for pipeline in pipelines:
                text = (REPO_ROOT / pipeline).read_text(encoding="utf-8")
                passed = {a or b for a, b in PIPELINE_PARAMETER.findall(text)}
                with self.subTest(pipeline=pipeline):
                    self.assertTrue(passed, "no notebook parameters found in pipeline")
                    self.assertLessEqual(passed, parameters, f"pipeline passes parameters the {name} notebook doesn't define")

    def test_restore_parameters_include_target_schema(self):
        parameters = assigned_names(code_cells(read_notebook("restore"))[0][1])
        self.assertIn("target_schema", parameters)


if __name__ == "__main__":
    unittest.main()
