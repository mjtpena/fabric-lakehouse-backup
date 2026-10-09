"""
Helpers for testing the Fabric notebooks without a Fabric workspace.

Fabric notebooks can't import modules from this repository, so code that is worth
unit testing lives in each notebook in a cell marked "# Pure helpers: ...". Those
cells only define constants and functions (standard library only), so the tests
can execute them on their own.
"""

import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

NOTEBOOKS = {
    "backup": REPO_ROOT / "nb_lakehouse_backup.Notebook" / "notebook-content.py",
    "restore": REPO_ROOT / "nb_lakehouse_restore.Notebook" / "notebook-content.py",
    "cleanup": REPO_ROOT / "nb_lakehouse_cleanup.Notebook" / "notebook-content.py",
}

HEADER = "# Fabric notebook source"
MARKERS = {
    "# PARAMETERS CELL ********************": "parameters",
    "# CELL ********************": "code",
    "# METADATA ********************": "metadata",
    "# MARKDOWN ********************": "markdown",
}
PURE_MARKER = "# Pure helpers: no Spark or notebookutils calls."


def read_notebook(name_or_path):
    """Notebook source with line endings normalised to \\n"""
    path = NOTEBOOKS.get(name_or_path, name_or_path)
    return pathlib.Path(path).read_text(encoding="utf-8").replace("\r\n", "\n")


def split_cells(source):
    """
    Split notebook-content.py into blocks: [(kind, text)], where kind is
    "header", "parameters", "code", "metadata" or "markdown".
    """
    blocks = [("header", [])]
    for line in source.split("\n"):
        kind = MARKERS.get(line.strip())
        if kind:
            blocks.append((kind, []))
        else:
            blocks[-1][1].append(line)
    return [(kind, "\n".join(lines).strip("\n")) for kind, lines in blocks]


def code_cells(source):
    """Parameters and code cells, in order"""
    return [(kind, text) for kind, text in split_cells(source) if kind in ("parameters", "code")]


def metadata_json(text):
    """Parse a METADATA block ("# META ..." lines) as JSON"""
    import json

    lines = [line[len("# META"):].lstrip(" ") if line.startswith("# META") else line for line in text.split("\n")]
    return json.loads("\n".join(lines))


def load_pure_helpers(name):
    """Execute the notebook's pure-helper cells and return their namespace"""
    cells = [text for kind, text in code_cells(read_notebook(name)) if PURE_MARKER in text]
    if not cells:
        raise AssertionError(f"{name} notebook has no '{PURE_MARKER}' cell")
    namespace = {"__name__": f"notebook_{name}_pure_helpers", "print": lambda *args, **kwargs: None}
    for text in cells:
        exec(compile(text, f"<{name} pure helpers>", "exec"), namespace)
    return namespace


class FakeEntry:
    """Stand-in for a notebookutils.fs.ls FileInfo"""

    def __init__(self, name, path, is_dir, size=0):
        self.name = name
        self.path = path
        self.isDir = is_dir
        self.isFile = not is_dir
        self.size = size


class FakeFs:
    """
    In-memory folder tree for discovery tests. Build it from relative paths; a trailing
    slash marks a folder, anything else is a file. Paths are rooted at `root`.

        fs = FakeFs("abfss://ws@onelake/lh.Lakehouse", ["Tables/dbo/orders/_delta_log/", "Tables/dbo/orders/part-0.parquet"])
    """

    def __init__(self, root, paths):
        self.root = root.rstrip("/")
        self.dirs = {self.root}
        self.files = set()
        for relative in paths:
            parts = relative.strip("/").split("/")
            for i in range(1, len(parts)):
                self.dirs.add(self._join(parts[:i]))
            full = self._join(parts)
            (self.dirs if relative.endswith("/") else self.files).add(full)

    def _join(self, parts):
        return "/".join([self.root] + list(parts))

    def ls(self, path):
        path = path.rstrip("/")
        if path not in self.dirs:
            raise FileNotFoundError(path)
        children = []
        for candidate in sorted(self.dirs | self.files):
            parent, _, name = candidate.rpartition("/")
            if parent == path and candidate != path:
                children.append(FakeEntry(name, candidate, candidate in self.dirs))
        return children

    def exists(self, path):
        path = path.rstrip("/")
        return path in self.dirs or path in self.files
