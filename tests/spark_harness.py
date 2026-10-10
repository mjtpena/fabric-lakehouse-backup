"""
Run the Fabric notebooks end to end on local Apache Spark + Delta Lake.

This is not Fabric: there is no OneLake, no lakehouse catalog and no real notebookutils.
It does run every notebook cell on the same Spark/Delta major versions as the Fabric
runtimes, against lakehouse-shaped folders on local disk:

  - notebookutils is replaced by LocalFs (ls/exists/cp/rm/mkdirs/head on local paths)
  - onelake_lakehouse_path() is replaced so abfss:// paths become local folders
  - cells run in order in one namespace; parameter overrides are applied right after the
    parameters cell, the way Fabric injects them for pipeline runs

Requires: pyspark, delta-spark and a Java runtime supported by that Spark version.
"""

import contextlib
import io
import os
import shutil
import sys
import types

from notebook_loader import code_cells, read_notebook


class LocalFileInfo:
    """Mimics the FileInfo objects returned by notebookutils.fs.ls"""

    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path.rstrip("/"))
        self.isDir = os.path.isdir(path)
        self.isFile = not self.isDir
        self.size = 0 if self.isDir else os.path.getsize(path)


class LocalFs:
    """The subset of notebookutils.fs the notebooks use, on local paths"""

    @staticmethod
    def _local(path):
        return path[len("file:"):] if path.startswith("file:") else path

    def ls(self, path):
        path = self._local(path).rstrip("/")
        if not os.path.isdir(path):
            raise FileNotFoundError(path)
        return [LocalFileInfo(os.path.join(path, name)) for name in sorted(os.listdir(path))]

    def exists(self, path):
        return os.path.exists(self._local(path))

    def cp(self, src, dest, recurse=False):
        src, dest = self._local(src), self._local(dest)
        if os.path.isdir(src):
            if not recurse:
                raise IsADirectoryError(src)
            shutil.copytree(src, dest, dirs_exist_ok=True)
        else:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copyfile(src, dest)
        return True

    def rm(self, path, recurse=False):
        path = self._local(path)
        if os.path.isdir(path):
            if not recurse:
                raise IsADirectoryError(path)
            shutil.rmtree(path)
        else:
            os.remove(path)
        return True

    def mkdirs(self, path):
        os.makedirs(self._local(path), exist_ok=True)
        return True

    def head(self, path, max_bytes=65536):
        with open(self._local(path), "rb") as f:
            return f.read(max_bytes).decode("utf-8", errors="replace")


def install_fake_notebookutils():
    module = types.ModuleType("notebookutils")
    module.fs = LocalFs()
    sys.modules["notebookutils"] = module
    return module


def local_onelake(onelake_root):
    """Replacement for onelake_lakehouse_path(): <onelake_root>/<workspace>/<lakehouse>.Lakehouse"""
    def onelake_lakehouse_path(workspace_name, lakehouse_name):
        return f"{onelake_root}/{workspace_name.strip()}/{lakehouse_name.strip()}.Lakehouse"
    return onelake_lakehouse_path


class NotebookRun:
    def __init__(self, namespace, output, error):
        self.namespace = namespace
        self.output = output
        self.error = error

    @property
    def ok(self):
        return self.error is None


def run_notebook(name_or_path, spark, parameters, onelake_root, overrides=None, echo=False):
    """Run every cell of a notebook; returns NotebookRun(namespace, captured output, exception or None)"""
    replacements = {"onelake_lakehouse_path": local_onelake(onelake_root)}
    replacements.update(overrides or {})
    namespace = {"__name__": "__main__", "spark": spark}
    output = io.StringIO()
    error = None
    stream = output if not echo else _Tee(output, sys.stdout)
    with contextlib.redirect_stdout(stream):
        try:
            for index, (kind, text) in enumerate(code_cells(read_notebook(name_or_path))):
                exec(compile(text, f"<{os.path.basename(str(name_or_path))} cell {index}>", "exec"), namespace)
                if kind == "parameters":
                    namespace.update(parameters)
                for key, value in replacements.items():
                    if key in namespace:
                        namespace[key] = value
        except Exception as e:  # The notebooks re-raise on failure, like a failed Fabric job
            error = e
    return NotebookRun(namespace, output.getvalue(), error)


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()


def create_spark(work_dir):
    """Local Spark session with Delta Lake, configured like a Fabric session where it matters"""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # Python workers must match the driver's Python version
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("fabric-lakehouse-backup-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", os.path.join(work_dir, "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={os.path.join(work_dir, 'derby')}")
        # Test data includes a pre-1582 date (the notebooks set the same options)
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
