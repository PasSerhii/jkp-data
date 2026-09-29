"""Centralized path management for the JKP data pipeline."""

from dataclasses import dataclass
from importlib.resources import as_file, files
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """Holds the user-specified output directory and exposes subdirectory paths.

    The directory layout under base_dir mirrors the pipeline's expected structure:
        base_dir/
            raw/raw_tables/     # downloaded WRDS data
            interim/            # intermediate pipeline files
            interim/raw_data_dfs/
            processed/          # final outputs
    """

    base_dir: Path

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "raw"

    @property
    def raw_tables_dir(self) -> Path:
        return self.base_dir / "raw" / "raw_tables"

    @property
    def interim_dir(self) -> Path:
        return self.base_dir / "interim"

    @property
    def processed_dir(self) -> Path:
        return self.base_dir / "processed"

    @property
    def production_dir(self) -> Path:
        """The single production CSV directory.

        Holds ``monthly/<country>.csv``, ``daily/<country>.csv`` and the six
        cross-country files (market returns, cutoffs, world monthly returns).
        This replaces the former ``processed/output/`` tree, which held a
        byte-identical second copy of every country CSV purely to present the
        SAS directory shape.
        """
        return self.processed_dir / "production"

    def raw_table_source(self, table_name: str) -> Path | str:
        """Return a file or Parquet glob for a downloaded source table.

        Most source tables are stored as one ``<schema>_<table>.parquet``
        file.  The very large daily Compustat tables are downloaded in
        restart-sized parts and exposed to DuckDB/Polars as one glob.
        """
        stem = table_name.replace(".", "_")
        parts_dir = self.raw_tables_dir / f"{stem}_parts"
        if parts_dir.is_dir():
            return str(parts_dir / "part-*.parquet")
        return self.raw_tables_dir / f"{stem}.parquet"


def _resource_path(filename: str) -> Path:
    """Return a filesystem Path to a bundled resource file.

    Uses importlib.resources.as_file() to ensure the result is a real
    filesystem path, even if the package is installed in a zip archive.
    """
    ref = files("jkp.data").joinpath("resources", filename)
    # as_file() returns a context manager, but for read-only resources that
    # exist on the filesystem (the common case), we can extract the path
    # directly. The context manager's cleanup is a no-op for real files.
    ctx = as_file(ref)
    return ctx.__enter__()


def get_siccodes_path() -> Path:
    """Return the path to the bundled Siccodes49.txt file."""
    return _resource_path("Siccodes49.txt")


def get_cluster_labels_path() -> Path:
    """Return the path to the bundled cluster_labels.csv file."""
    return _resource_path("cluster_labels.csv")


def get_country_classification_path() -> Path:
    """Return the path to the bundled country_classification.xlsx file."""
    return _resource_path("country_classification.xlsx")


def get_factor_details_path() -> Path:
    """Return the path to the bundled factor_details.xlsx file."""
    return _resource_path("factor_details.xlsx")


def get_data_readme_path() -> Path:
    """Return the path to the bundled data directory README file."""
    return _resource_path("README.md")


def get_production_monthly_columns() -> list[str]:
    """Return the ordered column list for the monthly production CSV output.

    Mirrors the column order of the SAS `save_main_production_data_csv` output
    (e.g. usa.csv), one column name per line.
    """
    path = _resource_path("production_monthly_columns.txt")
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]
