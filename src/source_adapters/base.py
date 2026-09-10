"""
source_adapters.base - the common source adapter contract.

Shared orchestration (the notebooks) decides *what* operation to perform for
each control-table row; the adapter decides *how* to communicate with and query
that row's source. A row's source is chosen purely from ``source_system`` via the
factory, so the notebooks never branch on the source dialect beyond obtaining an
adapter.

The pure query-building / policy methods are unit-testable with no Spark. The
two connection methods (``get_jdbc_url_and_props`` / ``read_jdbc``) need a secret
provider and a Spark session, both injected at construction time so nothing here
imports ``dbutils`` or ``pyspark`` at module load.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class SourceAdapter(ABC):
    """Abstract source adapter.

    Concrete adapters set ``source_system`` and implement the query/connection
    methods for their dialect. ``secret_provider`` is a callable
    ``(scope, key) -> value | None`` used to read connection secrets; it is only
    required for the connection methods.
    """

    source_system: str = None

    def __init__(self, secret_provider=None, secret_scope=None,
                 source_server=None, source_database=None, config=None):
        self._secret_provider = secret_provider
        self.secret_scope = secret_scope
        self.source_server = source_server
        self.source_database = source_database
        self.config = dict(config or {})

    # ---------------------------------------------------------------- secrets
    def _get_secret(self, key, required=True):
        """Read a secret via the injected provider; None when missing."""
        if self._secret_provider is None:
            if required:
                raise RuntimeError(
                    f"No secret provider configured to read {key!r}")
            return None
        try:
            val = self._secret_provider(self.secret_scope, key)
        except Exception:
            val = None
        if not val and required:
            raise RuntimeError(
                f"Required secret {key!r} not found in scope {self.secret_scope!r}")
        return val or None

    # ------------------------------------------------------------ connection
    @abstractmethod
    def get_jdbc_url_and_props(self, source_server=None, source_database=None):
        """Return (jdbc_url, props_dict) for this source row."""

    def extra_read_options(self) -> dict:
        """Dialect-specific JDBC read options (overridden where needed)."""
        return {}

    @staticmethod
    def redact_jdbc_url(url) -> str:
        """Return a log-safe JDBC URL with any embedded credentials removed."""
        import re
        if not url:
            return ""
        s = str(url)
        # Strip user=/password= properties and userinfo in URL authority.
        s = re.sub(r"(?i)(password|pwd|user|username)=[^;&\s]*", r"\1=***", s)
        s = re.sub(r"//[^/@]*@", "//***@", s)
        return s

    def read_jdbc(self, spark, dbtable, source_server=None, source_database=None,
                  fetchsize=10000, partition_column=None, lower_bound=None,
                  upper_bound=None, num_partitions=None):
        """Read a (sub)query via JDBC into a Spark DataFrame for this source."""
        server = source_server if source_server is not None else self.source_server
        database = source_database if source_database is not None else self.source_database
        url, props = self.get_jdbc_url_and_props(server, database)
        reader = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", dbtable)
            .option("user", props["user"])
            .option("password", props["password"])
            .option("driver", props["driver"])
            .option("fetchsize", str(fetchsize))
        )
        for opt_key, opt_val in self.extra_read_options().items():
            reader = reader.option(opt_key, opt_val)
        if (partition_column and num_partitions and int(num_partitions) > 1
                and lower_bound is not None and upper_bound is not None):
            reader = (
                reader
                .option("partitionColumn", partition_column)
                .option("lowerBound", str(lower_bound))
                .option("upperBound", str(upper_bound))
                .option("numPartitions", str(int(num_partitions)))
            )
        return reader.load()

    # ------------------------------------------------------------ metadata SQL
    @abstractmethod
    def columns_metadata_query(self, source_database, source_schema, source_table):
        ...

    @abstractmethod
    def primary_key_query(self, source_database, source_schema, source_table):
        ...

    @abstractmethod
    def top_n_probe_query(self, source_database, source_schema, source_table, n):
        ...

    @abstractmethod
    def count_query(self, source_database, source_schema, source_table):
        ...

    @abstractmethod
    def min_max_query(self, source_database, source_schema, source_table, column):
        ...

    @abstractmethod
    def upper_watermark_query(self, source_database, source_schema, source_table,
                              watermark_column, watermark_type):
        ...

    @abstractmethod
    def full_extract_query(self, source_database, source_schema, source_table,
                           columns=None, watermark_column=None, watermark_type=None):
        ...

    @abstractmethod
    def incremental_extract_query(self, source_database, source_schema, source_table,
                                  watermark_column, watermark_type, lower_watermark,
                                  upper_watermark, columns=None):
        ...

    # ------------------------------------------------------- watermark policy
    @abstractmethod
    def normalize_watermark_type(self, source_type):
        ...

    @abstractmethod
    def is_supported_watermark_type(self, source_type):
        ...

    @abstractmethod
    def watermark_type_rank(self, source_type):
        ...

    @abstractmethod
    def initial_watermark_value(self, source_type):
        """Canonical checkpoint used when a successfully loaded source is empty."""
        ...

    def resolve_watermark_decision(self, columns, primary_key_columns,
                                   configured_watermark=None):
        """Resolve the strategy + watermark for this row using this dialect."""
        try:
            from src.strategy import resolve_watermark_decision
        except ModuleNotFoundError:
            from strategy import resolve_watermark_decision
        return resolve_watermark_decision(
            columns, primary_key_columns, configured_watermark, source=self)

    # ------------------------------------------------------- partition policy
    @abstractmethod
    def resolve_partition_plan(self, source_metadata, target_type, min_value,
                               max_value, requested_partitions):
        ...

    # ------------------------------------------------------------ type mapper
    @abstractmethod
    def load_type_mapper(self):
        ...
