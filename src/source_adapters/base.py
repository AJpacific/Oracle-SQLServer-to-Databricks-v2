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
from dataclasses import dataclass


# Canonical, source-neutral policy codes. A source maps its own metadata to
# these so shared notebooks never interpret a dialect concept.
SOURCE_GENERATED_COLUMN = "SOURCE_GENERATED_COLUMN"
SOURCE_HIDDEN_COLUMN = "SOURCE_HIDDEN_COLUMN"
SOURCE_NON_WRITABLE_COLUMN = "SOURCE_NON_WRITABLE_COLUMN"
SOURCE_BINARY_VERSION_COLUMN = "SOURCE_BINARY_VERSION_COLUMN"

POLICY_CODES = (SOURCE_GENERATED_COLUMN, SOURCE_HIDDEN_COLUMN,
                SOURCE_NON_WRITABLE_COLUMN, SOURCE_BINARY_VERSION_COLUMN)


@dataclass(frozen=True)
class ColumnPolicyResult:
    """Normalized outcome of a source's column policy.

    Shared code consumes only these fields; it never inspects source_system.
    """

    include_column: bool
    mapping_status: str
    mapping_fidelity: str
    notes: str
    is_writable: bool
    requires_review: bool
    policy_code: str = None


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

    @abstractmethod
    def connection_probe_query(self):
        """Return the dialect's trivial connectivity probe sub-query.

        Shared code must never hard-code ``SELECT 1 FROM DUAL`` or ``SELECT 1``;
        it asks the adapter so a future source supplies its own probe.
        """

    def extra_read_options(self) -> dict:
        """Dialect-specific JDBC read options (overridden where needed)."""
        return {}

    @staticmethod
    def redact_jdbc_url(url) -> str:
        """Return a log-safe JDBC URL with any embedded credentials removed."""
        try:
            from src.failure_classifier import redact_url
        except ModuleNotFoundError:
            from failure_classifier import redact_url
        return redact_url(url)

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

    def batch_columns_metadata_query(self, source_database, tables: list):
        """Return a batch column metadata query for multiple candidate tables in a database."""
        raise NotImplementedError("Batch column metadata query is not implemented for this adapter")

    def batch_primary_key_query(self, source_database, tables: list):
        """Return a batch primary key metadata query for multiple candidate tables in a database."""
        raise NotImplementedError("Batch primary key query is not implemented for this adapter")

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

    # ------------------------------------------------------- discovery SQL
    # Broad source-assessment discovery. Concrete adapters return neutral
    # aliases (SCHEMA_NAME / OBJECT_NAME / OBJECT_TYPE / ROW_COUNT / SIZE_MB /
    # ROW_COUNT_METHOD) so the assessment notebook stays source-independent. A
    # broad assessment never runs COUNT(*) per table; it uses catalog/dictionary
    # metadata and labels the method CATALOG, ESTIMATED, or UNAVAILABLE.
    @abstractmethod
    def list_schemas_query(self, source_database=None):
        ...

    @abstractmethod
    def list_tables_query(self, source_database=None, source_schema=None):
        ...

    @abstractmethod
    def list_views_query(self, source_database=None, source_schema=None):
        ...

    @abstractmethod
    def list_routines_query(self, source_database=None, source_schema=None):
        ...

    @abstractmethod
    def table_statistics_query(self, source_database=None, source_schema=None):
        ...

    # --------------------------------------------------------- SQL objects
    # SQL-object support differs per source, so capability is explicit: a source
    # that cannot assess an object type says so instead of silently returning
    # empty data. Definition-text extraction stays on the concrete adapters
    # because the mechanism (ALL_SOURCE vs sys.sql_modules) is dialect-specific.
    SQL_OBJECT_TYPES = ()

    def supports_sql_object_type(self, object_type) -> bool:
        """True when this source can assess the given SQL object type."""
        return self.normalize_sql_object_type(object_type) in self.SQL_OBJECT_TYPES

    def normalize_sql_object_type(self, object_type) -> str:
        """Normalize a source object-type token to the shared vocabulary.

        Returns an empty string for a code this source does not recognize, so
        callers can skip it explicitly rather than mislabelling it.
        """
        token = (object_type or "").strip().upper().replace(" ", "_")
        return token if token in self.SQL_OBJECT_TYPES else ""

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

    @abstractmethod
    def type_rules_file(self) -> str:
        """Filename of this source's type-rules YAML (not a full path).

        Shared mapping code asks the adapter instead of branching on the source
        system, so a future source supplies its own rules file unchanged.
        """

    def legacy_secret_scope_widget(self):
        """Name of this source's legacy global secret-scope widget, or None.

        COMPATIBILITY ONLY. Production routing uses the registered
        ``source_connection.secret_scope``; shared code never infers a scope.
        """
        return None

    def resolve_effective_database(self, configured_database: str, requested_database: str = None) -> str:
        """Resolve effective database for source operation."""
        cfg = str(configured_database or "").strip()
        req = str(requested_database or "").strip() if requested_database is not None else ""
        if req and cfg and req.casefold() != cfg.casefold():
            raise ValueError(
                f"source_database override {req!r} does not match registered database {cfg!r}"
            )
        return req or cfg

    def resolve_operational_database(self, configured_database: str, operational_database: str) -> str:
        """Resolve and validate operational row database against configured database."""
        cfg = str(configured_database or "").strip()
        op = str(operational_database or "").strip()
        if not cfg:
            raise ValueError(f"{self.source_system or 'Source'} requires configured connection database")
        if not op:
            raise ValueError(f"{self.source_system or 'Source'} operational row requires nonblank database")
        if cfg.casefold() != op.casefold():
            raise ValueError(
                f"operational database {op!r} does not match configured connection database {cfg!r}"
            )
        return cfg

    def validate_connection_metadata(self, connection) -> None:
        """Raise ValueError when non-secret connection metadata is unusable.

        Never inspects or reports a credential.
        """
        c = dict(connection or {})
        if not (c.get("secret_scope") or "").strip():
            raise ValueError(
                f"connection {c.get('connection_id')!r} has no secret_scope; "
                f"register a secret scope for this {self.source_system} connection")

    # ---------------------------------------------------------- column policy
    def apply_column_policy(self, column_metadata, proposed_mapping):
        """Normalize a proposed type mapping through this source's column policy.

        The base behavior preserves the proposed mapping, includes the column,
        and treats it as writable. A concrete adapter overrides this to express
        its own dialect rules, returning the same normalized result so shared
        notebooks stay source-neutral.
        """
        return ColumnPolicyResult(
            include_column=True,
            mapping_status=proposed_mapping.status,
            mapping_fidelity=proposed_mapping.fidelity,
            notes=proposed_mapping.notes or "",
            is_writable=True,
            requires_review=(proposed_mapping.status or "").upper() == "REVIEW",
            policy_code=None,
        )

    @staticmethod
    def _flag(value) -> bool:
        """Interpret a source metadata flag (int, bool, or string) as boolean."""
        if value is None:
            return False
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "y")
        return bool(value)
