"""
source_adapters.sqlserver - SQL Server source adapter.

Implements the shared :class:`SourceAdapter` contract for SQL Server: Microsoft
JDBC driver, bracket-quoted identifiers, catalog-view metadata, ``TOP (n)``
syntax, SQL Server temporal watermark policy, native-integral partition planning
and the SQL Server type mapper. The Databricks *target* side stays shared.

Connection is secret-backed. Two shapes are supported:
  1. a complete JDBC URL template secret (``sqlserver-jdbc-url``) into which the
     control-row database is filled safely, or
  2. discrete host/port secrets plus the control-row database.

Credentials never appear in logs, queue rows, or generated queries.
"""

from __future__ import annotations

import os

try:
    from src.source_adapters.base import (
        SourceAdapter, ColumnPolicyResult, SOURCE_HIDDEN_COLUMN,
        SOURCE_GENERATED_COLUMN, SOURCE_BINARY_VERSION_COLUMN,
    )
    from src import sqlserver_sql_builder as ssb
    from src import partitioning as part
    from src.identifiers import validate_server, validate_database
    from src.type_mappers.sqlserver import SqlServerTypeMapper
except ModuleNotFoundError:
    from source_adapters.base import (
        SourceAdapter, ColumnPolicyResult, SOURCE_HIDDEN_COLUMN,
        SOURCE_GENERATED_COLUMN, SOURCE_BINARY_VERSION_COLUMN,
    )
    import sqlserver_sql_builder as ssb
    import partitioning as part
    from identifiers import validate_server, validate_database
    from type_mappers.sqlserver import SqlServerTypeMapper


class SqlServerSourceAdapter(SourceAdapter):
    source_system = "sqlserver"
    DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    DEFAULT_PORT = "1433"
    # SQL Server has no package objects.
    SQL_OBJECT_TYPES = ("VIEW", "PROCEDURE", "FUNCTION")

    # sys.objects type codes this adapter recognizes. An unlisted code (trigger,
    # rule, CLR module, ...) normalizes to '' so it is skipped explicitly rather
    # than mislabelled as a PROCEDURE.
    _MODULE_TYPE_CODES = {
        "V": "VIEW", "P": "PROCEDURE", "PC": "PROCEDURE",
        "FN": "FUNCTION", "IF": "FUNCTION", "TF": "FUNCTION",
        "FS": "FUNCTION", "FT": "FUNCTION",
    }

    def normalize_sql_object_type(self, object_type) -> str:
        token = (object_type or "").strip().upper()
        mapped = self._MODULE_TYPE_CODES.get(token)
        if mapped:
            return mapped
        normalized = token.replace(" ", "_")
        return normalized if normalized in self.SQL_OBJECT_TYPES else ""

    # ------------------------------------------------------------ connection
    def connection_probe_query(self):
        return "(SELECT 1 AS CONNECTION_OK) q"

    def clone(self, source_database=None, source_server=None):
        """Clone this adapter with optional source_database and source_server overrides."""
        return SqlServerSourceAdapter(
            secret_provider=self._secret_provider,
            secret_scope=self.secret_scope,
            source_server=source_server if source_server is not None else self.source_server,
            source_database=source_database if source_database is not None else self.source_database,
            config=dict(self.config),
        )

    def with_database(self, source_database: str):
        """Return a cloned adapter configured for an effective database override."""
        return self.clone(source_database=source_database)

    def resolve_effective_database(self, configured_database: str, requested_database: str = None) -> str:
        """Resolve effective database: task override takes precedence, then configured database, or blank."""
        cfg = str(configured_database or "").strip()
        req = str(requested_database or "").strip() if requested_database is not None else ""
        return req or cfg

    def resolve_operational_database(self, configured_database: str, operational_database: str) -> str:
        """Resolve operational database for SQL Server: permits blank configured database for discovery."""
        cfg = str(configured_database or "").strip()
        op = str(operational_database or "").strip()
        if not op:
            raise ValueError("SQL Server operational row requires nonblank source_database")
        if not cfg:
            return op
        if cfg.casefold() != op.casefold():
            raise ValueError(
                f"operational database {op!r} does not match configured connection database {cfg!r}"
            )
        return cfg

    def get_jdbc_url_and_props(self, source_server=None, source_database=None):
        """Build the SQL Server JDBC url + props from the secret scope.

        The ``source_database`` is taken from the effective override or the control row.
        The server is taken from the row, else a documented ``sqlserver-host``
        secret. Connections always use encrypt=true; the server certificate is
        trusted only when the registered connection sets
        ``trust_server_certificate=true`` (default false). Production
        deployments must use a trusted certificate with trust disabled.
        """
        user = self._get_secret("sqlserver-user")
        password = self._get_secret("sqlserver-password")

        database = source_database if source_database is not None else self.source_database
        if not database or not str(database).strip():
            raise ValueError(
                "SQL Server source_database is required (control row or override)")
        database = validate_database(str(database).strip())

        url_template = self._get_secret("sqlserver-jdbc-url", required=False)
        if url_template:
            url = self._fill_url_template(url_template, database)
        else:
            server = source_server if source_server is not None else self.source_server
            if not server:
                server = self._get_secret("sqlserver-host", required=False)
            if not server:
                raise ValueError(
                    "SQL Server source_server is required (control row or "
                    "sqlserver-host secret)")
            server = validate_server(server)
            port = self._get_secret("sqlserver-port", required=False) or self.DEFAULT_PORT
            url = self._build_url(server, port, database)
        props = {"user": user, "password": password, "driver": self.DRIVER}
        return url, props

    def _build_url(self, server, port, database):
        # Split a "host,port" / "host:port" server token; explicit port wins.
        host = server
        sep_port = None
        if "," in server:
            host, sep_port = server.split(",", 1)
        elif ":" in server and "\\" not in server:
            host, sep_port = server.split(":", 1)
        effective_port = sep_port or port
        # Trust the server certificate only when explicitly enabled; default is
        # false so a production deployment uses a properly trusted certificate.
        trust = "true" if self.config.get("trust_server_certificate") else "false"
        return (
            f"jdbc:sqlserver://{host}:{effective_port};"
            f"databaseName={database};"
            "encrypt=true;"
            f"trustServerCertificate={trust}"
        )

    def _fill_url_template(self, template, database):
        """Fill the database into a URL template safely.

        Supports a ``{database}`` placeholder or an existing ``databaseName=``
        property; otherwise the validated database is appended.
        """
        t = str(template)
        if "{database}" in t:
            return t.replace("{database}", database)
        import re
        if re.search(r"(?i)databasename=", t):
            return re.sub(r"(?i)databasename=[^;]*", f"databaseName={database}", t)
        sep = "" if t.endswith(";") else ";"
        return f"{t}{sep}databaseName={database}"

    def extra_read_options(self) -> dict:
        return {}

    # ------------------------------------------------------------ metadata SQL
    def columns_metadata_query(self, source_database, source_schema, source_table):
        db = validate_database(source_database) if source_database else None
        return ssb.columns_metadata_query(db, source_schema, source_table)

    def primary_key_query(self, source_database, source_schema, source_table):
        db = validate_database(source_database) if source_database else None
        return ssb.primary_key_query(db, source_schema, source_table)

    def batch_columns_metadata_query(self, source_database, tables: list):
        db = validate_database(source_database) if source_database else None
        return ssb.batch_columns_metadata_query(db, tables)

    def batch_primary_key_query(self, source_database, tables: list):
        db = validate_database(source_database) if source_database else None
        return ssb.batch_primary_key_query(db, tables)

    def top_n_probe_query(self, source_database, source_schema, source_table, n):
        db = validate_database(source_database) if source_database else None
        return ssb.build_top_n_probe(db, source_schema, source_table, n)

    def count_query(self, source_database, source_schema, source_table):
        db = validate_database(source_database) if source_database else None
        return ssb.build_count_query(db, source_schema, source_table)

    def min_max_query(self, source_database, source_schema, source_table, column):
        db = validate_database(source_database) if source_database else None
        return ssb.build_min_max_query(db, source_schema, source_table, column)

    def upper_watermark_query(self, source_database, source_schema, source_table,
                              watermark_column, watermark_type):
        db = validate_database(source_database) if source_database else None
        return ssb.build_upper_watermark_query(
            db, source_schema, source_table, watermark_column, watermark_type)

    def full_extract_query(self, source_database, source_schema, source_table,
                           columns=None, watermark_column=None, watermark_type=None):
        db = validate_database(source_database) if source_database else None
        return ssb.build_full_extract_query(
            db, source_schema, source_table, columns,
            watermark_column=watermark_column, watermark_type=watermark_type)

    def incremental_extract_query(self, source_database, source_schema, source_table,
                                  watermark_column, watermark_type, lower_watermark,
                                  upper_watermark, columns=None):
        db = validate_database(source_database) if source_database else None
        return ssb.build_incremental_extract_query(
            db, source_schema, source_table, watermark_column, watermark_type,
            lower_watermark, upper_watermark, columns)

    # ------------------------------------------------------- discovery SQL
    def accessible_databases_query(self) -> str:
        """SQL Server online, accessible, non-system databases as database_name."""
        return ssb.accessible_databases_query()

    def list_schemas_query(self, source_database=None):
        db = validate_database(source_database) if source_database else None
        return ssb.list_schemas_query(db)

    def list_tables_query(self, source_database=None, source_schema=None):
        db = validate_database(source_database) if source_database else None
        return ssb.list_tables_query(db, source_schema)

    def list_views_query(self, source_database=None, source_schema=None):
        db = validate_database(source_database) if source_database else None
        return ssb.list_views_query(db, source_schema)

    def list_routines_query(self, source_database=None, source_schema=None):
        db = validate_database(source_database) if source_database else None
        return ssb.list_routines_query(db, source_schema)

    def table_statistics_query(self, source_database=None, source_schema=None):
        db = validate_database(source_database) if source_database else None
        return ssb.table_statistics_query(db, source_schema)

    def module_definition_query(self, source_database, source_schema=None,
                                object_name=None):
        db = validate_database(source_database) if source_database else None
        return ssb.module_definition_query(db, source_schema, object_name)

    # ------------------------------------------------------- watermark policy
    def normalize_watermark_type(self, source_type):
        return ssb.normalize_watermark_type(source_type)

    def is_supported_watermark_type(self, source_type):
        return ssb.is_supported_watermark_type(source_type)

    def watermark_type_rank(self, source_type):
        return ssb.watermark_type_rank(source_type)

    def initial_watermark_value(self, source_type):
        if not self.is_supported_watermark_type(source_type):
            raise ValueError(f"Unsupported SQL Server watermark type: {source_type!r}")
        return "1900-01-01T00:00:00.000000Z"

    # ------------------------------------------------------- partition policy
    def resolve_partition_plan(self, source_metadata, target_type, min_value,
                               max_value, requested_partitions):
        meta = source_metadata or {}
        return part.resolve_partitioning_sqlserver(
            meta.get("data_type"), target_type, min_value, max_value,
            requested_partitions)

    # ------------------------------------------------------------ type mapper
    def load_type_mapper(self):
        return SqlServerTypeMapper.from_yaml_path(self._type_rules_path())

    def type_rules_file(self) -> str:
        return "type_rules_sqlserver.yaml"

    def legacy_secret_scope_widget(self):
        return "sqlserver_secret_scope"

    def validate_connection_metadata(self, connection) -> None:
        super().validate_connection_metadata(connection)
        # SQL Server permits a blank source_database for multi-database discovery mode.

    # ---------------------------------------------------------- column policy
    def apply_column_policy(self, column_metadata, proposed_mapping):
        """SQL Server column policy, expressed as canonical policy codes.

        A hidden/system-generated column is excluded from the target write
        projection; a computed column is not an ordinary writable source column
        and needs sign-off; a rowversion/timestamp column keeps its existing
        binary mapping but is flagged as non-writable version metadata.
        """
        meta = {str(k).lower(): v for k, v in dict(column_metadata or {}).items()}
        status = proposed_mapping.status
        fidelity = proposed_mapping.fidelity
        notes = proposed_mapping.notes or ""

        if self._flag(meta.get("is_hidden")):
            return ColumnPolicyResult(
                include_column=False, mapping_status="BLOCKED",
                mapping_fidelity="UNKNOWN",
                notes="hidden/system-generated column is not migrated automatically",
                is_writable=False, requires_review=True,
                policy_code=SOURCE_HIDDEN_COLUMN)

        if self._flag(meta.get("is_computed")):
            return ColumnPolicyResult(
                include_column=True, mapping_status="REVIEW",
                mapping_fidelity="UNKNOWN",
                notes="computed column requires explicit approval before materialization",
                is_writable=False, requires_review=True,
                policy_code=SOURCE_GENERATED_COLUMN)

        if self._flag(meta.get("is_rowversion")):
            return ColumnPolicyResult(
                include_column=True, mapping_status=status,
                mapping_fidelity=fidelity,
                notes=notes or "row-version column is source-maintained binary metadata",
                is_writable=False, requires_review=False,
                policy_code=SOURCE_BINARY_VERSION_COLUMN)

        return ColumnPolicyResult(
            include_column=True, mapping_status=status,
            mapping_fidelity=fidelity, notes=notes, is_writable=True,
            requires_review=(status or "").upper() == "REVIEW",
            policy_code=None)

    def _type_rules_path(self):
        override = self.config.get("type_rules_path")
        if override and os.path.isfile(override):
            return override
        here = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))))
        cand = os.path.join(here, "config", "type_rules_sqlserver.yaml")
        if os.path.isfile(cand):
            return cand
        raise FileNotFoundError(f"SQL Server type rules not found: {cand}")
