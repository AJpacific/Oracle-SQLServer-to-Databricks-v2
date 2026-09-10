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
    from src.source_adapters.base import SourceAdapter
    from src import sqlserver_sql_builder as ssb
    from src import partitioning as part
    from src.identifiers import validate_server, validate_database
    from src.crosssourcetypemapper import CrossSourceTypeMapper
except ModuleNotFoundError:
    from source_adapters.base import SourceAdapter
    import sqlserver_sql_builder as ssb
    import partitioning as part
    from identifiers import validate_server, validate_database
    from crosssourcetypemapper import CrossSourceTypeMapper


class SqlServerSourceAdapter(SourceAdapter):
    source_system = "sqlserver"
    DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    DEFAULT_PORT = "1433"

    # ------------------------------------------------------------ connection
    def get_jdbc_url_and_props(self, source_server=None, source_database=None):
        """Build the SQL Server JDBC url + props from the secret scope.

        The ``source_database`` MUST come from the control row (validated). The
        server is taken from the row, else a documented ``sqlserver-host``
        secret. For the current GCP POC, encrypt=true and
        trustServerCertificate=true are used because the SQL Server VM
        presents a certificate whose CA chain is not trusted by the
        Databricks JVM. Production deployments must use a trusted
        certificate and trustServerCertificate=false.
        """
        user = self._get_secret("sqlserver-user")
        password = self._get_secret("sqlserver-password")

        database = source_database if source_database is not None else self.source_database
        if not database:
            raise ValueError(
                "SQL Server source_database is required (from the control row)")
        database = validate_database(database)

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
        trust = "true"
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
        return CrossSourceTypeMapper.from_yaml_path(self._type_rules_path())

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
