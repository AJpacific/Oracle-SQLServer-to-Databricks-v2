"""
source_adapters.oracle - Oracle source adapter.

Wraps the existing, working Oracle logic (sql_builder / strategy / partitioning /
identifiers / type mapper) behind the shared :class:`SourceAdapter` contract so
the notebooks can route per control-table row without any Oracle-specific
branching. Behavior is intentionally identical to the pre-adapter accelerator.

Oracle connection is secret-backed (the ``oracle-migration`` scope by default).
For legacy rows with NULL ``source_server`` / ``source_database`` the adapter
uses the existing secret-configured URL, preserving current environments.
"""

from __future__ import annotations

import os

try:
    from src.source_adapters.base import SourceAdapter
    from src import sql_builder as sqlb
    from src import strategy as strat
    from src import partitioning as part
    from src.crosssourcetypemapper import CrossSourceTypeMapper
except ModuleNotFoundError:
    from source_adapters.base import SourceAdapter
    import sql_builder as sqlb
    import strategy as strat
    import partitioning as part
    from crosssourcetypemapper import CrossSourceTypeMapper


class OracleSourceAdapter(SourceAdapter):
    source_system = "oracle"
    DRIVER = "oracle.jdbc.OracleDriver"

    # ------------------------------------------------------------ connection
    def get_jdbc_url_and_props(self, source_server=None, source_database=None):
        """Build the Oracle thin JDBC url + props from the secret scope.

        Prefers a complete ``oracle-jdbc-url`` secret; otherwise assembles it
        from host/port/service. ``source_server`` / ``source_database`` from the
        control row are accepted for interface parity but Oracle connections are
        driven by the secret-configured URL to preserve existing behavior.
        """
        user = self._get_secret("oracle-user")
        password = self._get_secret("oracle-password")
        url = self._get_secret("oracle-jdbc-url", required=False)
        if not url:
            host = self._get_secret("oracle-host")
            port = self._get_secret("oracle-port")
            service = self._get_secret("oracle-service")
            url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"
        props = {"user": user, "password": password, "driver": self.DRIVER}
        return url, props

    def extra_read_options(self) -> dict:
        # Read Oracle DATE values using timestamp semantics.
        return {"oracle.jdbc.mapDateToTimestamp": "true"}

    # ------------------------------------------------------------ metadata SQL
    def columns_metadata_query(self, source_database, source_schema, source_table):
        return sqlb.columns_metadata_query(source_schema, source_table)

    def primary_key_query(self, source_database, source_schema, source_table):
        return sqlb.primary_key_query(source_schema, source_table)

    def top_n_probe_query(self, source_database, source_schema, source_table, n):
        return sqlb.build_top_n_probe(source_schema, source_table, n)

    def count_query(self, source_database, source_schema, source_table):
        return sqlb.build_count_query(source_schema, source_table)

    def min_max_query(self, source_database, source_schema, source_table, column):
        return sqlb.build_min_max_query(source_schema, source_table, column)

    def upper_watermark_query(self, source_database, source_schema, source_table,
                              watermark_column, watermark_type):
        return sqlb.build_upper_watermark_query(
            source_schema, source_table, watermark_column)

    def full_extract_query(self, source_database, source_schema, source_table,
                           columns=None, watermark_column=None, watermark_type=None):
        return sqlb.build_full_extract_query(source_schema, source_table, columns)

    def incremental_extract_query(self, source_database, source_schema, source_table,
                                  watermark_column, watermark_type, lower_watermark,
                                  upper_watermark, columns=None):
        return sqlb.build_incremental_extract_query(
            source_schema, source_table, watermark_column, watermark_type,
            lower_watermark, upper_watermark, columns)

    # ------------------------------------------------------- watermark policy
    def normalize_watermark_type(self, source_type):
        return strat.normalize_watermark_type(source_type)

    def is_supported_watermark_type(self, source_type):
        return strat.is_supported_watermark_type(source_type)

    def watermark_type_rank(self, source_type):
        return strat.oracle_watermark_type_rank(source_type)

    def initial_watermark_value(self, source_type):
        if not self.is_supported_watermark_type(source_type):
            raise ValueError(f"Unsupported Oracle watermark type: {source_type!r}")
        return "1900-01-01T00:00:00.000000Z"

    # ------------------------------------------------------- partition policy
    def resolve_partition_plan(self, source_metadata, target_type, min_value,
                               max_value, requested_partitions):
        meta = source_metadata or {}
        return part.resolve_partitioning(
            meta.get("data_type"), meta.get("numeric_precision"),
            meta.get("numeric_scale"), target_type, min_value, max_value,
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
        candidates = [
            os.path.join(here, "config", "type_rules_oracle.yaml"),
            os.path.join(here, "config", "type_rules.yaml"),
        ]
        for cand in candidates:
            if os.path.isfile(cand):
                return cand
        raise FileNotFoundError(
            "Oracle type rules not found. Checked: " + ", ".join(candidates))
