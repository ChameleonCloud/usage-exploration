"""
Docstring for chameleon_usage.make_spans

Loads parquet files for one site.
Validates, adds typed columns, joins, emits spans
"""

import polars as pl

from chameleon_usage import schemas
from chameleon_usage.utils import SiteConfig


def load_parquet(raw_spans: str, schema: str, table: str) -> pl.LazyFrame:
    path = f"{raw_spans}/{schema}.{table}.parquet"
    return pl.scan_parquet(path)


class RawSpansLoader:
    def __init__(self, site_conf: SiteConfig):
        self.raw_spans = site_conf.raw_spans

    def load_raw_tables(self):
        """
        Loads blazar computehost and properties.
        """

        self.blazar_hosts = schemas.BlazarHostRaw.validate(
            load_parquet(self.raw_spans, "blazar", "computehosts")
        )

        self.blazar_allocations = schemas.BlazarAllocationRaw.validate(
            load_parquet(self.raw_spans, "blazar", "computehost_allocations")
        )

        self.blazar_leases = schemas.BlazarLeaseRaw.validate(
            load_parquet(self.raw_spans, "blazar", "leases")
        )

        self.blazar_reservations = schemas.BlazarReservationRaw.validate(
            load_parquet(self.raw_spans, "blazar", "reservations")
        )

        self.nova_hosts = schemas.NovaHostRaw.validate(
            load_parquet(self.raw_spans, "nova", "compute_nodes")
        )

    def compute_reservable_spans(self):
        return self.blazar_hosts.select(
            pl.lit("reservable_span").alias("entity_type"),
            pl.col("id").alias("entity_id"),
            pl.col("created_at").alias("start_at"),
            pl.col("deleted_at").alias("end_end"),
            pl.col("id").alias("blazar_host_id"),
            pl.col("hypervisor_hostname"),
        )

    def compute_capacity_spans(self):
        self.nova_hosts.select(
            pl.lit("capacity_span").alias("entity_type"),
            pl.col("id").alias("entity_id"),
            pl.col("id").alias("nova_host_id"),
            pl.col("hypervisor_hostname"),
            pl.col("created_at").alias("start_at"),
            pl.col("deleted_at").alias("end_at"),
        )

    def compute_committed_spans(self):
        return (
            self.blazar_allocations.join(
                self.blazar_reservations,
                how="left",
                left_on="reservation_id",
                right_on="id",
                suffix="_res",
            )
            .join(
                self.blazar_leases,
                how="left",
                left_on="lease_id",
                right_on="id",
                suffix="_lease",
            )
            .join(
                self.blazar_hosts,
                how="left",
                left_on="compute_host_id",
                right_on="id",
            )
            .select(
                pl.lit("committed_span").alias("entity_type"),
                pl.col("id").alias("entity_id"),
                pl.col("created_at"),
                pl.col("deleted_at"),
                pl.col("start_date"),
                pl.col("end_date"),
                pl.col("id").alias("allocation_id"),
                pl.col("hypervisor_hostname"),
                pl.col("compute_host_id").alias("blazar_host_id"),
                pl.col("reservation_id"),
                pl.col("lease_id"),
                # pl.col("created_at_res"),
                # pl.col("created_at").alias("created_at_alloc"),
                # pl.col("deleted_at").alias("deleted_at_alloc"),
                # pl.col("deleted_at_res"),
            )
        )

    def compute_occupied_spans(self):
        raise NotImplementedError

    def compute_active_spans(self):
        raise NotImplementedError

    def compute_spans(self):
        self.capacity_spans = self.compute_capacity_spans()
        self.reservable_spans = self.compute_reservable_spans()
        self.committed_spans = self.compute_committed_spans()

        return pl.concat(
            [
                self.capacity_spans,
                self.reservable_spans,
                self.committed_spans,
            ],
            how="diagonal",
        ).collect()
