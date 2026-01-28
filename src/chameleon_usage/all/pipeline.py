import polars as pl

from chameleon_usage import spans
from chameleon_usage.common import PipelineOutput, SiteConfig


class UsagePipeline:
    def __init__(self, site_conf: SiteConfig):
        self.site_conf = site_conf
        self.span_loader = spans.RawSpansLoader(self.site_conf)
        self.legacy_usage_counts = pl.LazyFrame()

    # TODO: properly compute self.legacy_usage_counts from hours and node counts
    def load_legacy_usage(self) -> None:
        self.legacy_usage = self.span_loader.legacy_usage
        self.legacy_counts = self.span_loader.legacy_node_counts

    def compute_spans(self) -> PipelineOutput:
        """
        Orchestrates the loading, cleaning, and stacking of all span types.
        """
        sources: list[spans.BaseSpanSource] = [
            spans.NovaHostSource(self.span_loader),
            spans.BlazarHostSource(self.span_loader),  # when you add it
            spans.BlazarCommitmentSource(self.span_loader),
            spans.NovaOccupiedSource(self.span_loader),
        ]

        valids: list[pl.LazyFrame] = []
        audits: list[pl.LazyFrame] = []
        raws: list[pl.LazyFrame] = []
        for src in sources:
            v, a = src.get_spans()
            valids.append(v)
            audits.append(a)

            # each span source provides a lazy fetcher for the source data
            # TODO: store on the span loader instance as instance variable??
            r = src.get_raw_events()
            raws.append(r.with_columns(source=pl.lit(src.source_name)))

        return PipelineOutput(
            valid_spans=pl.concat(valids),
            audit_spans=pl.concat(audits, how="diagonal"),
            raw_spans=pl.concat(raws, how="diagonal"),
            legacy_usage=self.legacy_usage_counts,
        )
