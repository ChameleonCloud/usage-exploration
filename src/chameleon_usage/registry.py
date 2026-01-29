import polars as pl

from chameleon_usage import adapters
from chameleon_usage.config import AdapterDef, FileResource, Inputs, SourceConfig
from chameleon_usage.constants import Cols, Sources
from chameleon_usage.constants import QuantityTypes as QT
from chameleon_usage.models import raw

SOURCE_CATALOG = {
    Inputs.NOVA_COMPUTE: FileResource("nova.compute_nodes", raw.NovaHostRaw),
    Inputs.NOVA_SERVICE: FileResource("nova.services", raw.NovaServiceRaw),
    Inputs.NOVA_INSTANCES: FileResource("nova.instances", raw.NovaInstanceRaw),
    Inputs.BLAZAR_HOSTS: FileResource("blazar.computehosts", raw.BlazarHostRaw),
    Inputs.BLAZAR_ALLOC: FileResource(
        "blazar.computehost_allocations", raw.BlazarAllocationRaw
    ),
    Inputs.BLAZAR_RES: FileResource("blazar.reservations", raw.BlazarReservationRaw),
    Inputs.BLAZAR_LEASES: FileResource("blazar.leases", raw.BlazarLeaseRaw),
}


def nova_host_source(input, entity_column):
    return AdapterDef(
        adapter_class=adapters.GenericFactAdapter,
        required_inputs=[input],
        config=SourceConfig(
            quantity_type=QT.TOTAL,
            source=Sources.NOVA,
            col_map={
                Cols.ENTITY_ID: entity_column,
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    )


nova_computenode = AdapterDef(
    adapter_class=adapters.GenericFactAdapter,
    required_inputs=[Inputs.NOVA_COMPUTE],
    config=SourceConfig(
        quantity_type=QT.TOTAL,
        source=Sources.NOVA,
        col_map={
            Cols.ENTITY_ID: "hypervisor_hostname",
            Cols.CREATED_AT: "created_at",
            Cols.DELETED_AT: "deleted_at",
        },
    ),
)
nova_service = AdapterDef(
    adapter_class=adapters.GenericFactAdapter,
    required_inputs=[Inputs.NOVA_SERVICE],
    config=SourceConfig(
        quantity_type=QT.TOTAL,
        source=Sources.NOVA,
        col_map={
            Cols.ENTITY_ID: "host",
            Cols.CREATED_AT: "created_at",
            Cols.DELETED_AT: "deleted_at",
        },
        filter_expr=pl.col("binary") == "nova-compute",
    ),
)

nova_instance = AdapterDef(
    adapter_class=adapters.GenericFactAdapter,
    required_inputs=[Inputs.NOVA_INSTANCES],
    config=SourceConfig(
        quantity_type=QT.OCCUPIED,
        source=Sources.NOVA,
        col_map={
            Cols.ENTITY_ID: "node",
            Cols.CREATED_AT: "created_at",
            Cols.DELETED_AT: "deleted_at",
        },
    ),
)


def blazar_host(quantity_type=QT.RESERVABLE):
    return AdapterDef(
        adapter_class=adapters.GenericFactAdapter,
        required_inputs=[Inputs.BLAZAR_HOSTS],
        config=SourceConfig(
            quantity_type=quantity_type,
            source=Sources.BLAZAR,
            col_map={
                Cols.ENTITY_ID: "hypervisor_hostname",
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    )


def blazar_allocation(quantity_type=QT.COMMITTED):
    return AdapterDef(
        adapter_class=adapters.BlazarAllocationAdapter,
        required_inputs=[
            Inputs.BLAZAR_ALLOC,
            Inputs.BLAZAR_RES,
            Inputs.BLAZAR_LEASES,
        ],
        config=SourceConfig(
            quantity_type=quantity_type,
            source=Sources.BLAZAR,
            col_map={
                Cols.ENTITY_ID: "hypervisor_hostname",
                # Cols.ENTITY_ID: "id",
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    )


# when two adapters contribute to same quantity type, higher in list has priorirt
ADAPTER_REGISTRY = {
    ## Primary 4 Sources
    "nova_compute": nova_computenode,
    "blazar_host": blazar_host(),
    "blazar_allocation": blazar_allocation(),
    "nova_instance": nova_instance,
    ## Supplemental Rules
    # "blazar_allocation_res_cap": blazar_allocation(quantity_type=QT.RESERVABLE),
    # "blazar_host_total_cap": blazar_host(QT.TOTAL),
    # "nova_service": nova_service,
    # Allocation implies blazar host rule
    # "blazar_allocation_total_cap": blazar_allocation(quantity_type=QT.TOTAL),
}


def load_facts(base_path: str, site_name: str) -> pl.LazyFrame:
    path = f"{base_path}/{site_name}"

    # Collect only the inputs that adapters need
    needed_inputs = set()
    for adapter_def in ADAPTER_REGISTRY.values():
        needed_inputs.update(adapter_def.required_inputs)

    # Load only needed files
    loaded: dict[Inputs, pl.LazyFrame] = {}
    for input_key in needed_inputs:
        resource = SOURCE_CATALOG[input_key]
        loaded[input_key] = resource.model.validate(
            pl.scan_parquet(f"{path}/{resource.filename}.parquet")
        )
    # Run each adapter
    facts = []
    for adapter_source, adapter_def in ADAPTER_REGISTRY.items():
        inputs = [loaded[i] for i in adapter_def.required_inputs]
        if len(inputs) == 1:
            adapter = adapter_def.adapter_class(inputs[0], adapter_def.config)
        else:
            adapter = adapter_def.adapter_class(*inputs, adapter_def.config)
        facts.append(
            adapter.to_facts().with_columns(pl.lit(adapter_source).alias("source"))
        )

    return pl.concat(facts)
