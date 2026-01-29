import polars as pl

from chameleon_usage import adapters
from chameleon_usage.config import AdapterDef, FileResource, Inputs, SourceConfig
from chameleon_usage.constants import Cols
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


def generic_cfg(
    source_name,
    qty_type,
    required_inputs,
    id_col="hypervisor_hostname",
    expr=None,
    adapter_class=adapters.GenericFactAdapter,
):
    return AdapterDef(
        adapter_class=adapter_class,
        required_inputs=required_inputs,
        config=SourceConfig(
            quantity_type=qty_type,
            source=source_name,
            col_map={
                Cols.ENTITY_ID: id_col,
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    )


#####
# Priority flows Top down, first wins
# Identity: group_by column order
# Authority: Pivot column order
# tuple of (Entity_id, QuantityType) -> independent timeline
ADAPTER_PRIORITY = [
    generic_cfg(
        "nova_computenode", qty_type=QT.TOTAL, required_inputs=[Inputs.NOVA_COMPUTE]
    ),
    generic_cfg(
        "blazar_computehost",
        qty_type=QT.RESERVABLE,
        required_inputs=[Inputs.BLAZAR_HOSTS],
    ),
    generic_cfg(
        "blazar_allocation",
        qty_type=QT.COMMITTED,
        required_inputs=[
            Inputs.BLAZAR_ALLOC,
            Inputs.BLAZAR_RES,
            Inputs.BLAZAR_LEASES,
            Inputs.BLAZAR_HOSTS,
        ],
        adapter_class=adapters.BlazarAllocationAdapter,
    ),
    generic_cfg(
        "nova_instance",
        qty_type=QT.OCCUPIED,
        required_inputs=[Inputs.NOVA_INSTANCES],
        id_col="node",  # to hypervisor_hostname
    ),
    ####################
    # Supplemental rules
    ####################
    generic_cfg(
        "nova_compute_service",
        qty_type=QT.TOTAL,
        required_inputs=[Inputs.NOVA_SERVICE],
        expr=(pl.col("binary") == "nova-compute"),
        id_col="host",  # to hypervisor_hostname
    ),
    # blazar host implies nova host rule
    generic_cfg(
        "blazar_computehost_implies_nova",
        qty_type=QT.TOTAL,
        required_inputs=[Inputs.BLAZAR_HOSTS],
    ),
    # blazar allocation imples blazar host
    generic_cfg(
        "blazar_allocation_implies_host",
        qty_type=QT.RESERVABLE,
        required_inputs=[
            Inputs.BLAZAR_ALLOC,
            Inputs.BLAZAR_RES,
            Inputs.BLAZAR_LEASES,
            Inputs.BLAZAR_HOSTS,
        ],
        adapter_class=adapters.BlazarAllocationAdapter,
    ),
]
ADAPTER_REGISTRY = {d.config.source: d for d in ADAPTER_PRIORITY}


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

        facts.append(adapter.to_facts())

    return pl.concat(facts)
