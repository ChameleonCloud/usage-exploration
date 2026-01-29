import polars as pl

from chameleon_usage import adapters
from chameleon_usage.config import AdapterDef, FileResource, Inputs, SourceConfig
from chameleon_usage.constants import Cols, QuantityTypes, Sources
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


# The "Recipe Book"
ADAPTER_REGISTRY = {
    "nova_compute": AdapterDef(
        adapter_class=adapters.GenericFactAdapter,
        required_inputs=[Inputs.NOVA_COMPUTE],
        config=SourceConfig(
            quantity_type=QuantityTypes.TOTAL,
            source=Sources.NOVA,
            col_map={
                Cols.ENTITY_ID: "hypervisor_hostname",
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    ),
    "nova_instance": AdapterDef(
        adapter_class=adapters.GenericFactAdapter,
        required_inputs=[Inputs.NOVA_INSTANCES],
        config=SourceConfig(
            quantity_type=QuantityTypes.OCCUPIED,
            source=Sources.NOVA,
            col_map={
                Cols.ENTITY_ID: "node",
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    ),
    "blazar_host": AdapterDef(
        adapter_class=adapters.GenericFactAdapter,
        required_inputs=[Inputs.BLAZAR_HOSTS],
        config=SourceConfig(
            quantity_type=QuantityTypes.RESERVABLE,
            source=Sources.BLAZAR,
            col_map={
                Cols.ENTITY_ID: "hypervisor_hostname",
                Cols.CREATED_AT: "created_at",
                Cols.DELETED_AT: "deleted_at",
            },
        ),
    ),
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
    for adapter_def in ADAPTER_REGISTRY.values():
        inputs = [loaded[i] for i in adapter_def.required_inputs]
        if len(inputs) == 1:
            adapter = adapter_def.adapter_class(inputs[0], adapter_def.config)
        else:
            adapter = adapter_def.adapter_class(*inputs, adapter_def.config)
        facts.append(adapter.to_facts())

    return pl.concat(facts)
