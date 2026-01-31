# Time-boxed from scratch exercise.

## Plan of attack:

1. make sure we get the data.


## What inputs do we *need*

* chi@uc (baremetal)
* chi@tacc (baremetal)
* kvm@tacc (kvm)
* chi@edge (zun)

* Baremetal
  * nova computenodes
  * blazar hosts
  * blazar leases
  * blazar reservations (type physical:host)
  * blazar computehost_allocations
  * blazar extra capabilities
  * blazar resource properties

* KVM
  * nova computenodes
  * blazar hosts
  * blazar leases
  * blazar reservations (type instance / flavor, need name)
  * blazar allocations??
  * blazar extra capabilities
  * blazar resource properties

* Edge
  * zun computehosts
  * blazar devices
  * blazar leases
  * blazar device reservations (type device need name)
  * blazar device allocations??
  * blazar extra capabilities
  * blazar resource properties


## what data outputs do we *need*

### "usage_type" breakdown:
* P1
    * total
    * reservable
    * committed (active allocations)
* P2
    * occupied (instance/container using allocation)
* P3
    * ondemand vs reservable split
* P4 
    * split occupied into active, not active

Format of:
* timstamp, entity_id, entity_type, usage_type, usage_value

### Lead times breakdown
* P2
  * Just lease "scheduled start" - "created"

Format of:
  * timestamp, lead time, (group columns)

### Grouping columns

* node_type (from blazar capabilities)
* resource values (from variety of sources: nodes, vcpus, memory_mb, disk_gb, gpus)


### Plots of this output data

1. per site "stacked usage over time"
2. per site "% used over time"
3. per site "lead time by XXX TBD"

## What algorithm gets us there

* P1
  1. raw intervals to clean intervals (may be no-op at first).
     * group by (entity_id, usage_type)
  2. clean intervals to deltas
  3. deltas to cumulative usage
     * group by (usage_type)
