# Usage reporting implementation plan

## Phase 1 Deliverables

### Usage
Collector type:
* Legacy # values from chameleon_usage table
* New    # this tool

"""
Output categories
General: Total usage pool, reservable+ondemand+unknown
reservable: reservable usage pool
committed: pool of active allocations, usable for reservable instances
occupied: pool of capacity occupied by nova instances
"""
Usage type:
* General
* Reservable
* Committed
* Occupied
<!-- * Active # Optional -->

Value type:
* nodes
<!-- * vcpus -->
<!-- * memory_mb -->
<!-- * disk_gb -->
<!-- * gpus -->

Grouping Colums(optional):
* Node Type
* Hypervisor Hostname

Usage Timeline:
* timestamp
  * collector type
  * usage type
  * value type
  * value quantity
  * node type           # group by column
  * hypervisor hostname # group by column

-------------------------------------------------------------------
| timestamp | Collector Type | Count Type | Value | (Group_cols,) |
-------------------------------------------------------------------

Derived States:
* Available = Reservable - Comitted
* Idle = Comitted - Occupied
<!-- * Stopped = Occupied - Active -->

Plots:
1. Plot legacy vs current for each count_type
   a. output/plots/chi_tacc_legacy_facets.png
   b. output/plots/chi_uc_legacy_facets.png
2. Plot stacked usage:
   * black line total cap
   * dashed line reservable cap
   * ? line legacy reservable cap
   * area: maintenance cap
   * area: available cap
   * area: Idle cap
   * area: used cap
   a. output/plots/chi_tacc_usage_stack.png
   b. output/plots/chi_uc_usage_stack.png
   <!-- a. output/plots/kvm_tacc_usage_stack.png -->
3. Cross-site utilization
   * black line total cap
   * colors per site: 
     * green->UC
     * orange->tacc
     <!-- * yellow-kvm -->
   * light -> "available capacity"
   * dark -> "used capacity: reservable UNION used"
   a. output/plots/cross_site_usage.png

### Lead Time

Leadtime Type:
* requested (scheduled start - created)
* effective (actual_start - created) # need to handle case where lease was deleted prior to start, failed to start, ...
<!-- * projected (reconstruct past timeline) -->

Lead Time Timeline:
----------------------------------------------------------------
| timestamp | Leadtime Type | Value (duration) | (Group_cols,) |
----------------------------------------------------------------

Leadtime Plot
* point plot: x=timestamp, y=value, hue=node_type.
   * compare requested vs effective # Optional, Sanity check

## Phase 1 implementation


### Input Schemas

* Nova
  * computenode
  * instance
* blazar
  * lease
  * reservation
  * computehost_allocation
  * computehost
  * computehost_extra_capability
* legacy
  * node count cache
  * node hours cache

### Pipeline Schemas

* Facts Timeline
  * timestamp
  * source  # table+sufficient_id
  * entity_id  # primary key from table
  * quantity_type
  * value
  * node type           # group by column
  * hypervisor hostname # group by column


### Output Schemas


* uniqueness:
   * grouping produces exactly 1 row per group:
      * site, timestamp, collector type, count type
* Usage Timeline:
  * site
  * timestamp
  * collector type
  * count type
  * value
  <!-- * node type           # group by column -->
  <!-- * hypervisor hostname # group by column -->

* uniqueness:
   * grouping produces exactly 1 row per group:
      * site, timestamp, leadtime type
* leadtime timeline:
  * site
  * timestamp
  * leadtime type
  * value
  <!-- * node type           # group by column -->
  <!-- * hypervisor hostname # group by column -->


## Acceptance Criteria
* Date coverage: data includes 2016-01-01 through 2025-12-31. UTC
* known gaps
   * nova hosts prior to 2018
   * blazar hosts prior to 2020
* all schemas have columns, no extra columns
