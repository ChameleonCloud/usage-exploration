# Architecture Diagram
```mermaid
graph TD
    classDef source fill:#e3f2fd,stroke:#1565c0,stroke-width:1px;
    classDef lake fill:#e0f7fa,stroke:#00838f,stroke-width:2px;
    classDef table fill:#fff9c4,stroke:#fbc02d,stroke-width:1px;
    classDef adapter fill:#ffe0b2,stroke:#e65100,stroke-width:2px;
    classDef timeline fill:#ffe0b2,stroke:#e65100,stroke-width:2px;
    classDef engine fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    classDef output fill:#f3e5f5,stroke:#8e24aa,stroke-width:2px;

    s.uc_db["Openstack Region DB"]:::source
    s.back01["DB Backups"]:::source
    s.prom["Prometheus"]:::source
    s.other["Other"]:::source

    subgraph Lake ["Parquet Lake"]
        n.node["computenodes"]:::table
        b.host["computehost"]:::table
        b.lease["lease"]:::table
        b.res["reservation"]:::table
        b.alloc["allocation"]:::table
        n.instance["instances"]:::table
    end

    subgraph Adapters
        n_host["Nova Capacity"]
        n_inst["Nova Usage"]
        b_host["Blazar Capacity"]
        b_alloc["Blazar Committed"]
    end


    subgraph Resolver ["Resolve Sources"]
        P00["Group (Entity ID, Timeline)"]:::engine
        P01["Source Priority"]:::engine
        P02["Build Spans"]:::engine
    end


    subgraph Timelines
        total["Total"]
        reservable["Reservable"]
        committed["Committed"]
        occupied["Occupied"]
    end

    
    
    subgraph Pipeline ["Computation"]
        P03["Group(Timeline)"]:::engine
        P04["Cumulative Sum"]:::engine
        P05["Apply Constraints"]:::engine
    end


    s.uc_db & s.back01 & s.prom & s.other --> Lake

    n.node --> n_host
    n.node & b.host --> b_host
    b.host & b.lease & b.res & b.alloc --> b_alloc
    n.instance --> n_inst

    n_host --> P00
    b_host --> P00
    b_alloc --> P00
    n_inst --> P00

    P00 --> P01 --> P02
    P02 --> total & reservable & committed & occupied --> P03
    P03 --> P04 --> P05

    P05 --> output
    output["Usage Timeline"]:::output
