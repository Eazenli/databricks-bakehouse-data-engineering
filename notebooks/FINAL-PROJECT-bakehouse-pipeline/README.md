#### Project structure
```text
final-project-bakehouse-pipeline/
│
├── README.md
│
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── ingestion.py
│   ├── transformations.py
│   ├── quality.py
│   └── pipeline.py
│
├── tests/
│   ├── conftest.py
│   ├── test_transformations.py
│   └── test_quality.py
│
├── notebooks/
│   ├── 01_initialization
│   ├── 02_run_pipeline
│   ├── 03_business_analysis
│   ├── 04_final_validation
│   └── 05_performance_review
│
└── evidence/
```
Customer and franchise dimensions are built during initialization and treated as fixed reference tables. Transaction facts are processed incrementally using simulated batches A and B (divided by date). Bronze preserves every receipt, Silver uses an insert-only Delta _MERGE_ for idempotence, and Gold is fully rebuilt from cumulative Silver state.

#### Schema for pipeline
```test
                 SOURCES: bakehouse 
             ┌─────────────┼─────────────┐
             │             │             │
        transactions    customers    franchises
             │             │             │
             ▼             ▼             ▼
          BRONZE         BRONZE         BRONZE
             │             │             │
             │          validate       validate
             │             │             │
             │             ▼             ▼
             │          SILVER         SILVER
             │         customers      franchises
             │             │             │ 
             │             └──────┬──────│
             │                    │      │
             ▼                    │      │
       validate / dedup ◄─────────┘      │
             │           check ID        │
       ┌─────┴─────┐                     │
       ▼           ▼                     │
    invalid      SILVER                  │
              transactions               │
                    └──────────┬─────────┘             
                               ▼
                              GOLD
                    daily franchise sales
```
