# CHANGELOG

---
## X.Y.Z (2025-08-12)
### CHANGES
- Implement partitioned grain registry for horizontal scaling

  Refactor erleans_pm from single-server architecture to partitioned
  registry system using gproc_pool for consistent grain distribution
  across N partition workers, improving performance and scalability.

  **Core Changes:**
  - Split erleans_pm into router + partition architecture
  - Create erleans_registry_partition: individual CRDT partition workers
  - Create erleans_registry_sup: manages N partitions via gproc_pool
  - Convert erleans_pm to router using consistent hashing for grain distribution

  **Technical Implementation:**
  - Use gproc_pool hash strategy for deterministic grain-to-partition routing
  - Maintain same public API for backward compatibility
  - Preserve all CRDT synchronization and conflict resolution logic
  - Add configurable pm_partitions setting (default: 1, recommend: 4)

  **Configuration & Dependencies:**
  - Update startup order: erleans_config before erleans_registry_sup
  - Add pm_partitions config to test environments
  - Fix partisan peer service configuration in distributed tests
  - Integrate with erleans_config API instead of direct application:get_env

  **Testing & Validation:**
  - Add comprehensive registry_partition_SUITE for partition verification
  - Add partition_logic_test for gproc_pool behavior analysis
  - Fix dist_lifecycle_SUITE startup issues with partisan configuration
  - Preserve all existing test functionality through API compatibility

  **Benefits:**
  - Horizontal scaling: distribute grain load across multiple partitions
  - Improved concurrency: reduce contention on single registry server
  - Consistent routing: same grain always maps to same partition
  - Backward compatible: existing code works without changes