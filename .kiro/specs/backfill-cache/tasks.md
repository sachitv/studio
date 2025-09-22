# Implementation Plan

- [x] 1. Define BackfillBlock interface and core data structures

  - Create TypeScript interface for BackfillBlock with id, timestamp, messages map, lastAccess, size, and forwardBlock reference
  - Add BackfillBlock import and type definitions to CachingIterableSource
  - Define BlockPair utility type for managing synchronized operations
  - Add backfillBlocks Map storage to CachingIterableSource class
  - _Requirements: 5.1, 5.2, 2.1, 2.2_

- [x] 2. Implement basic BackfillBlock creation and storage

  - Write createBackfillBlock method that queries source for most recent messages per topic
  - Add logic to calculate BackfillBlock memory size including message data and metadata
  - Implement storeBackfillBlock method to add blocks to the backfillBlocks map
  - Create generateBackfillId utility function for unique block identification
  - Write unit tests for BackfillBlock creation and storage operations
  - _Requirements: 5.2, 5.3, 2.3, 4.1_

- [x] 3. Add bidirectional linking between forward and backfill blocks

  - Add optional backfillBlock reference field to CacheBlock interface
  - Implement linkBlocks method to establish bidirectional references between paired blocks
  - Add unlinkBlocks method to safely remove references during eviction
  - Create validateBlockLinks utility to verify reference integrity
  - Write unit tests for block linking and unlinking operations
  - _Requirements: 2.2, 5.1, 5.4, 6.3_

- [x] 4. Implement proactive backfill creation on forward block close

  - Modify forward block closing logic to extract topics from the completed block
  - Add calculateNextBlockStartTime utility to determine next forward block timestamp
  - Integrate createBackfillBlock call when closing forward blocks
  - Implement topic extraction from forward block messages for backfill creation
  - Write unit tests for automatic backfill creation during forward block lifecycle
  - _Requirements: 1.2, 5.3, 5.5, 2.1_

- [x] 5. Enhance forward block creation to link with existing backfill blocks

  - Modify forward block creation to check for existing backfill blocks at the start timestamp
  - Add logic to link existing backfill blocks to new forward blocks during creation
  - Implement fallback creation of backfill blocks when none exist for a timestamp
  - Update forward block creation to maintain 1:1 relationship invariant
  - Write unit tests for forward block creation with backfill linking
  - _Requirements: 1.1, 2.2, 5.1, 5.4_

- [x] 6. Integrate backfill blocks into memory size calculations

  - Update getTotalCacheSize method to include backfill block sizes
  - Modify memory pressure detection to account for combined forward+backfill sizes
  - Add getCombinedBlockSize utility for calculating paired block memory usage
  - Update memory usage reporting to include backfill block metrics
  - Write unit tests for memory calculation accuracy with backfill blocks
  - _Requirements: 1.4, 2.3, 3.5, 4.4_

- [x] 7. Implement synchronized eviction of forward and backfill block pairs

  - Modify eviction algorithm to identify and evict block pairs as atomic units
  - Update findEvictableBlocks to work with BlockPair objects instead of individual blocks
  - Implement evictBlockPair method that removes both forward and backfill blocks together
  - Add logic to update memory calculations after synchronized pair eviction
  - Write unit tests for synchronized eviction maintaining 1:1 relationship
  - _Requirements: 1.5, 2.5, 3.5, 5.1_

- [x] 8. Enhance getBackfillMessages to use cached backfill blocks

  - Modify getBackfillMessages to check for existing backfill blocks before querying source
  - Add cache hit path that returns messages from BackfillBlock.messages map
  - Implement cache miss path that creates and stores new backfill blocks
  - Update lastAccess time for both forward and backfill blocks on cache hits
  - Write unit tests for backfill cache hit and miss scenarios
  - _Requirements: 3.1, 3.2, 3.3, 6.1_

- [x] 9. Add topic coverage validation and repair mechanisms

  - Implement validateTopicCoverage method to ensure backfill blocks contain all forward block topics
  - Add repairTopicCoverage method to supplement missing topics from source
  - Create detectInconsistency utility to identify various consistency issues
  - Implement recovery strategies for count mismatches and broken links
  - Write unit tests for consistency validation and repair operations
  - _Requirements: 5.2, 5.4, 5.5, 4.2, 4.3_

- [x] 10. Update cache clearing operations to handle backfill blocks

  - Modify clearCache method to clear both forward blocks and backfill blocks
  - Update topic change handling to clear backfill blocks when topics change
  - Add clearBackfillBlocks utility method for selective backfill cache clearing
  - Ensure cache clearing maintains consistency between forward and backfill storage
  - Write unit tests for cache clearing operations with backfill blocks
  - _Requirements: 2.4, 6.5, 5.1_

- [x] 11. Add observability and debugging support for backfill operations

  - Add logging for backfill block creation, linking, and eviction operations
  - Implement debug information emission for cache hit/miss events
  - Add backfill block metrics to cache statistics reporting
  - Create detailed error messages for consistency validation failures
  - Write unit tests for logging and metrics collection
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 12. Implement comprehensive integration tests for end-to-end workflows

  - Write integration test for complete forward iteration with automatic backfill creation
  - Add integration test for rapid time jumps using cached backfill data
  - Create integration test for memory pressure scenarios with synchronized eviction
  - Implement integration test for topic changes and cache clearing
  - Add performance benchmark tests comparing cache hit rates before and after enhancement
  - _Requirements: 3.1, 3.4, 1.1, 2.4, 6.2_

- [x] 13. Add backward compatibility validation and API preservation

  - Verify existing getBackfillMessages API signature and behavior remains unchanged
  - Test existing messageIterator functionality continues to work with backfill enhancement
  - Validate existing eviction logic handles backfill blocks transparently
  - Ensure existing memory management code accounts for backfill blocks automatically
  - Write regression tests to prevent breaking changes to existing functionality
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 14. Optimize performance and finalize implementation
  - Profile memory usage patterns and optimize BackfillBlock storage efficiency
  - Benchmark cache hit rates and performance improvements from backfill caching
  - Optimize block linking operations for minimal overhead during creation
  - Add performance monitoring for synchronized eviction operations
  - Create comprehensive documentation for the enhanced backfill cache system
  - _Requirements: 3.4, 4.4, 2.3_
