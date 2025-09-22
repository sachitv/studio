# Requirements Document

## Introduction

This feature enhances the `CachingIterableSource` to create an intelligent link between backfill cache results and forward iteration blocks. Currently, backfill operations are performed independently and their results are not efficiently reused when forward iteration begins from the same time point. This enhancement will create a bidirectional relationship between backfill cache entries and forward blocks, improving performance for common robotics data access patterns where backfill is followed by forward playback.

## Requirements

### Requirement 1

**User Story:** As a robotics engineer using Lichtblick for data analysis, I want backfill results to be automatically cached and linked to subsequent forward iteration blocks, so that I get faster data access when jumping to different time points and starting playback.

#### Acceptance Criteria

1. WHEN a forward iteration block is created THEN the system SHALL create exactly one corresponding backfill block
2. WHEN a forward block is closed THEN the system SHALL create a backfill block for the next forward block that will be created
3. WHEN a backfill block is created THEN it SHALL contain the most recent messages for all topics present in its related forward block
4. WHEN backfill blocks are linked to forward blocks THEN they SHALL be included in memory usage calculations
5. WHEN a forward block is evicted from cache THEN its corresponding backfill block SHALL also be evicted

### Requirement 2

**User Story:** As a developer working with the caching system, I want backfill cache entries to have proper lifecycle management tied to forward blocks, so that memory usage remains bounded and cache coherency is maintained.

#### Acceptance Criteria

1. WHEN backfill blocks are created THEN they SHALL have a timestamp identifier matching the start time of their corresponding forward block
2. WHEN forward blocks are created THEN the system SHALL maintain a 1:1 relationship with backfill blocks
3. WHEN the cache eviction algorithm runs THEN backfill blocks SHALL be considered as part of the total memory footprint alongside their forward blocks
4. WHEN topics change and cache is cleared THEN all backfill blocks SHALL also be cleared
5. WHEN a forward block is removed due to memory pressure THEN its corresponding backfill block SHALL be removed in the same atomic operation

### Requirement 3

**User Story:** As a user performing time-based navigation in Lichtblick, I want the system to efficiently handle rapid time jumps by reusing cached backfill data, so that I experience smooth performance during data exploration.

#### Acceptance Criteria

1. WHEN performing rapid time jumps THEN the system SHALL reuse existing backfill blocks when their corresponding forward blocks are available
2. WHEN backfill blocks exist for a timestamp THEN they SHALL be returned without querying the underlying data source
3. WHEN backfill blocks are accessed THEN their access time SHALL be updated for LRU eviction purposes along with their forward block
4. WHEN forward iteration begins at a timestamp with a cached backfill block THEN the backfill results SHALL be immediately available
5. WHEN memory pressure occurs THEN backfill blocks SHALL be evicted together with their forward blocks using the same LRU strategy

### Requirement 4

**User Story:** As a system administrator monitoring Lichtblick performance, I want backfill cache operations to be observable and debuggable, so that I can understand cache behavior and optimize system performance.

#### Acceptance Criteria

1. WHEN backfill cache entries are created THEN the system SHALL log the operation with timestamp and size information
2. WHEN backfill cache entries are linked to forward blocks THEN the system SHALL emit appropriate debug information
3. WHEN backfill cache entries are evicted THEN the system SHALL log the eviction with reason and memory impact
4. WHEN requesting cache statistics THEN the system SHALL include backfill cache metrics in the response
5. WHEN cache coherency issues occur THEN the system SHALL provide detailed error information for debugging

### Requirement 5

**User Story:** As a system architect, I want to ensure that backfill blocks maintain perfect synchronization with forward blocks and contain accurate topic data, so that the cache provides consistent and correct backfill results.

#### Acceptance Criteria

1. WHEN the cache contains N forward blocks THEN it SHALL contain exactly N backfill blocks
2. WHEN a backfill block is created for a forward block THEN it SHALL contain the most recent message for every topic present in that forward block
3. WHEN a forward block is closed and a new one will be created THEN a backfill block SHALL be immediately created for the next forward block's start time
4. WHEN topics in a forward block change THEN the corresponding backfill block SHALL be updated to reflect the new topic set
5. WHEN a backfill block is queried THEN it SHALL return messages that represent the state of all topics at the exact start time of its corresponding forward block

### Requirement 6

**User Story:** As a developer extending the caching system, I want the backfill cache integration to maintain backward compatibility with existing APIs, so that current functionality continues to work without modification.

#### Acceptance Criteria

1. WHEN existing code calls getBackfillMessages THEN the method SHALL continue to work with the same signature and behavior
2. WHEN existing code uses messageIterator THEN forward iteration SHALL work unchanged while gaining backfill cache benefits
3. WHEN existing eviction logic runs THEN it SHALL handle both forward blocks and backfill blocks transparently
4. WHEN existing memory management code executes THEN it SHALL account for backfill block memory usage automatically
5. WHEN existing cache clearing operations occur THEN they SHALL clear both forward blocks and backfill blocks
