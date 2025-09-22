# Design Document

## Overview

This design enhances the `CachingIterableSource` to implement a synchronized backfill cache system where each forward iteration block has exactly one corresponding backfill block. The backfill blocks contain the most recent messages for all topics at the start time of their paired forward block, enabling efficient time-based navigation and seamless transitions between backfill and forward iteration.

## Architecture

### Core Components

```mermaid
classDiagram
    class CachingIterableSource {
        -Map~bigint, CacheBlock~ blocks
        -Map~bigint, BackfillBlock~ backfillBlocks
        +messageIterator()
        +getBackfillMessages()
        -createBackfillBlock()
        -linkBlocks()
    }

    class CacheBlock {
        +bigint id
        +Time start
        +Time end
        +Array~Tuple~ items
        +number lastAccess
        +number size
        +BackfillBlock? backfillBlock
    }

    class BackfillBlock {
        +bigint id
        +Time timestamp
        +Map~string, MessageEvent~ messages
        +number lastAccess
        +number size
        +CacheBlock forwardBlock
    }

    class BlockPair {
        +CacheBlock forward
        +BackfillBlock backfill
        +number combinedSize
        +number lastAccess
    }

    CachingIterableSource --> CacheBlock : manages
    CachingIterableSource --> BackfillBlock : manages
    CacheBlock --> BackfillBlock : linked to
    BackfillBlock --> CacheBlock : paired with
    CachingIterableSource --> BlockPair : evicts as unit
```

### Data Structures

#### BackfillBlock Structure

```typescript
interface BackfillBlock {
  id: bigint; // Unique identifier
  timestamp: Time; // Exact timestamp (start of forward block)
  messages: Map<string, MessageEvent>; // Most recent message per topic
  lastAccess: number; // For LRU eviction
  size: number; // Memory footprint in bytes
  forwardBlock: CacheBlock; // Reference to paired forward block
}
```

#### Enhanced CacheBlock

```typescript
interface CacheBlock {
  // ... existing fields
  backfillBlock?: BackfillBlock; // Reference to paired backfill block
}
```

## Components and Interfaces

### BackfillBlock Management

#### Creation Algorithm

```mermaid
flowchart TD
    CloseForward[Forward block closed] --> GetTopics[Extract topics from forward block]
    GetTopics --> CalcNextTime[Calculate next block start time]
    CalcNextTime --> QuerySource[Query source for backfill at next time]
    QuerySource --> CreateBackfill[Create BackfillBlock]
    CreateBackfill --> StoreBackfill[Store in backfillBlocks map]
    StoreBackfill --> LinkBlocks[Link to next forward block when created]

    subgraph "Backfill Creation Details"
        QuerySource --> ForEachTopic[For each topic in forward block]
        ForEachTopic --> FindRecent[Find most recent message before timestamp]
        FindRecent --> AddToBackfill[Add to backfill messages map]
        AddToBackfill --> NextTopic{More topics?}
        NextTopic -->|Yes| ForEachTopic
        NextTopic -->|No| CreateBackfill
    end

    style CreateBackfill fill:#c8e6c9
    style LinkBlocks fill:#e3f2fd
```

#### Linking Strategy

```mermaid
sequenceDiagram
    participant Iterator as Message Iterator
    participant Cache as CachingIterableSource
    participant Forward as CacheBlock
    participant Backfill as BackfillBlock

    Iterator->>Cache: Create new forward block at time T
    Cache->>Cache: Check for existing backfill at time T

    alt Backfill exists for time T
        Cache->>Forward: Create forward block
        Cache->>Cache: Link existing backfill to forward block
        Cache->>Forward: Set backfillBlock reference
        Cache->>Backfill: Set forwardBlock reference
        Note over Cache: Perfect 1:1 pairing established
    else No backfill exists
        Cache->>Forward: Create forward block
        Cache->>Cache: Create backfill for this timestamp
        Cache->>Cache: Link new backfill to forward block
    end

    Note over Iterator,Backfill: Both blocks now have synchronized lifecycle
```

### Memory Management

#### Synchronized Eviction

```mermaid
flowchart TD
    MemoryPressure[Memory pressure detected] --> FindEvictable[Find evictable block pairs]
    FindEvictable --> SortPairs[Sort pairs by lastAccess time]
    SortPairs --> EvictPair[Evict oldest pair]

    EvictPair --> RemoveForward[Remove forward block from cache]
    RemoveForward --> RemoveBackfill[Remove backfill block from cache]
    RemoveBackfill --> UpdateSize[Update total cache size]
    UpdateSize --> CheckPressure{Still over limit?}

    CheckPressure -->|Yes| EvictPair
    CheckPressure -->|No| Complete[Eviction complete]

    subgraph "Pair Eviction Details"
        EvictPair --> UnlinkBlocks[Unlink block references]
        UnlinkBlocks --> CalcCombinedSize[Calculate combined size reduction]
        CalcCombinedSize --> RemoveForward
    end

    style EvictPair fill:#ffcdd2
    style Complete fill:#c8e6c9
```

#### Size Calculation

```typescript
interface MemoryCalculation {
  forwardBlockSize: number; // Size of forward block messages
  backfillBlockSize: number; // Size of backfill messages
  combinedSize: number; // Total pair size
  overhead: number; // Metadata and reference overhead
}
```

### Enhanced API Methods

#### getBackfillMessages Enhancement

```mermaid
flowchart TD
    GetBackfill[getBackfillMessages called] --> CheckCache{Backfill block exists?}
    CheckCache -->|Yes| UpdateAccess[Update lastAccess time]
    UpdateAccess --> ReturnCached[Return cached messages]

    CheckCache -->|No| QuerySource[Query underlying source]
    QuerySource --> CreateBlock[Create backfill block]
    CreateBlock --> CacheBlock[Cache for future use]
    CacheBlock --> ReturnNew[Return new messages]

    subgraph "Cache Hit Path"
        UpdateAccess --> ValidateTopics{All requested topics present?}
        ValidateTopics -->|Yes| ReturnCached
        ValidateTopics -->|No| SupplementFromSource[Get missing topics from source]
        SupplementFromSource --> UpdateBlock[Update backfill block]
        UpdateBlock --> ReturnCached
    end

    style ReturnCached fill:#c8e6c9
    style QuerySource fill:#fff3e0
```

## Data Models

### Block Relationship Model

```mermaid
erDiagram
    CacheBlock ||--|| BackfillBlock : paired_with
    CacheBlock {
        bigint id PK
        Time start
        Time end
        Array items
        number lastAccess
        number size
        bigint backfillBlockId FK
    }
    BackfillBlock {
        bigint id PK
        Time timestamp
        Map messages
        number lastAccess
        number size
        bigint forwardBlockId FK
    }
    BlockPair {
        bigint forwardId FK
        bigint backfillId FK
        number combinedSize
        number lastAccess
    }
    CacheBlock ||--|| BlockPair : part_of
    BackfillBlock ||--|| BlockPair : part_of
```

### Topic Coverage Model

```mermaid
graph TD
    subgraph "Forward Block Topics"
        FT1[Topic A]
        FT2[Topic B]
        FT3[Topic C]
    end

    subgraph "Backfill Block Messages"
        BM1[Topic A: Most recent message before start time]
        BM2[Topic B: Most recent message before start time]
        BM3[Topic C: Most recent message before start time]
    end

    FT1 -.-> BM1
    FT2 -.-> BM2
    FT3 -.-> BM3

    subgraph "Guarantee"
        Guarantee[Every topic in forward block has corresponding backfill message]
    end

    style Guarantee fill:#e8f5e8
```

## Error Handling

### Consistency Validation

```mermaid
stateDiagram-v2
    [*] --> Validate: Block operation
    Validate --> CheckCount: Validate block counts
    CheckCount --> CountMatch: Forward count == Backfill count
    CheckCount --> CountMismatch: Counts don't match

    CountMatch --> CheckLinks: Validate links
    CheckLinks --> LinksValid: All blocks properly linked
    CheckLinks --> LinksInvalid: Broken or missing links

    LinksValid --> CheckTopics: Validate topic coverage
    CheckTopics --> TopicsComplete: All topics covered
    CheckTopics --> TopicsIncomplete: Missing topic coverage

    TopicsComplete --> [*]: Validation passed

    CountMismatch --> RepairCounts: Repair count mismatch
    LinksInvalid --> RepairLinks: Repair broken links
    TopicsIncomplete --> RepairTopics: Repair topic coverage

    RepairCounts --> Validate: Retry validation
    RepairLinks --> Validate: Retry validation
    RepairTopics --> Validate: Retry validation

    state RepairCounts {
        [*] --> IdentifyOrphans
        IdentifyOrphans --> CreateMissing
        CreateMissing --> RemoveOrphans
        RemoveOrphans --> [*]
    }
```

### Recovery Strategies

```typescript
interface RecoveryStrategy {
  detectInconsistency(): InconsistencyType;
  repairCountMismatch(): void;
  repairBrokenLinks(): void;
  repairTopicCoverage(): void;
  validateConsistency(): boolean;
}

enum InconsistencyType {
  COUNT_MISMATCH = "count_mismatch",
  BROKEN_LINKS = "broken_links",
  INCOMPLETE_TOPICS = "incomplete_topics",
  MEMORY_LEAK = "memory_leak",
}
```

## Testing Strategy

### Unit Tests

#### Block Pairing Tests

```typescript
describe("BackfillBlock Pairing", () => {
  test("creates backfill block when forward block is closed", () => {
    // Test automatic backfill creation
  });

  test("maintains 1:1 relationship between blocks", () => {
    // Test count invariant
  });

  test("links blocks with correct references", () => {
    // Test bidirectional linking
  });

  test("includes all forward block topics in backfill", () => {
    // Test topic coverage
  });
});
```

#### Eviction Tests

```typescript
describe("Synchronized Eviction", () => {
  test("evicts forward and backfill blocks together", () => {
    // Test atomic eviction
  });

  test("updates memory calculations correctly", () => {
    // Test size accounting
  });

  test("maintains consistency after eviction", () => {
    // Test post-eviction state
  });
});
```

### Integration Tests

#### End-to-End Workflow

```mermaid
sequenceDiagram
    participant Test as Test Suite
    participant Cache as CachingIterableSource
    participant Source as Mock Source

    Test->>Cache: Start forward iteration at T1
    Cache->>Source: Request messages from T1
    Source-->>Cache: Return messages for topics A, B
    Cache->>Cache: Create forward block for T1-T2
    Cache->>Cache: Create backfill block for T2

    Test->>Cache: Jump to T2 and request backfill
    Cache->>Cache: Return cached backfill (no source query)

    Test->>Cache: Start forward iteration at T2
    Cache->>Cache: Link existing backfill to new forward block

    Test->>Cache: Trigger memory pressure
    Cache->>Cache: Evict T1 forward+backfill pair together

    Note over Test,Source: Verify 1:1 relationship maintained throughout
```

### Performance Tests

#### Memory Efficiency

```typescript
describe("Memory Management", () => {
  test("backfill blocks do not exceed memory limits", () => {
    // Test memory bounds
  });

  test("eviction maintains performance under pressure", () => {
    // Test eviction performance
  });

  test("cache hit rates improve with backfill caching", () => {
    // Test performance improvement
  });
});
```

## Implementation Phases

### Phase 1: Core Data Structures

- Implement `BackfillBlock` interface
- Add backfill block storage to `CachingIterableSource`
- Create block linking mechanisms
- Add basic validation

### Phase 2: Block Lifecycle Management

- Implement synchronized creation
- Add automatic backfill generation on forward block close
- Implement bidirectional linking
- Add topic coverage validation

### Phase 3: Memory Management Integration

- Integrate backfill blocks into eviction algorithm
- Implement synchronized eviction
- Add combined size calculations
- Update memory pressure handling

### Phase 4: API Enhancement

- Enhance `getBackfillMessages` to use cache
- Update access time tracking
- Add cache hit/miss metrics
- Implement consistency validation

### Phase 5: Testing and Optimization

- Comprehensive test suite
- Performance benchmarking
- Memory usage optimization
- Error handling refinement

This design ensures that backfill blocks maintain perfect synchronization with forward blocks while providing efficient caching and memory management for robotics data visualization scenarios.
