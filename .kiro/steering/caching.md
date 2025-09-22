# CachingIterableSource Algorithm Documentation

## Overview

The `CachingIterableSource` is a sophisticated caching layer that sits between the Lichtblick player and underlying data sources (like ROS bags, MCAP files, etc.). It implements an intelligent memory-based caching system that optimizes data access patterns for robotics visualization and playback scenarios.

## Core Architecture

```mermaid
graph TB
    subgraph "Lichtblick Player"
        Player[IterablePlayer]
    end

    subgraph "Caching Layer"
        Cache[CachingIterableSource]
        Blocks[Cache Blocks]
        Eviction[Eviction Algorithm]
    end

    subgraph "Data Sources"
        ROS[ROS Bag Source]
        MCAP[MCAP Source]
        Other[Other Sources]
    end

    Player --> Cache
    Cache --> Blocks
    Cache --> Eviction
    Cache --> ROS
    Cache --> MCAP
    Cache --> Other

    style Cache fill:#e1f5fe
    style Blocks fill:#f3e5f5
    style Eviction fill:#fff3e0
```

## Cache Block Structure

Each cache block represents a continuous time range of messages and contains:

```mermaid
classDiagram
    class CacheBlock {
        +bigint id
        +Time start
        +Time end
        +Array~Tuple~ items
        +number lastAccess
        +number size
    }

    class Tuple {
        +bigint timestamp
        +IteratorResult result
    }

    CacheBlock --> Tuple : contains multiple

    note for CacheBlock "start: inclusive start time\nend: inclusive end time\nitems: sorted by timestamp\nlastAccess: for LRU eviction\nsize: total bytes"
```

## Message Iterator Algorithm

The core message iteration follows this sophisticated flow:

```mermaid
flowchart TD
    Start([messageIterator called]) --> Init{Initialize?}
    Init -->|No| Error[Throw Error]
    Init -->|Yes| TopicCheck{Topics changed?}

    TopicCheck -->|Yes| ClearCache[Clear entire cache]
    TopicCheck -->|No| SetReadHead[Set read head to start]
    ClearCache --> SetReadHead

    SetReadHead --> ComputeEvictable[Compute evictable blocks]
    ComputeEvictable --> MainLoop{readHead <= maxEnd?}

    MainLoop -->|No| Done([End iteration])
    MainLoop -->|Yes| FindBlock[Find cache block containing readHead]

    FindBlock --> BlockExists{Block found?}

    BlockExists -->|Yes| CheckEmpty{Block empty?}
    CheckEmpty -->|Yes| RemoveBlock[Remove empty block]
    RemoveBlock --> FindBlock

    CheckEmpty -->|No| ReadFromCache[Read messages from cache block]
    ReadFromCache --> UpdateAccess[Update block.lastAccess]
    UpdateAccess --> YieldCached[Yield cached messages]
    YieldCached --> AdvanceReadHead1[readHead = block.end + 1ns]
    AdvanceReadHead1 --> MainLoop

    BlockExists -->|No| ReadFromSource[Read from underlying source]
    ReadFromSource --> CreateBlock[Create new cache block]
    CreateBlock --> ProcessMessages[Process source messages]
    ProcessMessages --> CheckBlockSize{Block too big?}

    CheckBlockSize -->|Yes| CloseBlock[Close current block]
    CloseBlock --> AdvanceReadHead2[Advance read head]
    AdvanceReadHead2 --> ProcessMessages

    CheckBlockSize -->|No| CheckMemory{Memory limit?}
    CheckMemory -->|Yes| EvictBlocks[Evict old blocks]
    EvictBlocks --> YieldMessage[Yield message]
    CheckMemory -->|No| YieldMessage
    YieldMessage --> ProcessMessages

    ProcessMessages --> SourceDone{Source exhausted?}
    SourceDone -->|Yes| FinalizeBlock[Finalize block]
    FinalizeBlock --> AdvanceReadHead3[readHead = sourceEnd + 1ns]
    AdvanceReadHead3 --> MainLoop
    SourceDone -->|No| ProcessMessages

    style ReadFromCache fill:#c8e6c9
    style ReadFromSource fill:#ffcdd2
    style EvictBlocks fill:#fff3e0
```

## Cache Block Management

### Block Creation and Insertion

```mermaid
sequenceDiagram
    participant Iterator as Message Iterator
    participant Cache as CachingIterableSource
    participant Source as Underlying Source
    participant Block as Cache Block

    Iterator->>Cache: Request messages for time range
    Cache->>Cache: Check for existing block

    alt No existing block
        Cache->>Source: Request messages from source
        Source-->>Cache: Stream of messages
        Cache->>Block: Create new block
        Cache->>Cache: Find insertion point (sorted by start time)
        Cache->>Cache: Insert block at correct position

        loop For each message
            Cache->>Block: Add message to block
            Cache->>Cache: Update block size
            Cache->>Cache: Check if block exceeds maxBlockSize

            alt Block too large
                Cache->>Block: Finalize current block
                Cache->>Block: Create new block for remaining messages
            end
        end

        Cache->>Cache: Update loaded ranges
        Cache-->>Iterator: Yield messages
    else Block exists
        Cache->>Block: Update lastAccess timestamp
        Cache-->>Iterator: Yield cached messages
    end
```

### Time-based Block Boundaries

The algorithm carefully manages block boundaries based on message timestamps:

```mermaid
timeline
    title Cache Block Time Management

    section Block Creation
        Message at T1 : Block starts at T1
        Messages T1-T5 : Block accumulates messages
        Message at T6 : Block end set to T5 (last message time)

    section Block Finalization
        No more messages : Block end = requested end time
        Size limit hit : Close block, start new one at T6+1ns

    section Reading
        Request at T3 : Find block containing T3
        Read from T3 : Start from first message >= T3
        Block boundary : Next read starts at block.end + 1ns
```

## Memory Management and Eviction

### Eviction Algorithm

The cache uses a sophisticated eviction strategy that considers both temporal locality and memory pressure:

```mermaid
flowchart TD
    MemoryCheck{Total size > limit?} -->|No| Continue[Continue caching]
    MemoryCheck -->|Yes| FindEvictable[Find evictable blocks]

    FindEvictable --> SortBlocks[Sort blocks by end time]
    SortBlocks --> FindReadHead[Find block containing read head]

    FindReadHead --> ReadHeadFound{Read head block found?}
    ReadHeadFound -->|No| EvictOldest[Evict oldest block by lastAccess]
    ReadHeadFound -->|Yes| IdentifyEvictable[Identify evictable candidates]

    IdentifyEvictable --> BeforeReadHead[Blocks ending before read head]
    IdentifyEvictable --> AfterGap[Blocks after first gap in chain]

    BeforeReadHead --> EvictList[Add to eviction candidates]
    AfterGap --> EvictList
    EvictList --> EvictFirst[Evict first candidate]

    EvictFirst --> UpdateSize[Update total cache size]
    UpdateSize --> CheckAgain{Still over limit?}
    CheckAgain -->|Yes| EvictNext[Evict next candidate]
    CheckAgain -->|No| Continue
    EvictNext --> UpdateSize

    style EvictOldest fill:#ffcdd2
    style EvictFirst fill:#ffcdd2
    style Continue fill:#c8e6c9
```

### Evictable Block Identification

```mermaid
graph TD
    subgraph "Cache Blocks (sorted by time)"
        B1[Block 1<br/>T0-T10]
        B2[Block 2<br/>T11-T20]
        B3[Block 3<br/>T21-T30]
        B4[Block 4<br/>T35-T45]
        B5[Block 5<br/>T46-T55]
    end

    ReadHead[Read Head: T25]

    B1 -.->|Before read head| Evictable1[Evictable]
    B2 -.->|Before read head| Evictable2[Evictable]
    B3 -->|Contains read head| Protected[Protected Chain Start]
    B4 -.->|Gap after B4| Evictable3[Evictable]
    B5 -.->|After gap| Evictable4[Evictable]

    Protected --> Continuous[Continuous from read head]

    style Evictable1 fill:#ffcdd2
    style Evictable2 fill:#ffcdd2
    style Evictable3 fill:#ffcdd2
    style Evictable4 fill:#ffcdd2
    style Protected fill:#c8e6c9
    style Continuous fill:#c8e6c9
```

## Backfill Algorithm

The backfill mechanism retrieves the most recent message per topic before a given timestamp:

```mermaid
flowchart TD
    BackfillStart([getBackfillMessages called]) --> FindBlock[Find block containing target time]
    FindBlock --> BlockFound{Block found?}

    BlockFound -->|No| FallbackSource[Use source backfill]
    BlockFound -->|Yes| FindIndex[Find message index for target time]

    FindIndex --> ExactMatch{Exact timestamp match?}
    ExactMatch -->|Yes| FindLastMatch[Find last message with same timestamp]
    ExactMatch -->|No| FindPrevious[Find previous message index]

    FindLastMatch --> ReadBackwards[Read backwards from index]
    FindPrevious --> ReadBackwards

    ReadBackwards --> CheckTopic{Topic needed?}
    CheckTopic -->|Yes| AddToResults[Add to results]
    CheckTopic -->|No| Continue[Continue backwards]

    AddToResults --> RemoveTopic[Remove topic from needed list]
    RemoveTopic --> Continue
    Continue --> MoreTopics{More topics needed?}

    MoreTopics -->|No| SortResults[Sort results by time]
    MoreTopics -->|Yes| PrevBlock[Check previous block]

    PrevBlock --> GapCheck{Gap between blocks?}
    GapCheck -->|Yes| FallbackRemaining[Fallback to source for remaining]
    GapCheck -->|No| ReadBackwards

    FallbackRemaining --> MergeResults[Merge and sort all results]
    SortResults --> Return[Return results]
    MergeResults --> Return
    FallbackSource --> Return

    style AddToResults fill:#c8e6c9
    style FallbackSource fill:#ffcdd2
    style FallbackRemaining fill:#ffcdd2
```

## Cache Coherency and Loaded Ranges

The system maintains accurate loaded range information for UI progress indicators:

```mermaid
sequenceDiagram
    participant UI as User Interface
    participant Cache as CachingIterableSource
    participant Ranges as Loaded Ranges

    UI->>Cache: Request loadedRanges()
    Cache->>Ranges: Compute normalized ranges

    Note over Ranges: Ranges are normalized to [0,1]<br/>based on source start/end times

    loop For each cache block
        Ranges->>Ranges: Convert block time to normalized range
        Ranges->>Ranges: Merge continuous blocks
    end

    Note over Ranges: Continuous blocks are merged<br/>(block.end + 1ns = next.start)

    Ranges-->>Cache: Return merged ranges
    Cache-->>UI: Provide range array

    Cache->>Cache: Emit loadedRangesChange event
    UI->>UI: Update progress visualization
```

## Performance Optimizations

### Binary Search for Message Lookup

```mermaid
graph TD
    subgraph "Cache Block Items Array"
        I0["[T0, msg0]"]
        I1["[T1, msg1]"]
        I2["[T2, msg2]"]
        I3["[T3, msg3]"]
        I4["[T4, msg4]"]
        I5["[T5, msg5]"]
    end

    SearchKey[Search for T2.5]

    SearchKey --> BinarySearch[Binary search in sorted array]
    BinarySearch --> Found{Exact match?}

    Found -->|Yes| LinearScan[Linear scan for first occurrence]
    Found -->|No| InsertionPoint[Return insertion point]

    LinearScan --> FirstMatch[Return index of first match]
    InsertionPoint --> NextHigher[Return index of next higher element]

    style BinarySearch fill:#e3f2fd
    style LinearScan fill:#f3e5f5
```

### Consecutive Block Access Optimization

```mermaid
flowchart LR
    subgraph "Common Case Optimization"
        Request[Request messages] --> FirstCheck{First item >= key?}
        FirstCheck -->|Yes| ReturnZero[Return index 0]
        FirstCheck -->|No| BinarySearch[Perform binary search]
    end

    subgraph "Rationale"
        Playback[Sequential playback] --> Consecutive[Consecutive block access]
        Consecutive --> FirstItem[Usually need first item in block]
    end

    style ReturnZero fill:#c8e6c9
    style BinarySearch fill:#fff3e0
```

## Configuration Parameters

The caching behavior is controlled by several key parameters:

```mermaid
graph LR
    subgraph "Configuration"
        MaxTotal[maxTotalSize<br/>Default: 600MB]
        MaxBlock[maxBlockSize<br/>Default: 50MB]
    end

    subgraph "Behavior Impact"
        MaxTotal --> MemoryLimit[Controls when eviction starts]
        MaxBlock --> BlockSplit[Controls when blocks are split]

        MemoryLimit --> EvictionFreq[Eviction frequency]
        BlockSplit --> CacheGranularity[Cache granularity]
    end

    subgraph "Trade-offs"
        EvictionFreq --> MemoryUsage[Memory usage vs cache hits]
        CacheGranularity --> SeekPerformance[Seek performance vs memory efficiency]
    end

    style MaxTotal fill:#e1f5fe
    style MaxBlock fill:#e1f5fe
```

## Error Handling and Edge Cases

### Topic Change Handling

```mermaid
sequenceDiagram
    participant Player as IterablePlayer
    participant Cache as CachingIterableSource
    participant Source as Data Source

    Player->>Cache: messageIterator(topics: [A, B])
    Cache->>Cache: Cache messages for topics A, B

    Note over Cache: Cache contains blocks for topics A, B

    Player->>Cache: messageIterator(topics: [A, B, C])
    Cache->>Cache: Compare new topics with cached topics
    Cache->>Cache: Topics changed - clear entire cache

    Note over Cache: Heavy-handed but avoids complexity<br/>of managing disjoint topic ranges

    Cache->>Source: Request fresh data for topics A, B, C
    Source-->>Cache: Stream new messages
    Cache->>Cache: Build new cache blocks
```

### Memory Pressure Scenarios

```mermaid
stateDiagram-v2
    [*] --> Normal: Cache size < limit
    Normal --> Pressure: Adding message would exceed limit
    Pressure --> FindEvictable: Look for evictable blocks

    FindEvictable --> HasEvictable: Found evictable blocks
    FindEvictable --> NoEvictable: No evictable blocks

    HasEvictable --> Evict: Remove oldest evictable block
    Evict --> CheckSize: Check if under limit
    CheckSize --> Normal: Size OK
    CheckSize --> Evict: Still over limit

    NoEvictable --> BlockReading: Cannot read more messages
    BlockReading --> [*]: Return canReadMore() = false

    state Pressure {
        [*] --> CalculateEvictable
        CalculateEvictable --> BeforeReadHead: Blocks ending before read head
        CalculateEvictable --> AfterGaps: Blocks after gaps in chain
        BeforeReadHead --> [*]
        AfterGaps --> [*]
    }
```

## Integration with Lichtblick Player

The caching source integrates seamlessly with the broader Lichtblick architecture:

```mermaid
graph TB
    subgraph "UI Layer"
        Timeline[Timeline Component]
        Charts[Chart Components]
        3D[3D Visualization]
    end

    subgraph "Player Layer"
        Player[IterablePlayer]
        State[Player State Management]
    end

    subgraph "Caching Layer"
        Cache[CachingIterableSource]
        Blocks[Cache Blocks]
        Eviction[Eviction Logic]
    end

    subgraph "Data Layer"
        ROS[ROS Bag Reader]
        MCAP[MCAP Reader]
        Remote[Remote Data Sources]
    end

    Timeline --> Player
    Charts --> Player
    3D --> Player

    Player --> State
    Player --> Cache
    State --> Cache

    Cache --> Blocks
    Cache --> Eviction
    Cache --> ROS
    Cache --> MCAP
    Cache --> Remote

    Cache -.->|loadedRangesChange| Timeline

    style Cache fill:#e1f5fe
    style Player fill:#f3e5f5
    style Timeline fill:#e8f5e8
```

## Performance Characteristics

### Time Complexity

| Operation                   | Best Case | Average Case | Worst Case |
| --------------------------- | --------- | ------------ | ---------- |
| Message Iterator (cached)   | O(1)      | O(log n)     | O(n)       |
| Message Iterator (uncached) | O(m)      | O(m + log n) | O(m + n)   |
| Backfill (cached)           | O(log n)  | O(log n + k) | O(n)       |
| Block Eviction              | O(1)      | O(b)         | O(b)       |
| Cache Lookup                | O(1)      | O(log b)     | O(b)       |

Where:

- n = messages per block
- m = messages to read from source
- k = number of topics for backfill
- b = number of cache blocks

### Space Complexity

The cache uses O(M) space where M is the configured maximum cache size. The actual memory usage includes:

- Message data: Variable based on message sizes
- Block metadata: O(b) where b is number of blocks
- Index structures: O(n) where n is total cached messages
- Loaded ranges: O(b) for range computation

This sophisticated caching algorithm provides excellent performance for typical robotics data access patterns while maintaining bounded memory usage and providing mechanisms for cache coherency and eviction under memory pressure.
