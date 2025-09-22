# Tests to Create for 100% Coverage - CachingIterableSource

## 📋 **Remaining Uncovered Lines Analysis**

### **Lines 184-186: Empty Block Removal Logic**

```typescript
if (block && compare(block.start, block.end) === 0 && block.items.length === 0) {
  block = undefined; // Line 184 - UNCOVERED
  this.#cache.splice(cacheBlockIndex, 1); // Line 185 - UNCOVERED
  continue; // Line 186 - UNCOVERED
}
```

**Strategy**: Create a cache block with `start === end` and `items.length === 0`
**Test Name**: `should remove empty cache blocks with start === end`

### **Line 200: Cached Item Null Check**

```typescript
const cachedItem = block.items[idx];
if (!cachedItem) {
  // Line 200 - UNCOVERED
  break;
}
```

**Strategy**: Manipulate cache block items array to have undefined/null entries
**Test Name**: `should handle null cached items in block iteration`

### **Line 239: Source Read Invariant Error**

```typescript
if (compare(sourceReadStart, sourceReadEnd) > 0) {
  throw new Error("Invariant: sourceReadStart > sourceReadEnd"); // Line 239 - UNCOVERED
}
```

**Strategy**: Create block arrangement where sourceReadStart > sourceReadEnd
**Test Name**: `should throw invariant error when sourceReadStart > sourceReadEnd`

### **Line 245: Source Read End Adjustment**

```typescript
if (compare(sourceReadEnd, maxEnd) > 0) {
  sourceReadEnd = maxEnd; // Line 245 - UNCOVERED
}
```

**Strategy**: Create scenario where nextBlock.start > maxEnd
**Test Name**: `should adjust sourceReadEnd when it exceeds maxEnd`

### **Line 425: Cache Block Null Check in Backfill**

```typescript
const cacheBlock = this.#cache[idx];
if (!cacheBlock) {
  // Line 425 - UNCOVERED
  break;
}
```

**Strategy**: Manipulate cache array to have undefined entries during backfill
**Test Name**: `should handle null cache blocks during backfill iteration`

### **Line 469: Gap Detection in Backfill**

```typescript
if (prevBlock && compare(add(prevBlock.end, { sec: 0, nsec: 1 }), cacheBlock.start) !== 0) {
  break; // Line 469 - UNCOVERED
}
```

**Strategy**: Create cache blocks with gaps between them during backfill
**Test Name**: `should detect gaps between cache blocks in backfill`

### **Line 492: Recompute Loaded Range Invariant**

```typescript
if (!this.#initResult) {
  throw new Error("Invariant: uninitialized"); // Line 492 - UNCOVERED
}
```

**Strategy**: Call recomputeLoadedRangeCache before initialization
**Test Name**: `should throw invariant error in recomputeLoadedRangeCache when uninitialized`

### **Line 601: Failed Oldest Block Retrieval**

```typescript
if (!oldestBlock) {
  throw new Error("Failed to retrieve oldest block from cache"); // Line 601 - UNCOVERED
}
```

**Strategy**: Create scenario where minIndexBy returns invalid index
**Test Name**: `should throw error when oldest block retrieval fails`

### **Line 656: Active Block Eviction Error**

```typescript
if (block === activeBlock) {
  throw new Error("Cannot evict the active cache block."); // Line 656 - UNCOVERED
}
```

**Strategy**: Force eviction algorithm to try evicting the active block
**Test Name**: `should throw error when trying to evict active block`

### **Line 663: Eviction Return False**

```typescript
return false; // Line 663 - UNCOVERED
```

**Strategy**: Create scenario where block eviction fails
**Test Name**: `should return false when block eviction fails`

### **Lines 682-690: Linear Scan in FindStartCacheItemIndex**

```typescript
for (let i = idx - 1; i >= 0; --i) {
  // Line 682 - UNCOVERED
  const prevItem = items[i]; // Line 683 - UNCOVERED
  if (prevItem != undefined && prevItem[0] !== key) {
    // Line 684 - UNCOVERED
    break; // Line 685 - UNCOVERED
  }
  idx = i; // Line 687 - UNCOVERED
}
return idx; // Line 690 - UNCOVERED
```

**Strategy**: Create exact timestamp matches requiring backward linear scan
**Test Name**: `should perform linear scan for duplicate timestamps in FindStartCacheItemIndex`

## 🎯 **Advanced Testing Strategies**

1. **Internal State Manipulation**: Use TypeScript casting to access private members
2. **Precise Cache Block Creation**: Create specific block arrangements
3. **Memory Pressure Simulation**: Use exact size constraints
4. **Error Injection**: Mock dependencies to force error conditions
5. **Race Condition Simulation**: Create specific operation sequences
6. **Data Structure Manipulation**: Directly modify cache arrays
7. **Timing Control**: Use controlled message sequences

## 📝 **Implementation Plan**

1. Create helper functions to manipulate private cache state
2. Use reflection techniques to access private methods
3. Create mock sources with precise control over message timing
4. Use extreme memory constraints to force specific eviction paths
5. Create artificial cache block arrangements
6. Use error injection in dependencies
7. Create specific data patterns to trigger edge cases
