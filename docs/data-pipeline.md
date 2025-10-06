# Lichtblick Data Pipeline and Panel Integration

This guide provides a detailed overview of how Lichtblick loads, caches, and
distributes data. It follows the path of a message from its original source,
through multiple layers of buffering, and finally into a panel. The last
section compares the two mechanisms available for panels to connect to the
pipeline: the modern `PanelAPI` and the compatibility-focused
`PanelExtensionAdapter`.

## Data loading, caching, and buffering

### Source types and interfaces

Lichtblick can read from a variety of sources—MCAP files on disk, ROS bag
files, remote servers, or live streams. Every source implements the
`IIterableSource` interface so that the player can treat them uniformly. Two
subinterfaces exist:

- **`ISerializedIterableSource`** – Yields serialized message data. The MCAP
  reader is an example of this variant. Messages are read as byte ranges and
  decoded later.
- **`IDeserializedIterableSource`** – Provides fully decoded `MessageEvent`
  objects, often produced by a live connection.

Because the player only depends on `IIterableSource`, new source types can be
added without touching the rest of the pipeline.

### Player state machines

Different players implement a common set of lifecycles. File playback relies on
`IterablePlayer`, while live data streams use WebSocket-based players such as
`FoxgloveWebSocketPlayer` or `RosbridgePlayer`.

#### File playback (IterablePlayer)

`IterablePlayer` orchestrates recorded sources like MCAP or ROS bag files. It
behaves as a state machine with the following phases:

1. **Uninitialized** – The player has been constructed but the source is not
   yet open.
2. **Opening** – Metadata such as available topics, datatypes, and the global
   start and end time are read from the source.
3. **Ready** – Initialization is complete and the first `PlayerState` can be
   emitted to the message pipeline.
4. **Playing / Paused** – On each animation frame the player calls `tick()` to
   pull messages from its source. `tick()` advances time based on the current
   playback speed and returns a new `PlayerState` snapshot.
5. **Seeking** – When a user jumps to a new time, buffered data is discarded,
   the source is asked to seek, and `lastSeekTime` is updated so panels can
   reset their own state.
6. **Closed / Error** – Terminal states once playback finishes or a fatal error
   occurs.

#### WebSocket playback

WebSocket players maintain a connection-oriented state machine:

1. **Disconnected** – No socket is open. A user provides a URL to begin.
2. **Connecting** – The player opens a WebSocket and negotiates the protocol.
3. **Subscribing** – After the socket opens, the player fetches topic and
   datatype information and sends subscription requests.
4. **Streaming** – The connection is active. Messages are pushed from the
   network and batched into periodic `PlayerState` updates.
5. **Reconnecting** – If the socket closes unexpectedly, the player transitions
   here and attempts to re-establish the connection, updating `presence` to
   `RECONNECTING`.
6. **Closed / Error** – The session ends or cannot be recovered, and an error is
   reported.

### BufferedIterableSource and read‑ahead

Reading from disk or across a network can be slow. To avoid stuttering,
`IterablePlayer` wraps the `IIterableSource` in a `BufferedIterableSource`.
This helper behaves like a small in-memory message broker:

- A producer coroutine streams data from the underlying source into a
  `VecQueue`. The consumer (`IterablePlayer.tick`) dequeues results as fast as
  they are needed.
- The producer tries to stay `readAheadDuration` (10 s by default) in front of
  the consumer's read head. Playback pauses until at least
  `minReadAheadDuration` (1 s) of data has been buffered to prevent immediate
  underruns.
- `CachingIterableSource` underneath bounds the queue by total message size and
  drops messages that fall behind the read head, keeping memory usage bounded.
- Seeking clears the queue, aborts the producer, and restarts buffering from
  the new time so panels never see stale messages.

Because messages are ready in RAM before `tick()` asks for them, playback stays
responsive even when the underlying storage has inconsistent latency.

### Preloading with BlockLoader

Some panels require access to the entire history of a topic. When a panel
subscribes with `preload: true`, the message pipeline enables the `BlockLoader`.

`BlockLoader` divides the total time span of the data source into at most 100
equal-duration blocks. For each block it tracks which topics have been loaded.
Each `MessageBlock` covers a fixed nanosecond span and contains a
`messagesByTopic` map plus metadata such as `sizeInBytes` and a list of topics
still needed (`needTopics`). Loading proceeds as follows:

1. The loader groups contiguous block ranges that still need the same topics.
2. It requests all messages for that time span from the source via
   `getBackfillMessages` to minimize seeks.
3. Messages are inserted into the corresponding block's `messagesByTopic` array
   and the block's byte count is updated.

The block cache has a global `cacheSizeBytes` limit. When new messages would
exceed this budget, the loader runs `removeUnusedBlockTopics` which frees data
for topics that were unsubscribed or will be reloaded with different options.
If the cache is still full, preloading is halted and the player reports a
`PlayerProblem`. There is **no** LRU eviction: once a block's data is loaded it
remains until explicitly cleared, ensuring deterministic access for panels such
as Plot or State Transition.

Progress reporting is derived from how many blocks have all requested topics
filled. The message pipeline surfaces this in `PlayerState.progress` so panels
can render preload status bars.

### File and byte level caching

Several utilities reduce repeated I/O:

- **`VirtualLRUBuffer`** – Represents a large byte array split into fixed-size
  blocks. Blocks are kept in a least-recently-used map with a maximum capacity.
  Requesting a new block evicts the oldest one.
- **`CachedFilelike`** – Wraps a random-access file or network resource. Reads
  go through a `VirtualLRUBuffer`, and sequential reads trigger prefetching of
  upcoming blocks. MCAP file playback uses this class heavily.
- **Remote fetch helpers** – When data is streamed over HTTP, byte-range
  requests are cached to avoid re-downloading previously fetched portions.

### Message pipeline cache

Even with preloading, many panels only need the most recent message on a topic.
The message pipeline therefore keeps a per-topic cache of the last
`MessageEvent`. When a new panel subscribes, the cached message is delivered
immediately so that the panel can render without waiting for the next tick.

### End-to-end data flow

1. The user selects a data source. `DataSourceFactory` creates an
   `IIterableSource` and an `IterablePlayer` to manage it.
2. The player opens the source, reads metadata, and transitions to `Ready`.
3. The source is wrapped in a `BufferedIterableSource` for read‑ahead.
4. Panels declare subscriptions. The pipeline merges them into a single
   `SubscribePayload` and forwards it to the player. When at least one
   subscription includes `preload: true`, the player spins up a dedicated
   `BlockLoader` task. That task keeps polling the consolidated subscription set
   for topics that still need historical data, schedules block fetches via
   `getBackfillMessages`, and streams those results back into the cache while
   respecting the global memory budget.
5. During playback, `tick()` pulls messages from the buffer, updates
   `currentTime`, and emits a new `PlayerState`.
6. `MessagePipelineProvider` receives the state, updates its store, and
   notifies panels.
7. Panels render new data or publish outgoing messages back through the player.

## Message pipeline

### Purpose

The message pipeline bridges the player and React panels. It manages
subscriptions, publishers, and distribution of the `PlayerState` snapshot. A
provider component exposes this functionality via React hooks.

### MessagePipelineProvider and store

`MessagePipelineProvider` lives high in the React tree. Internally it uses a
`zustand` store containing:

- The current `PlayerState`
- Sets of active subscriptions and registered publishers
- A map of last messages per topic
- Helper functions for updating these structures

Whenever the player emits a new `PlayerState`, the provider updates the store
and triggers React re-rendering of any subscribed components.

### Subscriptions

Panels express their data needs by calling `subscribe` (via hooks or the adapter
context). A subscription lists topics and options such as `preload`. The
provider merges all panel subscriptions into a single set and forwards it to the
player. When subscriptions change, the player can stop loading unused topics,
saving bandwidth and memory.

### PlayerState in depth

`PlayerState` is the payload sent from the player to the pipeline. Important
fields include:

- **`presence`** – Enum describing the player's lifecycle (e.g.,
  `INITIALIZING`, `PRESENT`, `RECONNECTING`, `BUFFERING`, `ERROR`). UI components
  use it to show loading spinners or error messages.
- **`progress`** – Information about how much of the time range has been
  buffered. Panels can render progress bars based on this data.
- **`capabilities`** – Strings indicating which features the player supports,
  such as `playbackControl` or `publish`.
- **`activeData`** – Present only when a source is ready and includes:
  - `messages` – The batch of `MessageEvent`s read during the most recent
    `tick()`.
  - `currentTime` – Playback time at the end of that tick.
  - `startTime` / `endTime` – Bounds of the entire data set.
  - `isPlaying` and `speed` – Current playback controls.
  - `lastSeekTime` – Monotonically increasing value set whenever the user seeks.
    Panels should clear cached data when this changes.
  - `topics` – Complete list of topics.
  - `datatypes` – Map of datatype definitions.

### Publishing

Some players support publishing messages back to the data source. Panels declare
their publishers, and the message pipeline exposes a `publish` function that
forwards messages to the active player. The provider tracks which publishers are
registered so a panel cannot publish to an unsupported topic.

### Message flow to panels

1. A new `PlayerState` arrives at the provider.
2. The provider stores the most recent message for each topic and updates the
   store's time and playback fields.
3. Panels using `PanelAPI` hooks or the adapter context receive updates and
   render.

## Panel integration

Panels render data for users. They can integrate with the message pipeline in
two ways.

### Direct integration with PanelAPI

The modern approach is to build panels as React components and use hooks from
`@lichtblick/suite-base`:

- `useDataSourceInfo` returns static metadata such as topic and datatype lists.
- `useMessageReducer` lets panels define internal state and a reducer that
  processes incoming `MessageEvent` arrays.
- `useMessagesByTopic` builds on `useMessageReducer` to expose the last *N*
  messages for a set of topics.
- `usePublish` registers publishers so the panel can send messages back through
  the player.

Panels built with hooks benefit from React's declarative model. Components
re-render automatically when their subscribed data changes, and developer tools
work out of the box.

### Using the PanelExtensionAdapter

Some panels were originally written for Foxglove Studio's extension API. The
`PanelExtensionAdapter` allows these panels to run without a full rewrite.

Panels using this adapter export an `initPanel` function. The adapter:

1. Creates a DOM element and calls `initPanel` with that element and a `context`
   object.
2. The `context` exposes methods like `subscribe`, `unsubscribe`, `setPublishers`,
   `publish`, and a way to register an `onRender` callback.
3. When the pipeline has new data, it calls the registered `onRender` with a
   `RenderState` that mirrors `PlayerState`. The panel is responsible for
   drawing into the DOM element and must call the provided `done` callback once
   rendering is complete.
4. When the panel is removed, the adapter invokes the panel's `onUnmount`
   handler to allow cleanup.

This style is imperative and places responsibility for DOM updates on the panel
itself, but it enables reuse of existing extension panels.

### Comparison matrix

| Feature | PanelAPI (Direct) | PanelExtensionAdapter |
| --- | --- | --- |
| **API style** | React hooks and declarative rendering | Imperative callbacks and manual DOM manipulation |
| **Lifecycle management** | Controlled by React mount/unmount and re-rendering | Adapter calls `initPanel`/`onUnmount` |
| **State management** | Any React state tool (`useState`, `useReducer`, Zustand, etc.) | Panel keeps its own state and receives `RenderState` snapshots |
| **Data access** | Panel pulls data via hooks like `useMessagesByTopic` | Adapter pushes data via `onRender` |
| **Publishing** | `usePublish` registers publishers and sends messages | `context.publish` after calling `setPublishers` |
| **Configuration** | Props or React context | Adapter provides a frozen config to `initPanel` |
| **Rendering target** | Component returns JSX | Panel renders into a raw DOM element |
| **Hot reloading** | Works with React Fast Refresh | Typically requires full reload |
| **Extension packaging** | Bundled with the application or as npm packages | Often shipped as standalone JS bundles |
| **Typical use** | New panels developed within this repo | Porting existing Foxglove Studio panels or third-party extensions |

### Choosing an approach

Use **PanelAPI** whenever you control the panel code and can depend on React. It
offers better integration with the rest of the application and the broader
React ecosystem.

Choose **PanelExtensionAdapter** only when you need to run a legacy panel or a
third-party extension that expects the legacy API. These panels trade
ergonomics for compatibility.

## Summary

Lichtblick's data pipeline combines multiple layers of caching and buffering to
provide smooth playback from a variety of sources. The message pipeline
distributes `PlayerState` snapshots and keeps per-topic caches so that panels
receive data efficiently. When building panels, prefer the React-based
`PanelAPI` but leverage the `PanelExtensionAdapter` to run legacy or external
panels without modification.

