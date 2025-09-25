# Lichtblick Data Pipeline and Panel Integration

This document provides a detailed explanation of how data is loaded, cached, and processed through the message pipeline in Lichtblick. It also details the two primary ways that panels can be integrated into the system: by using the `PanelAPI` directly or by using the `PanelExtensionAdapter`.

## Data Loading, Caching, and Buffering

The data loading architecture in Lichtblick is designed to be flexible and performant, handling various data sources and providing a smooth user experience. The core components of this system are the `IterablePlayer` and its associated sources.

### Core Components

*   **`IterablePlayer`**: This is the heart of the data loading and playback system. It is implemented as a state machine that manages the entire lifecycle of a data source, including initialization, playback, seeking, and closing. It receives an `IIterableSource` and is responsible for reading messages from it and feeding them into the message pipeline.

*   **`IIterableSource`**: This is an interface that abstracts the underlying data source. There are two main implementations:
    *   `ISerializedIterableSource`: For sources that provide serialized message data (e.g., raw MCAP or ROS bag files).
    *   `IDeserializedIterableSource`: For sources that provide deserialized message data.

    This abstraction allows the `IterablePlayer` to be agnostic to the specific format of the data source.

*   **`BufferedIterableSource`**: To ensure smooth playback, the `IterablePlayer` wraps its source in a `BufferedIterableSource`. This component reads data from the underlying source ahead of the current playback time and stores it in a buffer. This helps to mitigate an I/O stalls and ensures that data is available when the player needs it.

*   **`BlockLoader`**: For panels that require access to all messages on a topic for all time (e.g., the Plot panel), the `BlockLoader` provides a mechanism for preloading data. When a panel subscribes to a topic with the `preload: true` option, the `BlockLoader` will attempt to load all messages for that topic into a memory cache.

    *   **What are "blocks"?** The `BlockLoader` divides the entire time range of the data source into a fixed number of "blocks" of equal time duration (currently capped at 100 blocks). Each block is a container for all the messages that fall within its specific time range, organized by topic. This partitioning allows for more granular loading and management of the data.

    *   **Caching Algorithm**: The `BlockLoader`'s goal is to fill these blocks with message data. It works by identifying contiguous ranges of blocks that need the same topics and fetching the data for that entire time range at once. The cache has a hard size limit. If adding new messages would exceed this limit, the loader first attempts to free up space by removing messages for topics that are no longer subscribed to. If the cache is still full, preloading stops, and an error is reported. There is no automatic eviction of "old" blocks based on usage (like an LRU strategy); it simply fills up and then stops.

### Parallel Loading: Full Preload vs. Forward Fill

The data loading system is designed to run two different loading strategies in parallel to optimize the user experience for different types of panels. This is controlled by the `preloadType` property on a panel's topic subscription.

*   **Forward Fill (Partial Preload):** This is the **default** strategy, used by most panels that only need to display the latest data (e.g., the Raw Messages or 3D panel). It is handled by the `BufferedIterableSource`. This component reads a small amount of data ahead of the current playback time (a "forward fill") to ensure that data is always ready for the next frame, providing smooth playback. It does *not* attempt to load the entire history for a topic.

*   **Full Preload:** This strategy is used by panels that need access to the entire time series of a topic at once, such as the Plot panel. When a panel subscribes with `preloadType: "full"`, it signals to the `IterablePlayer` that it needs the complete history. This task is handled by the `BlockLoader`, which runs in the background to download and cache all the messages for that topic into its block cache.

These two mechanisms run concurrently. The `BufferedIterableSource` ensures that live playback is always smooth, while the `BlockLoader` works in the background to fill in the complete data history for panels that require it.

## The Player Abstraction

The `Player` is a crucial abstraction in Lichtblick. It is an interface that normalizes how the rest of the application interacts with a data source, regardless of whether that source is a static local file or a live data stream. The `MessagePipelineProvider` receives a `Player` instance and listens to the `PlayerState` objects it emits, which in turn drive all panel updates.

The `Player` interface defines a set of responsibilities, including `setListener`, `setSubscriptions`, `startPlayback`, and `seekPlayback`. Different data sources require different implementations of this interface. The two primary implementations are `IterablePlayer` for file-based sources and `FoxgloveWebSocketPlayer` for live data streams.

### File-based Playback: `IterablePlayer`

This is the primary implementation for all static, finite data sources like MCAP and ROS bag files.

*   **Core Concept:** It treats the data source as a finite, iterable collection of messages with a defined start and end time.
*   **State Management:** It operates as a state machine (with states like `initialize`, `play`, `seek-backfill`) to manage the playback lifecycle.
*   **Playback Control:** It manages a `currentTime` playback cursor and fully supports seeking, pausing, and variable playback speeds. The `tick()` method is called in a loop to "pull" chunks of data from the source based on the current time and playback speed.
*   **Capabilities:** A panel connected to an `IterablePlayer` will see capabilities like `playbackControl` and `setSpeed`.

### Live Data Playback: `FoxgloveWebSocketPlayer`

This implementation is for connecting to live data streams that follow the Foxglove websocket protocol.

*   **Core Concept:** It operates reactively, not iteratively. It listens for events from the websocket and "pushes" data into the pipeline as it arrives.
*   **State Management:** It does not have a concept of a finite timeline. The time range grows as new data arrives, and the `currentTime` is typically based on the wall clock or a time message from the server.
*   **Playback Control:** Seeking and speed control are not applicable and not supported. The player is always "playing" the latest data from the live stream.
*   **Capabilities:** Its capabilities reflect its real-time nature, often including `advertise` and `callServices` (for two-way communication with the server) but *not* `playbackControl`.

### End-to-End Data Flow

This section describes the entire journey of data, from a file on disk to a rendered visualization in a panel.

1.  **Initiation:** A user opens a data source (e.g., by dragging an MCAP file into the app).
2.  **Player Creation:** A `DataSourceFactory` identifies the file type and creates the corresponding `IIterableSource` (which knows how to read the file format) and an `IterablePlayer`.
3.  **Initialization:** The `IterablePlayer` initializes the source, reading its metadata (topics, time range, etc.). This information is passed to the `MessagePipelineProvider` and becomes available to all panels, which might use it to update their UI (e.g., populating a topic picker).
4.  **Subscription:** Panels declare their data needs by creating subscriptions (e.g., via `useMessageReducer` or `context.subscribe`). These subscriptions are aggregated by the `MessagePipelineProvider` and passed down to the `IterablePlayer`.
5.  **Buffering & Caching:**
    *   The `IterablePlayer`'s `BufferedIterableSource` starts reading ahead from the current time to ensure smooth playback.
    *   Simultaneously, the `BlockLoader` identifies any topics with `preload: true` subscriptions and begins loading all messages for those topics into its block cache.
6.  **Playback Tick:** During playback, the `IterablePlayer`'s `tick()` method runs on a loop. In each tick, it:
    *   Calculates the next time range to load based on playback speed.
    *   Reads messages within that time range from the `BufferedIterableSource`.
7.  **State Update:** The `IterablePlayer` packages these new messages, along with the current time and other status information, into a new `PlayerState` object.
8.  **Pipeline Distribution:** The `PlayerState` object is sent to the `MessagePipelineProvider`'s listener.
9.  **Store Update:** The `MessagePipelineProvider` updates its internal `zustand` store with the new `PlayerState`.
10. **Panel Re-render:** The `zustand` store update triggers a re-render in all panels that are subscribed to the pipeline's data.
11. **Data Consumption:**
    *   A panel using `useMessageReducer` will have its reducer function called with the new messages.
    *   A panel using `PanelExtensionAdapter` will have its `onRender` callback invoked with the new `RenderState`.
12. **UI Update:** The panel uses the new data to update its visualization on the screen.

## The Message Pipeline

The message pipeline is the central nervous system of Lichtblick's data flow. It is responsible for taking the data provided by the `IterablePlayer` and efficiently distributing it to all the panels in the current layout.

### Core Components

*   **`MessagePipelineProvider`**: This is a React component that sits high up in the component tree and uses a `zustand` store to manage the state of the pipeline. It receives the active `player` as a prop and listens for `PlayerState` updates.

*   **`PlayerState`**: This is the central object emitted by the `player` that contains all the information the UI needs to render the current state of the data source. It is the snapshot of the player at a given moment in time.

*   **Subscriptions**: Panels register their data requirements with the message pipeline by creating subscriptions. These subscriptions specify which topics a panel is interested in. The `MessagePipelineProvider` aggregates all subscriptions from all panels and passes them to the `player`, which can then optimize its data loading based on what is currently needed.

### The `PlayerState` Object in Detail

The `PlayerState` object is the primary data structure that flows from the player to the panels. Here are its key fields:

*   **`presence`**: An enum that describes the player's status (e.g., `INITIALIZING`, `PRESENT`, `BUFFERING`, `ERROR`). This is used to show appropriate UI feedback, like loading spinners.
*   **`progress`**: An object containing information about the data loading progress, including which time ranges of the source are fully loaded into the cache.
*   **`capabilities`**: An array of strings indicating the features supported by the current player (e.g., `playbackControl`, `setSpeed`).
*   **`activeData`**: This is the most critical field for panel rendering. It is `undefined` while the player is initializing but once present, it contains the core playback data:
    *   **`messages`**: An array of the most recent `MessageEvent` objects to be rendered.
    *   **`currentTime`**: The current time of the playback cursor.
    *   **`startTime`** and **`endTime`**: The start and end times of the entire data source.
    *   **`isPlaying`**: A boolean indicating if playback is currently active.
    *   **`speed`**: The current playback speed multiplier.
    *   **`lastSeekTime`**: A timestamp that is updated whenever there is a discontinuity in the data (e.g., after a user seeks to a new time). This is a signal for panels to clear their internal state and re-render from the new data.
    *   **`topics`**: A complete list of all topics available from the data source.
    *   **`datatypes`**: A map of all message definitions (e.g., ROS datatypes).

## Panel Integration

There are two primary methods for integrating a panel into Lichtblick, each with its own trade-offs.

### 1. Direct Integration with `PanelAPI`

This is the modern, recommended approach for building panels in Lichtblick. It involves using a set of React hooks provided by the `PanelAPI` to connect a panel directly to the message pipeline.

*   **`useMessageReducer`**: This is the most powerful hook in the `PanelAPI`. It allows a panel to define a state, an initializer for that state, and a reducer function that is called with new messages as they arrive. This gives the panel full control over how it processes and stores data.
*   **`useMessagesByTopic`**: A simpler hook that provides a panel with the last N messages on a set of topics. It's a convenient wrapper around `useMessageReducer` for panels with simple data needs.
*   **`useDataSourceInfo`**: Provides non-changing metadata about the data source, such as topic lists and datatypes.

This approach integrates seamlessly with React's declarative component model and is very flexible.

### 2. Using the `PanelExtensionAdapter`

The `PanelExtensionAdapter` is a compatibility layer designed to support a different, more imperative style of panel, likely inherited from the original Foxglove Studio.

*   **Imperative API**: Instead of using hooks, a panel using the adapter is initialized once with an `initPanel` function. This function receives a `context` object with methods for interacting with the system (e.g., `context.subscribe()`, `context.publish()`).
*   **`onRender` Callback**: The panel provides an `onRender` callback through the context. The adapter calls this function whenever new data is available. The panel receives a `RenderState` object with all the data it needs and is responsible for rendering itself into a provided DOM element. It must call a `done` callback when it has finished rendering.

This approach is less integrated with the React ecosystem and is primarily intended for maintaining compatibility with existing panels written in this style.

### Comparison Matrix

| Feature               | `PanelAPI` (Direct)                                     | `PanelExtensionAdapter`                                                              |
| --------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **API Style**         | Declarative (React hooks)                               | Imperative (callback-based)                                                          |
| **Lifecycle**         | Integrated with React component lifecycle               | Managed by the adapter (`initPanel`, `onUnmount`)                                    |
| **State Management**  | Managed by the panel using React state (e.g., `useState`, `useReducer`) | Can be managed internally by the panel, but state is passed in via `onRender`          |
| **Data Flow**         | Data is "pulled" into the component via hooks           | Data is "pushed" to the panel via the `onRender` callback                              |
| **React Integration** | High. Panels are standard React components.             | Low. Panels are given a raw DOM element to render into.                              |
| **Flexibility**       | High. Full access to the React ecosystem and hooks.     | Lower. Constrained to the API provided by the `context` object.                      |
| **Use Case**          | Recommended for all new panels.                         | Primarily for compatibility with existing panels from Foxglove Studio.                 |

### When to Use Which?

*   **Use the `PanelAPI` for all new panel development.** It is the native, modern, and most flexible way to build panels in Lichtblick. It allows you to leverage the full power of React and its ecosystem.

*   **Use the `PanelExtensionAdapter` only if you are porting an existing panel from Foxglove Studio** that was written using the extension API. While it is a powerful compatibility layer, it is not the recommended path for new development.

## Code Examples

To make the difference between the two approaches more concrete, here are simplified examples based on real panels in the Lichtblick codebase.

### Example 1: `PanelAPI` (based on the Raw Messages panel)

This example shows how a panel can use the `useMessageDataItem` hook (which is built on `useMessageReducer`) to subscribe to a topic and display its content.

```tsx
import { useMemo } from "react";
import { useMessageDataItem } from "@lichtblick/suite-base/components/MessagePathSyntax/useMessageDataItem";
import Panel from "@lichtblick/suite-base/components/Panel";

type RawMessagesPanelConfig = {
  topicPath: string;
};

function RawMessages(props: { config: RawMessagesPanelConfig }) {
  const { topicPath } = props.config;

  // The useMessageDataItem hook subscribes to the given topic path.
  // It returns the most recent message that matches the path.
  // The hook handles all the subscription logic with the message pipeline.
  const matchedMessages = useMessageDataItem(topicPath, { historySize: 1 });
  const latestMessage = matchedMessages[0];

  const messageContent = useMemo(() => {
    if (!latestMessage) {
      return "Waiting for message...";
    }
    // In the real panel, this is a complex JSON tree view.
    return JSON.stringify(latestMessage.queriedData, null, 2);
  }, [latestMessage]);

  return (
    <div>
      <h2>{topicPath}</h2>
      <pre>{messageContent}</pre>
    </div>
  );
}

export default Panel(
  Object.assign(RawMessages, {
    panelType: "RawMessages",
    defaultConfig: { topicPath: "" },
  }),
);
```

**Key takeaways:**
*   The panel is a standard React functional component.
*   It uses a hook (`useMessageDataItem`) to declare its data dependency.
*   It receives data from the hook's return value and uses it to render.
*   There is no manual subscription or lifecycle management.

### Example 2: `PanelExtensionAdapter` (based on the 3D/Image panel)

This example shows how a more complex panel, like the 3D panel, is initialized using the `PanelExtensionAdapter`.

```tsx
import {
  BuiltinPanelExtensionContext,
  PanelExtensionAdapter,
} from "@lichtblick/suite-base/components/PanelExtensionAdapter";
import Panel from "@lichtblick/suite-base/components/Panel";

// The actual rendering logic is in a separate `ThreeDeeRender` class.
// This class will be instantiated inside the initPanel function.
import { ThreeDeeRender } from "./ThreeDeeRender";

// 1. An 'initPanel' function is defined. It receives the extension context.
function initPanel(context: BuiltinPanelExtensionContext) {
  // The context provides a `panelElement` to render into.
  const renderer = new ThreeDeeRender(context);

  // The context's `onRender` property is assigned a callback function.
  // The adapter will call this function with new `RenderState` on every frame.
  context.onRender = (renderState, done) => {
    // The renderer class receives the new state and renders the scene.
    renderer.render(renderState);
    // The `done` callback must be called to signal that rendering is complete.
    done();
  };

  // Return a cleanup function to be called when the panel is unmounted.
  return () => {
    renderer.dispose();
  };
}

// 2. An adapter component wraps the PanelExtensionAdapter.
function ThreeDeeRenderAdapter(props: { config; saveConfig }) {
  return (
    <PanelExtensionAdapter
      config={props.config}
      saveConfig={props.saveConfig}
      initPanel={initPanel}
    />
  );
}

// 3. The final panel is exported.
export default Panel(
  Object.assign(ThreeDeeRenderAdapter, {
    panelType: "3D",
    defaultConfig: {},
  }),
);
```

**Key takeaways:**
*   The panel's logic is encapsulated in an `initPanel` function.
*   It interacts with the system via the `context` object, not hooks.
*   Data is "pushed" to the panel via the `onRender` callback.
*   The panel is responsible for its own rendering into a provided DOM element and for cleaning up its resources.
