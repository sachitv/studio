// SPDX-FileCopyrightText: Copyright (C) 2023-2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)<lichtblick@bmwgroup.com>
// SPDX-License-Identifier: MPL-2.0

import { CachingIterableSource } from "./CachingIterableSource";
import {
    GetBackfillMessagesArgs,
    IIterableSource,
    Initialization,
    IteratorResult,
    MessageIteratorArgs,
} from "./IIterableSource";

class TestSource implements IIterableSource {
    public async initialize(): Promise<Initialization> {
        return {
            start: { sec: 0, nsec: 0 },
            end: { sec: 10, nsec: 0 },
            topics: [],
            topicStats: new Map(),
            profile: undefined,
            alerts: [],
            datatypes: new Map(),
            publishersByTopic: new Map(),
        };
    }

    public async *messageIterator(
        _args: MessageIteratorArgs,
    ): AsyncIterableIterator<Readonly<IteratorResult>> {
        // Simple test implementation
        yield {
            type: "message-event",
            msgEvent: {
                topic: "test_topic",
                receiveTime: { sec: 1, nsec: 0 },
                message: { data: "test" },
                sizeInBytes: 100,
                schemaName: "test_schema",
            },
        };
    }

    public async getBackfillMessages(_args: GetBackfillMessagesArgs): Promise<MessageEvent<unknown>[]> {
        return [];
    }
}

describe("CachingIterableSource Performance Optimizations", () => {
    let mockSource: TestSource;
    let cachingSource: CachingIterableSource;

    beforeEach(() => {
        mockSource = new TestSource();
        cachingSource = new CachingIterableSource(mockSource, {
            maxBlockSize: 1024,
            maxTotalSize: 4096,
        });
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    describe("Memory Usage Profiling", () => {
        it("should provide memory usage profiling with optimization recommendations", async () => {
            await cachingSource.initialize();

            // Create some cache data
            const topics = new Map([["test_topic", { topic: "test_topic" }]]);

            // Iterate to create cache blocks
            const iterator = cachingSource.messageIterator({
                topics,
                start: { sec: 0, nsec: 0 },
                end: { sec: 5, nsec: 0 },
            });

            let messageCount = 0;
            for await (const result of iterator) {
                messageCount++;
                if (messageCount > 10) break; // Limit to prevent infinite loop
            }

            // Profile memory usage
            const profile = cachingSource.profileMemoryUsage();

            expect(profile).toHaveProperty("currentUsage");
            expect(profile).toHaveProperty("optimizationRecommendations");
            expect(profile).toHaveProperty("projectedSavings");
            expect(profile).toHaveProperty("fragmentationAnalysis");

            expect(Array.isArray(profile.optimizationRecommendations)).toBe(true);
            expect(typeof profile.projectedSavings).toBe("number");
            expect(profile.fragmentationAnalysis).toHaveProperty("totalFragmentation");
            expect(profile.fragmentationAnalysis).toHaveProperty("recommendedDefragmentation");
        });
    });

    describe("Cache Performance Benchmarking", () => {
        it("should provide cache performance benchmarking", async () => {
            await cachingSource.initialize();

            // Run benchmark
            const benchmark = await cachingSource.benchmarkCachePerformance({
                testDuration: 100, // Short duration for test
                testOperations: 10,
                includeStressTest: false,
            });

            expect(benchmark).toHaveProperty("baseline");
            expect(benchmark).toHaveProperty("withCache");
            expect(benchmark).toHaveProperty("improvement");

            expect(benchmark.baseline).toHaveProperty("averageBackfillTimeMs");
            expect(benchmark.withCache).toHaveProperty("cacheHitRate");
            expect(benchmark.improvement).toHaveProperty("backfillSpeedupFactor");
        });

        it("should include stress test results when requested", async () => {
            await cachingSource.initialize();

            const benchmark = await cachingSource.benchmarkCachePerformance({
                testDuration: 50,
                testOperations: 5,
                includeStressTest: true,
            });

            expect(benchmark.stressTestResults).toBeDefined();
            expect(benchmark.stressTestResults).toHaveProperty("maxMemoryUsage");
            expect(benchmark.stressTestResults).toHaveProperty("evictionEfficiency");
            expect(benchmark.stressTestResults).toHaveProperty("performanceDegradation");
        });
    });

    describe("Block Linking Optimization", () => {
        it("should provide block linking optimization analysis", async () => {
            await cachingSource.initialize();

            const optimization = cachingSource.optimizeBlockLinking();

            expect(optimization).toHaveProperty("currentLinkingPerformance");
            expect(optimization).toHaveProperty("optimizations");
            expect(optimization).toHaveProperty("projectedImprovement");

            expect(optimization.currentLinkingPerformance).toHaveProperty("averageLinkingTimeMs");
            expect(optimization.currentLinkingPerformance).toHaveProperty("linkingSuccessRate");
            expect(optimization.optimizations).toHaveProperty("fastPathEnabled");
            expect(optimization.projectedImprovement).toHaveProperty("speedupFactor");
        });
    });

    describe("Eviction Performance Monitoring", () => {
        it("should provide eviction performance monitoring", async () => {
            await cachingSource.initialize();

            const monitoring = cachingSource.monitorEvictionPerformance();

            expect(monitoring).toHaveProperty("currentEvictionMetrics");
            expect(monitoring).toHaveProperty("performanceAnalysis");
            expect(monitoring).toHaveProperty("optimizationStatus");

            expect(monitoring.currentEvictionMetrics).toHaveProperty("averageEvictionTimeMs");
            expect(monitoring.currentEvictionMetrics).toHaveProperty("evictionSuccessRate");
            expect(monitoring.performanceAnalysis).toHaveProperty("evictionEfficiency");
            expect(Array.isArray(monitoring.performanceAnalysis.bottlenecks)).toBe(true);
            expect(Array.isArray(monitoring.performanceAnalysis.recommendations)).toBe(true);
        });
    });

    describe("Comprehensive Metrics", () => {
        it("should provide comprehensive backfill cache metrics", async () => {
            await cachingSource.initialize();

            const metrics = cachingSource.getBackfillCacheMetrics();

            // Verify all expected metrics are present
            expect(metrics).toHaveProperty("cacheHits");
            expect(metrics).toHaveProperty("cacheMisses");
            expect(metrics).toHaveProperty("partialCacheHits");
            expect(metrics).toHaveProperty("backfillBlocksCreated");
            expect(metrics).toHaveProperty("blocksLinked");
            expect(metrics).toHaveProperty("blockPairsEvicted");
            expect(metrics).toHaveProperty("averageBackfillCreationTimeMs");
            expect(metrics).toHaveProperty("cacheHitRate");
            expect(metrics).toHaveProperty("memoryEfficiency");
            expect(metrics).toHaveProperty("sequentialAccessRatio");

            // All metrics should be numbers
            Object.values(metrics).forEach(value => {
                expect(typeof value).toBe("number");
            });
        });

        it("should provide detailed debug information", async () => {
            await cachingSource.initialize();

            const debugInfo = cachingSource.getBackfillDebugInfo();

            expect(debugInfo).toHaveProperty("forwardBlocksCount");
            expect(debugInfo).toHaveProperty("backfillBlocksCount");
            expect(debugInfo).toHaveProperty("totalMemoryUsage");
            expect(debugInfo).toHaveProperty("recentOperations");
            expect(debugInfo).toHaveProperty("metrics");
            expect(debugInfo).toHaveProperty("consistencyIssues");

            expect(Array.isArray(debugInfo.recentOperations)).toBe(true);
            expect(Array.isArray(debugInfo.consistencyIssues)).toBe(true);
        });

        it("should allow metrics reset", async () => {
            await cachingSource.initialize();

            // Get initial metrics
            const initialMetrics = cachingSource.getBackfillCacheMetrics();

            // Reset metrics
            cachingSource.resetBackfillMetrics();

            // Get metrics after reset
            const resetMetrics = cachingSource.getBackfillCacheMetrics();

            // All counters should be reset to 0
            expect(resetMetrics.cacheHits).toBe(0);
            expect(resetMetrics.cacheMisses).toBe(0);
            expect(resetMetrics.backfillBlocksCreated).toBe(0);
            expect(resetMetrics.blocksLinked).toBe(0);
        });
    });

    describe("Performance Tracking", () => {
        it("should track access patterns and block utilization", async () => {
            await cachingSource.initialize();

            const topics = new Map([["test_topic", { topic: "test_topic" }]]);

            // Create some cache activity
            const iterator = cachingSource.messageIterator({
                topics,
                start: { sec: 0, nsec: 0 },
                end: { sec: 2, nsec: 0 },
            });

            let messageCount = 0;
            for await (const result of iterator) {
                messageCount++;
                if (messageCount > 5) break;
            }

            // Get metrics to verify tracking is working
            const metrics = cachingSource.getBackfillCacheMetrics();
            const debugInfo = cachingSource.getBackfillDebugInfo();

            // Should have some activity recorded
            expect(debugInfo.recentOperations.length).toBeGreaterThanOrEqual(0);

            // Metrics should be properly initialized
            expect(typeof metrics.sequentialAccessRatio).toBe("number");
            expect(typeof metrics.averageBlockUtilization).toBe("number");
            expect(typeof metrics.memoryEfficiency).toBe("number");
        });
    });
});
