---
title: Spark Memory Management
---

# Spark Memory Management

This guide describes Auron's memory management on Spark, based on [revision `a1d46ccb`][revision]. See [Getting Started](./getting-started.md) for setup and [Configuration](./configuration.md) for the full configuration reference.

## Memory management overview

Auron runs inside the Spark executor's JVM process and has its own native memory manager. Spark and Auron manage different memory uses within that process:

| Memory use | How it is managed |
| --- | --- |
| Native working data: Arrow batches, sort keys, aggregation tables, and shuffle buffers | Participating operators report usage to Auron's native memory manager, which can trigger spilling. Allocating this data does not reserve bytes from Spark's `spark.memory.offHeap.size` budget; Spark does not account for it as off-heap execution/storage memory. |
| Heap-backed spill data: serialized native state kept for later readback | Spark accounts for these buffers as on-heap execution memory and can move them to local disk. |
| Auron memory outside operator accounting: temporary serialization/compression buffers and hash-join build state, alongside JVM internals and thread stacks | These uses contribute to process resident memory even when they are absent from Auron's reported operator totals. They still need room in the executor's overall memory allowance. |

**Auron's native budget is a spill target, not a hard allocation limit.** Its manager reacts to usage reports; it does not intercept every allocation or continuously reclaim memory.

[![Container and JVM memory scopes with configuration braces; green highlights identify Auron heap spill buffers and native allocations. The optional Spark-managed off-heap pool is separate and disabled in this example. These are conceptual scopes, not fixed physical partitions.](/memory-layout.svg)](/memory-layout.svg)

## How the native budget is calculated

Spark normally launches the executor with `spark.executor.memory` as its maximum heap (`-Xmx`). When the JVM's reported maximum matches that setting, the native budget simplifies to:

```text
native target = spark.executor.memoryOverhead * spark.auron.memoryFraction
```

The [implementation][native-helper] subtracts `Runtime.getRuntime.maxMemory()` from configured executor memory plus overhead before applying the fraction. The configuration is the requested heap size; `maxMemory()` is the maximum reported by the running JVM, which can differ slightly because of heap sizing/alignment. Neither is current heap usage.

Set `spark.executor.memoryOverhead` explicitly. Otherwise, this revision uses `max(10% of configured executor memory, 384 MiB)`, which may differ from the deployment's Spark defaults.

[Native initialization][native-init] creates one manager per process, shared across tasks and native plans. Configure it before native execution starts. Driver initialization uses the corresponding driver settings.

## Memory manager bookkeeping

An Auron **memory consumer** is a registered native working-state object, such as a sorter, aggregation table, or shuffle repartitioner for a task partition. It is an accounting registration, not a relationship between parent and child operators. A task can register several consumers.

Each consumer reports an estimate of its retained memory to the [native manager][memmgr]. For example, a sorter measures its buffered sorted blocks and reports an updated total after processing an input batch. The manager replaces the previous estimate and adjusts its process-wide totals by the difference; it does not discover usage by inspecting every allocation.

The operator also declares whether its state is **spillable**. Sorters and aggregation tables register as spillable while collecting input, then explicitly mark themselves unspillable for output/merging. The manager does not infer this from the memory size or Spark plan. Unspillable state remains in the accounting but is excluded from the number of consumers sharing the spillable budget.

To calculate each spillable consumer's share, the manager first subtracts reported unspillable usage and the JVM buffer-pool signal from its native target:

```text
available = max(native target - JVM buffer-pool capacity - unspillable usage, 0)
consumer share = available / number of spillable consumers
```

The share changes as consumers register, deregister, or change spillability. Registering another consumer does not immediately reclaim existing allocations; the next eligible usage update can trigger spilling. Dropping a consumer removes its remaining reported usage. Memory that was never reported is **untracked**, not unspillable.

The JVM deduction, logged as `jvm_direct`, sums buffer-pool capacities through [JniBridge][jni-bridge]; it is a pressure signal rather than a complete native-memory measurement.

## How spilling works

When the native manager triggers a spill, the operator can use available Spark heap memory to avoid immediate disk I/O. If heap storage is ineligible or a write cannot acquire enough memory, it uses a local file. Heap-backed spills can also move to disk later.

[![Spill-storage decisions: heap storage requires an active task and both Spark execution-pool and JVM committed-heap usage below the configured threshold, 90% by default. Each write must then receive a full memory grant. Otherwise use disk; existing heap buffers can also move to disk later.](/memory-spilling.svg)](/memory-spilling.svg)

### 1. Is heap storage eligible?

The [spill factory][spill] checks for an active Spark task and compares two usage ratios with `spark.auron.onHeapSpill.memoryFraction`, which defaults to `0.9`:

| Check | Usage ratio |
| --- | --- |
| Spark on-heap execution pool | Used execution memory / current used-plus-free pool capacity |
| JVM heap | Used heap / currently committed heap |

**Both ratios must be below the threshold.** This avoids choosing heap storage when either Spark's execution pool or the wider JVM heap is already busy. The JVM check includes objects outside Spark's execution accounting and uses committed heap, not maximum heap. Without an active task, or if either check fails, the spill goes directly to disk.

### 2. Does Spark grant enough memory for the write?

Eligibility permits an attempt; it does not reserve space. [SparkOnHeapSpillManager][heap-manager] requests on-heap execution memory from Spark for each write. A full grant allows the serialized bytes to remain in heap buffers. If the grant is partial or refused, [OnHeapSpill][heap-spill] returns any partial grant, moves buffered data to a local file, and continues writing there.

### 3. Can the heap buffers remain in memory?

A later write can fail to acquire memory, or Spark can call the heap manager's spill callback to reclaim execution memory. In either case, heap-backed spill data can move to disk. Spark is reclaiming the serialized spill buffers, not directly spilling the original native operator state.

Files use Spark's local block directories. The compressed spill interface uses `spark.auron.spill.compression.codec`.

For remote shuffle, such as Celeborn or Uniffle, a spill instead drains Auron's buffered shuffle output into the service's client writer. This releases Auron's buffer without creating a temporary heap/file spill; the client may still buffer data before sending it. Sort and aggregation spills are temporary state for later merging, not output sent to a shuffle service.

### What triggers the native spill?

On an eligible growth update, the native manager can trigger a spill when total reported usage exceeds the adjusted budget, the consumer exceeds its share, or process resident memory exceeds:

```text
process-pressure threshold =
    (spark.executor.memory + executor memory overhead)
    * spark.auron.process.vmrss.memoryFraction
```

The process signal samples Linux resident memory, including heap and other allocations in the JVM process. It is not total container usage or a measurement of the container limit. The checks are cooperative, not continuous; some operators can also request a forced spill.

### Readback and cleanup

Merging spilled state still needs working memory and can be unspillable. Reading or releasing heap spill buffers returns their accounted bytes to Spark; releasing file spills closes and deletes their files.

Dropped native consumers deregister their remaining usage. Task listeners close native wrappers and release held heap spills; native finalization cancels pending work. Cleanup can be asynchronous, and abrupt process termination can prevent it.

## Configuration

Defaults below apply to the referenced revision.

| Setting | Default in the referenced source | Role |
| --- | --- | --- |
| `spark.auron.memoryFraction` | `0.6` | Multiplier used to initialize the process-wide native target. |
| `spark.auron.process.vmrss.memoryFraction` | `0.9` | Process-RSS spill threshold fraction of configured heap plus overhead. |
| `spark.auron.onHeapSpill.memoryFraction` | `0.9` | Usage-ratio threshold for allowing a new heap-backed spill. |
| `spark.auron.batchSize` | `10000` rows | Suggested Arrow batch row count, not a byte ceiling. |
| `spark.auron.suggested.batch.memSize` | `8388608` bytes (8 MiB) | Suggested batch-memory size used by batching helpers. |
| `spark.auron.spill.compression.codec` | `lz4` | Codec for the compressed spill interface. |
| `spark.auron.rss.spill.memoryFraction` | `0.4` | Relative consumer-share trigger for remote sort shuffle. |
| `spark.auron.rss.spill.memorySize` | `0` (disabled) | Additional absolute byte trigger for remote sort shuffle. |
| `spark.auron.smjfallback.enable` | `false` | Enable the hash-map build's sorted-stream fallback. |
| `spark.auron.smjfallback.mem.threshold` | `134217728` bytes (128 MiB) | Staged-input byte threshold when that fallback is enabled. |
| `spark.auron.smjfallback.rows.threshold` | `10000000` rows | Staged-input row threshold when that fallback is enabled. |

Lowering the native fraction encourages earlier spilling; lowering the heap-spill threshold routes more new spills to disk. Concurrency, skew, aggregation cardinality, and join strategy also affect the working set. A batch row limit cannot bound bytes for variable-sized rows.

## Implementation references

These links are pinned to the revision used for this guide.

| Area | Source |
| --- | --- |
| Spark budget calculation | [NativeHelper][native-helper] |
| Native initialization and allocator | [exec.rs][native-init], [alloc.rs][allocator] |
| Consumer accounting and pressure decisions | [auron-memmgr][memmgr] |
| JVM measurements | [JniBridge][jni-bridge], [SparkAuronAdaptor][spark-adaptor] |
| Arrow ownership and runtime cleanup | [AuronCallNativeWrapper][native-wrapper], [FFI reader][ffi-reader], [native runtime][native-runtime], [ArrowUtils][arrow-utils] |
| Spill storage and metrics | [spill.rs][spill], [metrics.rs][spill-metrics] |
| Spark heap spill management | [SparkOnHeapSpillManager][heap-manager], [OnHeapSpill][heap-spill], [SpillBuf][spill-buf] |
| Operator examples | [Sort][sort], [Aggregation][aggregation], [Sort shuffle][shuffle], [Remote sort shuffle][remote-shuffle] |
| Hash-map building and sharing | [Hash-map builder][hash-builder], [Broadcast join][broadcast-join] |
| Defaults and configuration keys | [AuronConfiguration][base-conf], [SparkAuronConfiguration][spark-conf] |

[revision]: https://github.com/apache/auron/commit/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843
[native-helper]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/scala/org/apache/spark/sql/auron/NativeHelper.scala
[native-init]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron/src/exec.rs
[allocator]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron/src/alloc.rs
[memmgr]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron-memmgr/src/lib.rs
[jni-bridge]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/auron-core/src/main/java/org/apache/auron/jni/JniBridge.java
[spark-adaptor]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/java/org/apache/auron/jni/SparkAuronAdaptor.java
[native-wrapper]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/auron-core/src/main/java/org/apache/auron/jni/AuronCallNativeWrapper.java
[ffi-reader]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/ffi_reader_exec.rs
[native-runtime]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron/src/rt.rs
[arrow-utils]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/scala/org/apache/spark/sql/execution/auron/arrowio/util/ArrowUtils.scala
[spill]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron-memmgr/src/spill.rs
[spill-metrics]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/auron-memmgr/src/metrics.rs
[heap-manager]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/SparkOnHeapSpillManager.scala
[heap-spill]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/OnHeapSpill.scala
[spill-buf]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/scala/org/apache/spark/sql/auron/memory/SpillBuf.scala
[sort]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/sort_exec.rs
[aggregation]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/agg/agg_table.rs
[shuffle]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/shuffle/sort_repartitioner.rs
[remote-shuffle]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/shuffle/rss_sort_repartitioner.rs
[hash-builder]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/broadcast_join_build_hash_map_exec.rs
[broadcast-join]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/native-engine/datafusion-ext-plans/src/broadcast_join_exec.rs
[base-conf]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/auron-core/src/main/java/org/apache/auron/configuration/AuronConfiguration.java
[spark-conf]: https://github.com/apache/auron/blob/a1d46ccb8ce1e6b0be5893b0ebc4ddef76c9b843/spark-extension/src/main/java/org/apache/auron/spark/configuration/SparkAuronConfiguration.java
