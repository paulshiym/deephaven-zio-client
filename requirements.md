# Requirements: Arrow <-> Case Class Mapping + Deephaven ZIO Streams

## Overview
Define a bidirectional mapping between Arrow schemas and Scala case classes using Shapeless, enabling:
- Publishing a ZIO ZStream of case class rows into Deephaven.
- Subscribing to a Deephaven table and receiving a ZIO ZStream of rows and updates.
- Comprehensive ZIO tests for type conversions.

## Scope
- Scala implementation inside this project.
- Arrow type support covers all primitive Arrow types plus Option and List.
- Two-way conversions (case class <-> Arrow vectors/batches).
- Integration points for Deephaven publish/subscribe flows.
- Use Scala 2.13 (latest patch) and ZIO latest version that supports Scala 2.13, built with sbt.

## Functional Requirements

### 1) Schema Derivation
- Provide a typeclass `ArrowSchema[A]` (name may differ) that can derive an Arrow `Schema` from a case class `A`.
- Use Shapeless `LabelledGeneric` (or equivalent) to derive schemas for product types (case classes).
- Field names in the Arrow schema must match case class field names exactly (configurable renaming is out of scope unless specified later).

### 2) Type Support
- Support all Arrow primitive types, mapping to Scala primitives and common JVM types.
- Minimum primitive list: Boolean, Int, Long, Float, Double, String, Byte, Short, Char, Binary/ByteArray, Decimal, Date/Time/Timestamp.
- Support Option[T] mapped to Arrow nullability for any supported T.
- Support List[T] mapped to Arrow list vectors for any supported T.
- If any Arrow primitive type cannot be mapped directly, define an explicit mapping rule and document it.

### 3) Encoding (Case Class -> Arrow)
- Provide an encoder that writes rows of `A` into Arrow vectors / record batches.
- Encoder must handle:
  - Nulls for Option[T].
  - Lists for List[T] (nested list vectors).
  - All supported primitive types.
- Batching must be supported to stream record batches to Deephaven efficiently.

### 4) Decoding (Arrow -> Case Class)
- Provide a decoder that reads Arrow vectors / record batches into instances of `A`.
- Decoder must handle:
  - Nulls mapped to Option[T].
  - Lists for List[T].
  - All supported primitive types.

### 5) Publishing Stream to Deephaven
- Provide an API that takes `ZStream[Any, Throwable, A]` and publishes it to Deephaven as a table.
- Must allow setting:
  - Table name.
  - Batch size.
  - Optional update mode (append-only vs keyed updates, if supported by Deephaven API).
- Use the Arrow encoder to convert batches before sending.
- Expose the publish API via a ZIO service layer that owns and closes required resources.

### 6) Subscribing to Deephaven Table
- Provide an API that subscribes to a Deephaven table and returns `ZStream[Any, Throwable, A]`.
- Stream must include initial snapshot and subsequent updates (additions/changes/removals) if supported.
- Use the Arrow decoder to convert data into case class instances.
- Expose the subscribe API via a ZIO service layer that owns and closes required resources.

## Testing Requirements
- Implement a ZIO Test suite for type conversions.
- Tests must cover:
  - Round-trip conversion: case class -> Arrow -> case class.
  - Each supported Arrow primitive type mapping.
  - Option[T] (Some/None) for multiple types.
  - List[T] for multiple types including empty and non-empty.
  - Composite case classes with multiple fields and mixed types.
- Tests should assert schema correctness (field names, types, nullability).
- Tests should be deterministic and not require a running Deephaven server.

## Non-Functional Requirements
- Conversions should be type-safe and compile-time derived where possible.
- Avoid excessive allocations where possible; reuse vectors/buffers in batching where safe.
- Clear, user-facing errors for unsupported types.

## Java 17+ note (Apache Arrow)
Apache Arrow requires additional JVM flags on Java 17+ due to the module system.

In CI we set:

- `JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`

If you run tests locally on Java 17+, use the same env var (or configure your build accordingly).

## Out of Scope
- Custom field name annotations or renaming logic.
- Advanced nested structures beyond List and Option unless specified later.
- End-to-end integration tests against a live Deephaven server.
