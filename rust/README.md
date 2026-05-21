<!---
  Copyright 2026-present Alibaba Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Paimon C++ + Vortex Rust SST Lookup 技术方案

本文档描述如何在 `paimon-cpp` 中引入一个 Rust 子工程，用 Vortex 的轻量级列编码能力改造 lookup SST block 内部布局，从而加速 primary key 点查。

文档目标是给实现者使用，不是概念介绍。读完后应当能明确知道：

- C++ 和 Rust 的职责边界
- 为什么 FFI 必须 batch 化
- Arrow C Data Interface 如何传递数据和所有权
- leaf block / non-leaf block 内部应该如何布局
- compare path 如何在压缩态上逐列短路比较
- v1 需要实现哪些 API，哪些优化可以后续再做

## 背景

`paimon-cpp` 当前 lookup SST 的创建路径有两层特征：

- 上游读数据文件时已经是 batch / Arrow 风格。
- 落到当前 SST block writer 时，数据被拆成一条条 `key/value` 记录写入 row-wise block。

当前 C++ SST block 的核心路径是：

- `LookupLevels::CreateSstFileFromDataFile(...)` 从数据文件创建 lookup file。
- `KeyValueDataFileRecordReader::NextBatch()` 批量读入 Arrow batch，再通过 iterator 按行产出 `KeyValue`。
- `LookupStoreWriter::Put(...)` 最终走向 `SstFileWriter::Write(...)`。
- `SstFileWriter::Write(...)` 调用 `BlockWriter::Write(key, value)`。
- `BlockWriter::Write(...)` 以 row-wise 方式写入 `key_len | key | value_len | value`。

这意味着现在的 lookup file 构建并不是从头到尾 row-by-row。真正 row-wise 的部分主要发生在 SST block 写入的最后一段。

本方案利用这个事实，把 Rust 接入点放在 block codec 层，而不是重写完整 SST。

## 目标

v1 目标：

- 保持 `paimon-cpp` 现有 SST 宏观结构不变。
- 保持 file-level writer、block split、index block、footer、bloom filter 仍由 C++ 管理。
- Rust 只负责把一个已经排序的 block-sized Arrow batch 编码成 Vortex-backed SST block bytes。
- C++ 调 Rust 时使用 batch FFI，一次调用处理一个 leaf block 或 non-leaf block。
- FFI 输入通过 Arrow C Data Interface 零拷贝导入 Rust。
- Rust 生成的 block 内部使用 columnized key layout，支持 compare path 上的随机访问轻压缩。
- 多列主键按字典序逐列比较，前一列已经分出大小时不读取后一列。
- 字符串 key v1 使用 `prefix key + full key verification`，先不引入复杂字符串专用 encoding 设计。

非目标：

- 不用 Rust 重写 SST 文件整体格式。
- 不用 Rust 重写 block split 策略。
- 不用 Rust 重写 lookup levels / sorted run / level0 搜索逻辑。
- 不把整个 SST file 设计成一个 Vortex file。
- 不承诺从输入 Arrow batch 到最终 block bytes 全过程零拷贝。

这里的零拷贝含义是：C++ 到 Rust 的 FFI 输入边界零拷贝，不做 row-wise 中间 materialization。Rust 编码 SST block 时仍然需要分配最终输出 buffer，这是正常且必要的。

## 总体架构

```text
                         paimon-cpp
┌─────────────────────────────────────────────────────────────────┐
│ LookupLevels / LookupFile / LookupStore                         │
│                                                                 │
│  1. 从 data file 读取 Arrow batch                               │
│  2. 按 SST block size 聚合出一个 sorted block batch              │
│  3. 通过 Arrow C Data Interface 调 Rust                          │
│  4. 接收 Rust 返回的 encoded block bytes                         │
│  5. 继续写 block handle / index block / footer / bloom filter    │
└──────────────────────────────┬──────────────────────────────────┘
                               │ batch FFI
                               │ ArrowSchema + ArrowArray
                               ▼
                         Rust cdylib
┌─────────────────────────────────────────────────────────────────┐
│ paimon_vortex_sst                                               │
│                                                                 │
│  1. import Arrow C batch                                         │
│  2. 校验 key columns / value columns / row ordering               │
│  3. 构建 columnized leaf/non-leaf block                          │
│  4. 使用 Vortex array encodings 压缩 compare path columns         │
│  5. 序列化为 SST block bytes                                     │
│  6. 返回 owned bytes 给 C++                                      │
└─────────────────────────────────────────────────────────────────┘
```

核心边界：

- C++ 是 SST orchestrator。
- Rust 是 block codec engine。
- Arrow C Data Interface 是跨语言数据边界。
- Vortex 是 Rust 内部的列编码与随机访问 compare 引擎。

## 代码位置

Rust 子工程位于：

```text
paimon-cpp/rust/
```

当前文件：

```text
rust/Cargo.toml
rust/src/lib.rs
rust/src/sst.rs
rust/src/sst/ffi.rs
```

当前 `Cargo.toml` 使用本地 Vortex path dependency：

```toml
vortex = { path = "/home/xiaoheng/RustProjects/vortex/vortex", features = ["files"] }
```

后续如果进入正式工程，应将该路径改成可配置来源，例如：

- git dependency
- workspace vendor dependency
- CMake 传入的 `VORTEX_RUST_PATH`
- release tarball / submodule

## C++ / Rust 职责边界

C++ 继续负责：

- 从 Paimon data file 读取数据。
- 处理 schema evolution、row kind、sequence number、partition、bucket、level 等 Paimon 语义。
- 确保输入给 Rust 的 block batch 已按 primary key 排序。
- 控制 SST block split。
- 管理 leaf block 和 non-leaf block 的整体 B-Tree-like 结构。
- 管理 block handle、index block、footer、bloom filter。
- 管理 lookup file cache、remote lookup file download、本地临时文件生命周期。
- 将 Rust 返回的 block bytes 写入现有 OutputStream。

Rust 负责：

- 接收一个 block-sized Arrow batch。
- 从 Arrow batch 中识别 key columns、value columns、child pointer columns。
- 将 row-wise batch 逻辑视图转换成 block 内 columnized layout。
- 对 compare path 上的 key columns 选择 Vortex encoding。
- 实现 `compare_at(row, query)` 的逐列短路比较。
- 实现 block 内 binary search。
- 序列化 / 反序列化 leaf block 和 non-leaf block。
- 返回 encoded block bytes、block stats、separator key 等 C++ 需要的元数据。

一个重要原则：

Rust 不应该知道 Paimon level、manifest、commit、data file split 等上层概念。Rust 只理解“这里有一批已排序的 key/value 行，请编码成一个可点查的 SST block”。

## 为什么必须 batch FFI

逐行 FFI 的问题：

- 每条 key/value 都跨一次语言边界，调用成本高。
- C++ 需要先把 Arrow batch 拆成 row object。
- Rust 再把 row object 重新组回 column layout，重复工作。
- 变长字符串和 value bytes 容易产生额外拷贝。

batch FFI 的好处：

- 一次 FFI 调用处理一个 leaf block。
- Rust 直接看 Arrow buffers。
- key columns 天然是列式输入，适合 Vortex encoding。
- value payload 可以按 column 或 arena 方式单独处理。
- C++ 只负责 block 聚合和排序，不参与 block 内编码细节。

推荐粒度：

```text
1 FFI call = 1 logical leaf block
1 FFI call = 1 logical non-leaf block
```

不要设计成：

```text
1 FFI call = 1 row
```

也不建议 v1 设计成：

```text
1 FFI call = whole SST file
```

whole SST file 粒度会迫使 Rust 接管 block split、index block、footer，边界太大。

## Arrow C Data Interface 约定

v1 采用 Arrow C Data Interface：

```c
struct ArrowSchema;
struct ArrowArray;
```

Rust 侧当前 API 形态：

```c
int vortex_sst_build_leaf_block_from_ffi(
    const struct ArrowSchema* schema,
    const struct ArrowArray* array,
    const struct VortexSstLeafBuildOptions* options,
    struct VortexOwnedBytes* out_bytes);

void vortex_sst_free_owned_bytes(struct VortexOwnedBytes bytes);
```

当前 Rust 代码里对应：

```rust
#[repr(C)]
pub struct VortexSstLeafBuildOptions {
    pub key_column_count: u32,
    pub string_prefix_len: u32,
}

#[repr(C)]
pub struct VortexOwnedBytes {
    pub data: *const u8,
    pub len: usize,
}
```

### Ownership

推荐 ownership 规则：

- C++ 创建 `ArrowSchema` / `ArrowArray`。
- C++ 调用 Rust FFI 时，将 `ArrowSchema` / `ArrowArray` 所有权转移给 Rust。
- Rust import 后负责触发 Arrow release callback。
- C++ 调用返回后不再访问这两个 FFI struct。
- Rust 返回 `VortexOwnedBytes`。
- C++ 写完 block bytes 后必须调用 `vortex_sst_free_owned_bytes(...)`。

这样可以避免双重释放，也避免 Rust import 时复制 Arrow buffers。

如果 C++ 希望调用后继续持有原始 Arrow batch，需要在 C++ 侧 export 一个新的 Arrow C wrapper，而不是把仍需继续使用的 wrapper 交给 Rust 消费。

## 输入 batch schema 约定

v1 leaf block 输入建议：

```text
StructArray {
  key_col_0,
  key_col_1,
  ...,
  key_col_n,
  value_payload
}
```

通过 `VortexSstLeafBuildOptions::key_column_count` 指定前多少列是 primary key。

v1 先支持：

- `Int32`
- `Int64`
- `UInt32`
- `UInt64`
- `Utf8`
- `LargeUtf8`
- `Binary`
- `LargeBinary`

如果 Paimon primary key 包含 decimal、date、timestamp，v1 可先在 C++ 侧转成稳定可比较的 physical representation，例如：

- date -> `Int32`
- timestamp -> `Int64`
- decimal -> fixed-width binary 或 scaled integer

后续再扩展 Rust 侧 native dtype 支持。

### 输入排序要求

C++ 必须保证输入 batch 已按 Paimon primary key 字典序排序：

```text
ORDER BY key_col_0, key_col_1, ..., key_col_n
```

Rust v1 应该做 debug / validation：

- 检查 row count > 0。
- 检查 key column count > 0。
- 检查 key column count <= batch column count。
- 可选检查排序。

排序检查可以配置开关：

- debug / test 默认打开。
- release 可以关闭或采样检查。

## Leaf Block 内部布局

推荐 v1 leaf block layout：

```text
LeafBlock
├── Header
│   ├── magic
│   ├── version
│   ├── row_count
│   ├── key_column_count
│   ├── section offsets
│   └── encoding metadata
│
├── Hot Compare Path
│   ├── key_col_0 encoded by Vortex
│   ├── key_col_1 encoded by Vortex
│   ├── ...
│   └── key_col_n encoded by Vortex
│
├── String Key Support
│   ├── prefix_col_i encoded by Vortex
│   └── full_key_storage_i
│
├── Value Index
│   ├── value_offset encoded by Vortex
│   └── value_len encoded by Vortex
│
└── Cold Payload
    └── value_bytes
```

热路径：

- key numeric columns
- string prefix columns
- value offset/len only after key match

冷路径：

- full key verification storage
- value bytes

### 为什么 key columnized

点查 binary search 的每一步只需要读取 `mid` 位置的 key。多列主键比较时，前一列已经分出大小，就不需要读取后一列。

row-wise layout 的问题是：

- 需要定位一整条 row record。
- 多列 key 混在一起，不利于只读第 1 列。
- zstd 等整块压缩需要先解压 block 才能比较。

columnized layout 的优势是：

- `key_col_0.scalar_at(mid)` 可以直接读取第 1 列。
- 如果第 1 列已经决定大小，不读 `key_col_1`。
- 如果第 1 列相等，再读第 2 列。
- 对整数 / 时间 / ID 类列，Vortex 的 FoR / BitPacked / RLE 等 encoding 可以保留随机访问能力。

## Non-leaf Block 内部布局

non-leaf block 也应该使用同样的逐列比较思想。

推荐 v1 non-leaf block layout：

```text
NonLeafBlock
├── Header
│   ├── magic
│   ├── version
│   ├── separator_count
│   ├── key_column_count
│   └── section offsets
│
├── Hot Compare Path
│   ├── separator_key_col_0 encoded by Vortex
│   ├── separator_key_col_1 encoded by Vortex
│   └── ...
│
└── Child Pointers
    ├── child_block_offset encoded by Vortex or fixed-width raw
    ├── child_block_len encoded by Vortex or fixed-width raw
    └── optional child metadata
```

查找流程：

1. 在 root non-leaf block 中二分 separator keys。
2. 用逐列短路 compare 找到 child pointer。
3. 读取下一层 non-leaf 或 leaf block。
4. 重复直到 leaf block。
5. 在 leaf block 中二分，命中后读取 value。

non-leaf 的 key 数量通常比 leaf 少，但它在每次 lookup 中必经，所以仍值得避免整块解压。

## Compare Path

核心接口：

```rust
fn compare_at(row: usize, query: &QueryKey) -> Ordering
```

伪代码：

```text
compare_at(row, query):
  for col_idx in 0..key_column_count:
    block_value = key_col[col_idx].scalar_at(row)
    query_value = query[col_idx]

    ordering = compare(block_value, query_value)

    if ordering != Equal:
      return ordering

  return Equal
```

字符串列 v1 使用 prefix + full verification：

```text
compare_string_at(row, query):
  prefix = prefix_col.scalar_at(row)
  query_prefix = truncate(query, prefix_len)

  if prefix < query_prefix:
    return Less

  if prefix > query_prefix:
    return Greater

  if prefix is enough to prove equality:
    return Equal

  full_key = full_key_storage.get(row)
  return compare(full_key, query)
```

binary search：

```text
binary_search_in_block(query):
  left = 0
  right = row_count

  while left < right:
    mid = left + (right - left) / 2
    ordering = compare_at(mid, query)

    if ordering < 0:
      left = mid + 1
    else if ordering > 0:
      right = mid
    else:
      return Found(mid)

  return NotFound
```

关键规则：

- compare path 必须按 primary key 字典序读取列。
- 前一列已分出大小时，禁止继续读取后一列。
- string prefix 只能作为提前判断手段，不能替代 full key verification。
- 命中 value 前不读取 value bytes。

## Encoding 选择

compare path 上的 key columns 必须支持 `scalar_at` 或等价随机访问 compare。

推荐：

- 整数 / 时间 / ID：`FoR`、`BitPacked`、`Delta`、`RLE`
- 重复值多的列：`RLE`
- 单调或局部密集整数：`FoR + BitPacked`
- 字符串 / 二进制 key：v1 使用 `prefix column + full key storage`
- value bytes：不进入热路径，可用更重压缩

不推荐：

- 把 key compare path 放进 zstd 整块压缩。
- 把 leaf 主路径做成 row-wise compressed record block。
- 在 compare path 上使用必须整列或整块解压才能取单点的 encoding。

zstd 可以用于：

- cold value arena
- 大 value payload
- 命中后才访问的数据

zstd 不应用于：

- leaf key columns
- non-leaf separator key columns
- block 内 binary search 主路径

## Rust API 规划

v1 C ABI：

```c
typedef struct VortexSstLeafBuildOptions {
    uint32_t key_column_count;
    uint32_t string_prefix_len;
} VortexSstLeafBuildOptions;

typedef struct VortexOwnedBytes {
    const uint8_t* data;
    uintptr_t len;
} VortexOwnedBytes;

typedef enum VortexSstStatusCode {
    VORTEX_SST_OK = 0,
    VORTEX_SST_NULL_POINTER = 1,
    VORTEX_SST_INVALID_INPUT = 2,
    VORTEX_SST_INTERNAL_ERROR = 3,
} VortexSstStatusCode;

VortexSstStatusCode vortex_sst_build_leaf_block_from_ffi(
    const struct ArrowSchema* schema,
    const struct ArrowArray* array,
    const VortexSstLeafBuildOptions* options,
    VortexOwnedBytes* out_bytes);

void vortex_sst_free_owned_bytes(VortexOwnedBytes bytes);
```

v2 增加：

```c
VortexSstStatusCode vortex_sst_build_non_leaf_block_from_ffi(...);

VortexSstStatusCode vortex_sst_lookup_leaf_block(
    const uint8_t* block_data,
    uintptr_t block_len,
    const struct ArrowSchema* key_schema,
    const struct ArrowArray* key_array,
    VortexLookupResult* out);
```

v3 可考虑 stateful builder：

```c
VortexSstStatusCode vortex_sst_leaf_builder_new(..., VortexSstLeafBuilder** out);
VortexSstStatusCode vortex_sst_leaf_builder_push_batch(...);
VortexSstStatusCode vortex_sst_leaf_builder_finish(...);
void vortex_sst_leaf_builder_free(...);
```

stateful builder 适合 C++ 侧 block split 和 Rust 侧 encoding 选择需要协同的场景。v1 先不做，避免边界扩大。

## C++ 接入点

建议先新增一套 lookup store writer：

```text
VortexLookupStoreWriter
VortexLookupStoreReader
VortexBlockWriterAdapter
```

接入位置：

- `LookupStoreFactory` 增加一种 store type，例如 `VORTEX_SORT`。
- `CreateSstFileFromDataFile(...)` 仍然由 C++ 调度。
- C++ 从 `KeyValueDataFileRecordReader::NextBatch()` 获得 batch 后，不要立刻逐行 `Put`。
- C++ 聚合一个 leaf block batch。
- C++ 调 `vortex_sst_build_leaf_block_from_ffi(...)`。
- C++ 将返回 bytes 写入 current SST data block area。

v1 为降低侵入性，可以先这样接：

```text
existing LookupLevels
  -> existing reader
  -> new VortexLookupStoreWriter
  -> block-sized Arrow batch buffer
  -> Rust leaf block codec
  -> existing OutputStream
```

不要第一步就改全局 lookup 逻辑。先让新的 writer 能生成一个兼容读取路径的实验文件，或者生成独立实验 lookup file。

## Rust 内部模块规划

建议 Rust 侧模块拆分：

```text
rust/src/lib.rs
rust/src/sst.rs
rust/src/sst/ffi.rs
rust/src/sst/block.rs
rust/src/sst/leaf.rs
rust/src/sst/non_leaf.rs
rust/src/sst/compare.rs
rust/src/sst/encoding.rs
rust/src/sst/serialize.rs
```

职责：

- `ffi.rs`：C ABI、Arrow C import、ownership、error code。
- `block.rs`：公共 block header、section table、version。
- `leaf.rs`：leaf block builder / reader。
- `non_leaf.rs`：non-leaf block builder / reader。
- `compare.rs`：`compare_at`、`binary_search_in_block`。
- `encoding.rs`：根据 dtype 和 stats 选择 Vortex encoding。
- `serialize.rs`：block bytes 格式。

当前已经先拆出 FFI 边界和 v1 leaf block codec：

- `rust/src/sst.rs`：SST codec 模块门面，只做子模块声明和 public re-export。
- `rust/src/sst/ffi.rs`：C ABI，包含 Arrow C import、ownership、status code、panic boundary 和返回 buffer 释放接口。
- `rust/src/sst/block.rs`：v1 block header、section table、little-endian 基础读写。
- `rust/src/sst/leaf.rs`：v1 leaf block encode / reader / binary search / compare path。

后续实现 `block.rs`、`leaf.rs`、`non_leaf.rs` 等模块时，不应把 block 编码逻辑继续堆进 `ffi.rs`。`ffi.rs` 只负责跨语言边界，导入 Arrow batch 后应调用内部 builder/serializer。

## 序列化格式建议

v1 可以先自定义一个简单 block format，不要求和 Vortex file format 兼容。

建议 header：

```text
BlockHeader
├── magic: "VXST"
├── version: u16
├── block_kind: u8
│   ├── 1 = leaf
│   └── 2 = non_leaf
├── row_count: u32
├── key_column_count: u16
├── section_count: u16
└── section_table_offset: u32
```

section table：

```text
SectionEntry
├── section_kind: u16
├── column_index: u16
├── offset: u64
├── length: u64
└── flags: u32
```

section kinds：

```text
1  = key_column
2  = string_prefix_column
3  = full_key_storage
4  = value_offset_column
5  = value_len_column
6  = value_bytes
7  = child_block_offset
8  = child_block_len
```

这样 reader 可以先读 header 和 section table，再按需访问热路径 section。

## Error Handling

C ABI 不要跨语言传 Rust panic 或 Rust error object。

v1 用 status code：

- `Ok`
- `NullPointer`
- `InvalidInput`
- `InternalError`

v2 应增加 error message API：

```c
const char* vortex_sst_last_error_message(void);
void vortex_sst_clear_error(void);
```

或者让每个 handle 维护 error buffer。

Rust FFI 边界必须：

- catch panic
- 将 panic 转成 `InternalError`
- 不 unwind 到 C++

当前 C ABI 已经在 `vortex_sst_build_leaf_block_from_ffi`、`vortex_sst_lookup_leaf_block`、`vortex_sst_free_owned_bytes` 外层使用 `catch_unwind`。

## Memory Management

Rust 返回 bytes：

```rust
let boxed = bytes.into_boxed_slice();
let len = boxed.len();
let data = Box::into_raw(boxed) as *const u8;
```

C++ 使用后调用：

```c
vortex_sst_free_owned_bytes(bytes);
```

规则：

- C++ 不要用 `free()` 释放 Rust 返回的 buffer。
- Rust 不要释放 C++ 仍在使用的 Arrow batch。
- Arrow C import 后由 Rust 按 Arrow release callback 释放 FFI wrapper。
- Arrow buffers 本身由 Arrow reference counting / release callback 管理。

## 实现阶段

### Phase 0：脚手架

已完成：

- `rust/Cargo.toml`
- `rust/src/lib.rs`
- `rust/src/sst.rs`
- 本地 `vortex` path dependency
- batch FFI API scaffold
- `cargo check` 通过
- `cargo test -p paimon_vortex_sst` 通过

### Phase 1：Leaf block encode

实现内容：

- 从 Arrow `StructArray` 中拆出 key columns 和 value column。
- 支持 fixed-width numeric key。
- 支持 string prefix column。
- 生成 `value_offset/value_len/value_bytes`。
- 使用 Vortex array encoding 压缩 key columns。
- 序列化为 `LeafBlock` bytes。

验收标准：

- Rust 单测能构建一个 leaf block。
- Rust 单测能从 leaf block 中点查命中 value。
- 第一列分出大小时，不读取第二列。
- string prefix 碰撞时才读取 full key。

### Phase 2：Leaf block decode + lookup

实现内容：

- 从 block bytes 解析 `LeafBlockReader`。
- 实现 `binary_search_in_block`。
- 实现 `compare_at(row, query)`。
- 支持 Arrow C query key 输入。

验收标准：

- 能用 Rust API 对 block bytes 点查。
- 能测量 compare path 读取列次数。
- 能证明 value bytes 只在命中后读取。

### Phase 3：C++ writer 集成

实现内容：

- C++ 增加 `VortexLookupStoreWriter`。
- 聚合 block-sized Arrow batch。
- 调 Rust FFI 生成 leaf block bytes。
- 将 bytes 写入现有 SST output stream。
- 保留现有 block handle / index / footer 流程。

验收标准：

- 能从一个 data file 生成 vortex lookup file。
- 文件级 lookup 能走到 Rust leaf block reader。
- 与现有 SORT lookup 返回结果一致。

### Phase 4：Non-leaf block

实现内容：

- separator key columnized。
- child pointer columnized。
- non-leaf binary search。
- root-to-leaf 导航。

验收标准：

- 多层 SST lookup 正确。
- non-leaf compare path 不需要整块解压。

### Phase 5：性能验证

测试维度：

- 单列 int primary key。
- 多列 int primary key。
- int + string primary key。
- 高重复 key prefix。
- 大 value payload。
- 不同 block size。

指标：

- lookup p50 / p95 / p99 latency。
- 每次 lookup 读取 bytes。
- 每次 lookup 解压 bytes。
- FFI 调用次数。
- block build CPU。
- lookup file size。

对比对象：

- 当前 SORT lookup file。
- 当前 HASH lookup file。
- zstd row-wise block 方案。

## 风险与决策

### 风险：Arrow ownership 接错导致 double free

决策：

- FFI 文档明确 transfer ownership。
- C++ wrapper 封装一次性 export，不允许继续复用已交给 Rust 的 FFI wrapper。
- 增加 ASAN / UBSAN 测试。

### 风险：Vortex encoding 不适合 scalar_at 热路径

决策：

- compare path encoding 必须白名单。
- key columns 不允许使用必须整块解压的 encoding。
- 每个 encoding 加 `scalar_at` 单测。

### 风险：string prefix 误判

决策：

- prefix 只用于提前判断 `<` 或 `>`。
- prefix 相等时必须 full key verification。
- 测试覆盖 prefix collision，例如 `alpha` vs `alpine`。

### 风险：block format 过早复杂化

决策：

- v1 只做 leaf block。
- v1 只支持必要 dtype。
- v1 不引入自定义 Vortex extension dtype。
- v1 不接管完整 SST file。

## 当前状态

当前 `rust/src/sst.rs` 是 SST codec 模块门面，Rust 侧已经具备 v1 leaf block codec：

- 已定义 `VortexSstLeafBuildOptions`
- 已定义 `VortexOwnedBytes`
- 已定义 `VortexSstLookupResult`
- 已定义 `VortexSstStatusCode`
- 已实现 `vortex_sst_build_leaf_block_from_ffi`
- 已实现 `vortex_sst_lookup_leaf_block`
- 已实现 `vortex_sst_free_owned_bytes`
- 已能 import Arrow C `StructArray`
- 已能创建 Vortex session
- 已实现 sectioned leaf block bytes：header、section table、typed key columns、`value_offset/value_len/value_bytes`
- 已实现 Rust 内部 `LeafBlockReader::binary_search_in_block` 和 `compare_at`
- 已支持 `Int32`、`Int64`、`UInt32`、`UInt64`、`Utf8`、`LargeUtf8`、`Binary`、`LargeBinary` key
- v1 value payload 要求是单个 `Utf8` / `LargeUtf8` / `Binary` / `LargeBinary` 列
- C++ 侧已准备 `include/paimon/common/lookup/vortex/vortex_sst_ffi.h`
- C++ 侧已准备 `include/paimon/common/lookup/vortex/vortex_sst_codec.h`

当前验证命令：

```bash
cd /home/xiaoheng/CLionProjects/paimon-cpp/rust
cargo metadata --format-version 1 --no-deps
cargo check
cargo test -p paimon_vortex_sst

cd /home/xiaoheng/CLionProjects/paimon-cpp
c++ -std=c++17 -Iinclude -fsyntax-only -include paimon/common/lookup/vortex/vortex_sst_codec.h -x c++ /dev/null
```

## 下一步优先级

建议下一步直接实现 Phase 1：

1. 在 Rust 中新增 `LeafBlockBuilder`。
2. 支持 numeric key column。
3. 支持 string prefix column。
4. 输出真实 block bytes。
5. 增加 Rust 单测验证 binary search。
6. 再补 C header 和 C++ wrapper。

不要先做完整 CMake 接入。先让 Rust block codec 单独稳定，再接 C++ build system。
