// Copyright 2026-present Alibaba Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Vortex-backed SST block codec surface.

pub mod block;
pub mod ffi;
pub mod leaf;

pub use ffi::VortexOwnedBytes;
pub use ffi::VortexSstLeafBuildOptions;
pub use ffi::VortexSstLookupResult;
pub use ffi::VortexSstStatusCode;
pub use ffi::vortex_sst_build_leaf_block_from_ffi;
pub use ffi::vortex_sst_free_owned_bytes;
pub use ffi::vortex_sst_lookup_leaf_block;
