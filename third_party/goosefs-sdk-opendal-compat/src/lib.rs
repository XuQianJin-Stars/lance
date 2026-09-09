//! Re-export Tencent `goosefs-sdk` 0.2.x under the crate version OpenDAL accepts.
//!
//! Apache OpenDAL's GooseFS service still depends on `goosefs-sdk = "0.1.9"`
//! (`^0.1.9` → `<0.2.0`). Patching crates.io with git `main` (now `0.2.1`) is
//! ignored by Cargo. This shim keeps the 0.1.9 version number and forwards the
//! public API from current Tencent main.

pub use goosefs_sdk_real::*;
