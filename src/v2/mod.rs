#![allow(dead_code)]

//! longnamefs v2 模块骨架。
//!
//! 目标：基于 `refine-fs-plan.md` 的设计，后续在这里实现
//! xattr + 索引版的长文件名映射。

pub mod error;
pub mod fs;
pub mod index;
pub mod path;
