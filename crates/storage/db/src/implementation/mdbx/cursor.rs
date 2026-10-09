//! Dispatch between native MDBX cursors and explicitly selected packed-state cursors.

pub use super::{
    native_cursor::decode,
    routing_cursor::{Cursor, CursorRO, CursorRW},
};
