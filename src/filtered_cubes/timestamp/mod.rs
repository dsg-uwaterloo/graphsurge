mod gstimestamp_1d;
#[cfg(feature = "nd-timestamps")]
mod gstimestamp_3d;
pub mod timestamp_mappings;

pub type DimensionId = u16;

#[cfg(not(feature = "nd-timestamps"))]
pub use self::gstimestamp_1d::GSTimestamp1D as GSTimestamp;
#[cfg(feature = "nd-timestamps")]
pub use self::gstimestamp_3d::GSTimestamp3D as GSTimestamp;
