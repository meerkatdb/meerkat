#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int32Stats {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Int64Stats {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Float64Stats {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringStats {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnMeta {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "ColumnType", tag = "2")]
    pub column_type: i32,
    #[prost(bool, tag = "3")]
    pub nullable: bool,
    #[prost(enumeration = "Encoding", tag = "4")]
    pub encoding: i32,
    #[prost(uint32, tag = "8")]
    pub cardinality: u32,
    #[prost(enumeration = "IndexType", tag = "5")]
    pub index_type: i32,
    #[prost(uint32, tag = "6")]
    pub num_values: u32,
    #[prost(uint32, tag = "7")]
    pub num_nulls: u32,
    #[prost(uint64, tag = "9")]
    pub compressed_size: u64,
    #[prost(uint64, tag = "10")]
    pub uncompressed_size: u64,
    #[prost(oneof = "column_meta::Stats", tags = "11, 12, 13, 14")]
    pub stats: ::core::option::Option<column_meta::Stats>,
}
/// Nested message and enum types in `ColumnMeta`.
pub mod column_meta {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Stats {
        #[prost(message, tag = "11")]
        Int32Stats(super::Int32Stats),
        #[prost(message, tag = "12")]
        Int64Stats(super::Int64Stats),
        #[prost(message, tag = "13")]
        Float64Stats(super::Float64Stats),
        #[prost(message, tag = "14")]
        StringStats(super::StringStats),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SegmentMeta {
    #[prost(bytes = "vec", tag = "1")]
    pub segment_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "2")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub shard_id: u32,
    #[prost(uint64, tag = "5")]
    pub compressed_size: u64,
    #[prost(uint64, tag = "6")]
    pub uncompressed_size: u64,
    #[prost(uint32, tag = "7")]
    pub num_rows: u32,
    #[prost(map = "string, message", tag = "8")]
    pub columns_meta: ::std::collections::HashMap<::prost::alloc::string::String, ColumnMeta>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Range {
    #[prost(uint64, tag = "1")]
    pub start: u64,
    #[prost(uint64, tag = "2")]
    pub end: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    #[prost(message, optional, tag = "1")]
    pub segment_meta: ::core::option::Option<SegmentMeta>,
    #[prost(map = "string, message", tag = "2")]
    pub columns_layout: ::std::collections::HashMap<::prost::alloc::string::String, ColumnLayout>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringIndexV1 {
    #[prost(message, optional, tag = "1")]
    pub range: ::core::option::Option<Range>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockIndexLayout {
    #[prost(message, optional, tag = "1")]
    pub range: ::core::option::Option<Range>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryRleDict {
    #[prost(message, optional, tag = "1")]
    pub range: ::core::option::Option<Range>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NoIndex {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NoLayout {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnLayout {
    #[prost(message, optional, tag = "3")]
    pub block_index_layout: ::core::option::Option<BlockIndexLayout>,
    #[prost(oneof = "column_layout::ColumnIndexLayout", tags = "1, 2")]
    pub column_index_layout: ::core::option::Option<column_layout::ColumnIndexLayout>,
    #[prost(oneof = "column_layout::EncoderLayout", tags = "4, 5, 6")]
    pub encoder_layout: ::core::option::Option<column_layout::EncoderLayout>,
}
/// Nested message and enum types in `ColumnLayout`.
pub mod column_layout {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ColumnIndexLayout {
        #[prost(message, tag = "1")]
        NoIndex(super::NoIndex),
        #[prost(message, tag = "2")]
        StringIndexV1(super::StringIndexV1),
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum EncoderLayout {
        #[prost(message, tag = "4")]
        Plain(super::NoLayout),
        #[prost(message, tag = "5")]
        BinaryDict(super::BinaryRleDict),
        #[prost(message, tag = "6")]
        Snappy(super::NoLayout),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ColumnType {
    Bool = 0,
    Datetime = 1,
    Decimal = 2,
    Dynamic = 3,
    Float64 = 4,
    Guid = 5,
    Int32 = 6,
    Int64 = 7,
    String = 8,
    Timespan = 9,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Encoding {
    Plain = 0,
    BitPacked = 1,
    DeltaBinaryPacked = 2,
    RleDictionary = 3,
    Snappy = 4,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IndexType {
    NoIndex = 0,
    FullText = 1,
    Brin = 2,
    Kdtree = 3,
}
