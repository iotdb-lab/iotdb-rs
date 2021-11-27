use simplelog::*;

/// IotDB datatype enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DataType {
    BOOLEAN,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    TEXT,
}

impl From<&String> for DataType {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "BOOLEAN" => DataType::BOOLEAN,
            "INT32" => DataType::INT32,
            "INT64" => DataType::INT64,
            "FLOAT" => DataType::FLOAT,
            "DOUBLE" => DataType::DOUBLE,
            "TEXT" => DataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value {
            "BOOLEAN" => DataType::BOOLEAN,
            "INT32" => DataType::INT32,
            "INT64" => DataType::INT64,
            "FLOAT" => DataType::FLOAT,
            "DOUBLE" => DataType::DOUBLE,
            "TEXT" => DataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for DataType {
    fn into(self) -> i32 {
        match self {
            DataType::BOOLEAN => 0,
            DataType::INT32 => 1,
            DataType::INT64 => 2,
            DataType::FLOAT => 3,
            DataType::DOUBLE => 4,
            DataType::TEXT => 5,
        }
    }
}

/// IotDB encoding enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Encoding {
    PLAIN,
    PlainDictionary,
    RLE,
    DIFF,
    Ts2diff,
    BITMAP,
    GorillaV1,
    REGULAR,
    GORILLA,
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::PLAIN
    }
}

impl From<i32> for Encoding {
    fn from(value: i32) -> Self {
        match value {
            0 => Encoding::PLAIN,
            1 => Encoding::PlainDictionary,
            2 => Encoding::RLE,
            3 => Encoding::DIFF,
            4 => Encoding::Ts2diff,
            5 => Encoding::BITMAP,
            6 => Encoding::GorillaV1,
            7 => Encoding::REGULAR,
            8 => Encoding::GORILLA,
            _ => panic!("This '{}' encoding doesn't exist", value),
        }
    }
}

impl From<String> for Encoding {
    fn from(value: String) -> Self {
        match value.as_str() {
            "PLAIN" => Encoding::PLAIN,
            "PlainDictionary" => Encoding::PlainDictionary,
            "RLE" => Encoding::RLE,
            "DIFF" => Encoding::DIFF,
            "Ts2diff" => Encoding::Ts2diff,
            "BITMAP" => Encoding::BITMAP,
            "GorillaV1" => Encoding::GorillaV1,
            "REGULAR" => Encoding::REGULAR,
            "GORILLA" => Encoding::GORILLA,
            _ => panic!("This '{}' encoding doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for Encoding {
    fn into(self) -> i32 {
        match self {
            Encoding::PLAIN => 0,
            Encoding::PlainDictionary => 1,
            Encoding::RLE => 2,
            Encoding::DIFF => 3,
            Encoding::Ts2diff => 4,
            Encoding::BITMAP => 5,
            Encoding::GorillaV1 => 6,
            Encoding::REGULAR => 7,
            Encoding::GORILLA => 8,
        }
    }
}

/// IotDB compressor enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Compressor {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    SDT,
    PAA,
    PLA,
    LZ4,
}

impl Default for Compressor {
    fn default() -> Self {
        Compressor::SNAPPY
    }
}

impl From<i32> for Compressor {
    fn from(value: i32) -> Self {
        match value {
            0 => Compressor::UNCOMPRESSED,
            1 => Compressor::SNAPPY,
            2 => Compressor::GZIP,
            3 => Compressor::LZO,
            4 => Compressor::SDT,
            5 => Compressor::PAA,
            6 => Compressor::PLA,
            7 => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

impl From<&str> for Compressor {
    fn from(value: &str) -> Self {
        match value {
            "UNCOMPRESSED" => Compressor::UNCOMPRESSED,
            "SNAPPY" => Compressor::SNAPPY,
            "GZIP" => Compressor::GZIP,
            "LZO" => Compressor::LZO,
            "SDT" => Compressor::SDT,
            "PAA" => Compressor::PAA,
            "PLA" => Compressor::PLA,
            "LZ4" => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

impl From<String> for Compressor {
    fn from(value: String) -> Self {
        match value.as_str() {
            "UNCOMPRESSED" => Compressor::UNCOMPRESSED,
            "SNAPPY" => Compressor::SNAPPY,
            "GZIP" => Compressor::GZIP,
            "LZO" => Compressor::LZO,
            "SDT" => Compressor::SDT,
            "PAA" => Compressor::PAA,
            "PLA" => Compressor::PLA,
            "LZ4" => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for Compressor {
    fn into(self) -> i32 {
        match self {
            Compressor::UNCOMPRESSED => 0,
            Compressor::SNAPPY => 1,
            Compressor::GZIP => 2,
            Compressor::LZO => 3,
            Compressor::SDT => 4,
            Compressor::PAA => 5,
            Compressor::PLA => 6,
            Compressor::LZ4 => 7,
        }
    }
}

/// A tiny Logger
pub struct Logger {}

impl Logger {
    pub fn init(level: LevelFilter) {
        let logger = CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Info,
            Default::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap();
        CombinedLogger::init(vec![TermLogger::new(
            level,
            Default::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap_or(logger);
    }
}
