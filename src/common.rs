#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DataType {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    TEXT = 5,
}

impl Default for DataType {
    fn default() -> Self {
        DataType::TEXT
    }
}

impl From<i32> for DataType {
    fn from(value: i32) -> Self {
        match value {
            0 => DataType::BOOLEAN,
            1 => DataType::INT32,
            2 => DataType::INT64,
            3 => DataType::FLOAT,
            4 => DataType::DOUBLE,
            5 => DataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

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

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Encoding {
    PLAIN = 0,
    PlainDictionary = 1,
    RLE = 2,
    DIFF = 3,
    Ts2diff = 4,
    BITMAP = 5,
    GorillaV1 = 6,
    REGULAR = 7,
    GORILLA = 8,
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

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Compressor {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    SDT = 4,
    PAA = 5,
    PLA = 6,
    LZ4 = 7,
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

#[derive(Copy, Clone, Debug)]
pub struct Field {
    data_type: DataType,
    bool_value: Option<bool>,
    int_value: Option<i32>,
    long_value: Option<i64>,
    float_value: Option<f32>,
    double_value: Option<f64>,
    binary_value: Option<u8>,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            data_type: DataType::default(),
            bool_value: None,
            int_value: None,
            long_value: None,
            float_value: None,
            double_value: None,
            binary_value: None,
        }
    }
}

impl Field {
    pub fn new(data_type: DataType) -> Self {
        let mut field = Field::default();

        if !DataType::default().eq(&data_type) {
            field.data_type = data_type
        }

        field
    }
}

#[derive(Clone, Debug)]
pub struct RowRecord {
    timestamp: i64,
    fields: Vec<Field>,
}

impl RowRecord {
    pub fn new(timestamp: i64, fields: Vec<Field>) -> Self {
        Self { timestamp, fields }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field)
    }
}

pub struct Tablet {}

impl Tablet {}

use fern::Dispatch;
use std::io;
use std::path::PathBuf;

/// Logger
pub struct Logger {
    level: String,
    log_path: Option<PathBuf>,
}

impl Default for Logger {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            log_path: None,
        }
    }
}

impl Logger {
    pub fn new(level: &str, log_path: Option<PathBuf>) -> Self {
        Self {
            level: level.to_string(),
            log_path,
        }
    }

    pub fn init(&mut self) -> Result<Logger, fern::InitError> {
        let mut base_config = fern::Dispatch::new();
        let log_path = self.log_path.clone();

        base_config = match self.level.as_str() {
            "trace" => base_config
                .level(log::LevelFilter::Trace)
                .level_for("overly-verbose-target", log::LevelFilter::Trace),
            "debug" => base_config
                .level(log::LevelFilter::Debug)
                .level_for("overly-verbose-target", log::LevelFilter::Debug),
            "info" => base_config
                .level(log::LevelFilter::Info)
                .level_for("overly-verbose-target", log::LevelFilter::Info),
            "warn" => base_config
                .level(log::LevelFilter::Warn)
                .level_for("overly-verbose-target", log::LevelFilter::Warn),
            "error" => base_config
                .level(log::LevelFilter::Error)
                .level_for("overly-verbose-target", log::LevelFilter::Error),
            _ => base_config
                .level(log::LevelFilter::Error)
                .level_for("overly-verbose-target", log::LevelFilter::Error),
        };

        // Separate file config so we can include year, month and day in file logs
        let file_config: Dispatch = match log_path.clone() {
            None => fern::Dispatch::new(),
            Some(path_buf) => fern::Dispatch::new()
                .format(|out, message, record| {
                    out.finish(format_args!(
                        "{}[{}][{}] {}",
                        chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                        record.target(),
                        record.level(),
                        message
                    ))
                })
                .chain(fern::log_file(path_buf.as_path()).unwrap()),
        };

        let stdout_config = fern::Dispatch::new()
            .format(|out, message, record| {
                if record.level() > log::LevelFilter::Info && record.target() == "cmd_program" {
                    out.finish(format_args!(
                        "---\nDEBUG: {}: {}\n---",
                        chrono::Local::now().format("%H:%M:%S"),
                        message
                    ))
                } else {
                    out.finish(format_args!(
                        "[{}][{}][{}] {}",
                        chrono::Local::now().format("%H:%M"),
                        record.target(),
                        record.level(),
                        message
                    ))
                }
            })
            .chain(io::stdout());

        base_config
            .chain(file_config)
            .chain(stdout_config)
            .apply()?;

        Ok(Self {
            level: self.level.clone(),
            log_path,
        })
    }
}
