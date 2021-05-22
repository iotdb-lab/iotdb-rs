use getset::{CopyGetters, Getters, MutGetters, Setters};

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSDataType {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    TEXT = 5,
}

impl Default for TSDataType {
    fn default() -> Self {
        TSDataType::TEXT
    }
}

impl From<i32> for TSDataType {
    fn from(value: i32) -> Self {
        match value {
            0 => TSDataType::BOOLEAN,
            1 => TSDataType::INT32,
            2 => TSDataType::INT64,
            3 => TSDataType::FLOAT,
            4 => TSDataType::DOUBLE,
            5 => TSDataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

impl Into<i32> for TSDataType {
    fn into(self) -> i32 {
        match self {
            TSDataType::BOOLEAN => 0,
            TSDataType::INT32 => 1,
            TSDataType::INT64 => 2,
            TSDataType::FLOAT => 3,
            TSDataType::DOUBLE => 4,
            TSDataType::TEXT => 5,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSEncoding {
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

impl Default for TSEncoding {
    fn default() -> Self {
        TSEncoding::PLAIN
    }
}

impl From<i32> for TSEncoding {
    fn from(value: i32) -> Self {
        match value {
            0 => TSEncoding::PLAIN,
            1 => TSEncoding::PlainDictionary,
            2 => TSEncoding::RLE,
            3 => TSEncoding::DIFF,
            4 => TSEncoding::Ts2diff,
            5 => TSEncoding::BITMAP,
            6 => TSEncoding::GorillaV1,
            7 => TSEncoding::REGULAR,
            8 => TSEncoding::GORILLA,
            _ => panic!("This '{}' encoding doesn't exist", value),
        }
    }
}

impl Into<i32> for TSEncoding {
    fn into(self) -> i32 {
        match self {
            TSEncoding::PLAIN => 0,
            TSEncoding::PlainDictionary => 1,
            TSEncoding::RLE => 2,
            TSEncoding::DIFF => 3,
            TSEncoding::Ts2diff => 4,
            TSEncoding::BITMAP => 5,
            TSEncoding::GorillaV1 => 6,
            TSEncoding::REGULAR => 7,
            TSEncoding::GORILLA => 8,
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

#[derive(Copy, Clone, Debug, Getters, Setters, MutGetters, CopyGetters)]
pub struct Field {
    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    data_type: TSDataType,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    bool_value: Option<bool>,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    int_value: Option<i32>,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    long_value: Option<i64>,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    float_value: Option<f32>,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    double_value: Option<f64>,

    #[getset(get_copy = "pub", set = "pub", get_mut = "pub")]
    binary_value: Option<u8>,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            data_type: TSDataType::default(),
            bool_value: None,
            int_value: None,
            long_value: None,
            float_value: None,
            double_value: None,
            binary_value: None,
        }
    }
}

///  Example:
///     let mut field = Field::new(TSDataType::BOOLEAN);
///     field.set_int_value(Some(10));
///     field.int_value();
impl Field {
    pub fn new(data_type: TSDataType) -> Self {
        let mut field = Field::default();

        if !TSDataType::default().eq(&data_type) {
            field.data_type = data_type
        }

        field
    }
}

#[derive(Clone, Debug, Getters, Setters, MutGetters, CopyGetters)]
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

pub struct RpcDataSet {}

impl RpcDataSet {
    // TODO
}

pub struct SessionDataSet {}

impl SessionDataSet {
    // TODO
}

pub struct Tablet {}

impl Tablet {}
