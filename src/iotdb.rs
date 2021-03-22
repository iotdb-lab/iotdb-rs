#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TSDataType {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    TEXT = 5,
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

pub struct Field {
    data_type: TSDataType,
    bool_value: Option<TSDataType>,
    int_value: Option<TSDataType>,
    long_value: Option<TSDataType>,
    float_value: Option<TSDataType>,
    double_value: Option<TSDataType>,
    binary_value: Option<TSDataType>,
}

impl Field {
    pub fn new(data_type: TSDataType) -> Self {
        let mut bool_value: Option<TSDataType> = None;
        let mut int_value: Option<TSDataType> = None;
        let mut long_value: Option<TSDataType> = None;
        let mut float_value: Option<TSDataType> = None;
        let mut double_value: Option<TSDataType> = None;
        let mut binary_value: Option<TSDataType> = None;

        match data_type {
            TSDataType::BOOLEAN => bool_value = Some(TSDataType::BOOLEAN),
            TSDataType::INT32 => int_value = Some(TSDataType::INT32),
            TSDataType::INT64 => long_value = Some(TSDataType::INT64),
            TSDataType::FLOAT => float_value = Some(TSDataType::FLOAT),
            TSDataType::DOUBLE => double_value = Some(TSDataType::DOUBLE),
            TSDataType::TEXT => binary_value = Some(TSDataType::TEXT),
        }

        Self {
            data_type,
            bool_value,
            int_value,
            long_value,
            float_value,
            double_value,
            binary_value,
        }
    }

    pub fn get_data_type(&self) -> TSDataType {
        self.data_type
    }

    /// TODO
    pub fn get_string_value(&self) {}

    /// TODO
    pub fn get_object_value(&self) {}

    /// TODO
    pub fn get_field(&self) {}
}

pub struct RowRecord {
    timestamp: i64,
    fields: Vec<Field>,
}

impl RowRecord {
    pub fn new(timestamp: i64, fields: Vec<Field>) -> Self {
        Self { timestamp, fields }
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

impl Tablet {
    // TODO
}
