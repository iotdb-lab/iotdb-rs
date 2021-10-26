use crate::common::DataType;
use crate::rpc::TSExecuteStatementResp;
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{DateTime, Local, TimeZone};
use log::debug;
use prettytable::Row as PrettyRow;
use prettytable::{Cell, Table};
use std::io::Cursor;

#[derive(Clone, Debug)]
pub struct Field {
    data_type: DataType,
    pub bool_value: Option<bool>,
    pub int_value: Option<i32>,
    pub long_value: Option<i64>,
    pub float_value: Option<f32>,
    pub double_value: Option<f64>,
    pub binary_value: Option<Vec<u8>>,
}

impl Field {
    pub fn new(data_type: DataType) -> Field {
        Self {
            data_type,
            bool_value: None,
            int_value: None,
            long_value: None,
            float_value: None,
            double_value: None,
            binary_value: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ValueRow {
    timestamp: i64,
    fields: Vec<Field>,
}

impl ValueRow {
    pub fn new() -> Self {
        Self {
            timestamp: 0,
            fields: vec![],
        }
    }

    pub fn timestamp(&mut self, timestamp: i64) -> &mut ValueRow {
        self.timestamp = timestamp;
        self
    }

    pub fn add_field(&mut self, field: Field) -> &mut ValueRow {
        self.fields.push(field);
        self
    }
}

#[derive(Clone, Debug)]
pub struct RecordBatch {
    columns: Vec<String>,
    values: Vec<ValueRow>,
    is_empty: bool,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            columns: vec![],
            values: vec![],
            is_empty: true,
        }
    }
}

impl RecordBatch {
    fn new(columns: Vec<String>, values: Vec<ValueRow>) -> Self {
        Self {
            columns,
            values,
            is_empty: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataSet {
    statement: String,
    session_id: i64,
    fetch_size: i32,
    query_id: i64,
    record_batch: RecordBatch,
    ignore_time_stamp: Option<bool>,
}

impl DataSet {
    pub(crate) fn new(
        statement: String,
        session_id: i64,
        fetch_size: i32,
        resp: TSExecuteStatementResp,
    ) -> DataSet {
        debug!("{:?}", resp);
        // set data_types
        let data_types: Vec<DataType> = match resp.data_type_list.clone() {
            None => vec![],
            Some(data_type_list) => {
                let mut tmp_data_types: Vec<DataType> = Vec::new();
                data_type_list.iter().for_each(|data_type| {
                    let ds_data_type = DataType::from(data_type);
                    tmp_data_types.push(ds_data_type);
                });
                tmp_data_types
            }
        };

        let record_batch = if resp.query_data_set.is_some() {
            Self::resp_to_rows(resp.clone(), data_types)
        } else {
            RecordBatch::default()
        };

        Self {
            statement,
            session_id,
            fetch_size,
            query_id: resp.query_id.clone().unwrap(),
            record_batch,
            ignore_time_stamp: resp.ignore_time_stamp.clone(),
        }
    }

    fn resp_to_rows(resp: TSExecuteStatementResp, data_types: Vec<DataType>) -> RecordBatch {
        const FLAG: i32 = 0x80;
        let query_data_set = resp.query_data_set.unwrap();
        let columns = resp.columns.unwrap();
        let bitmap_buffer = query_data_set.bitmap_list;
        let mut value_list = query_data_set.value_list;

        let mut values: Vec<ValueRow> = Vec::new();
        let mut row_num = 0;
        loop {
            let sum_len: usize = value_list.iter().map(|value| value.len()).sum();
            if sum_len == 0 {
                break;
            }

            // construct time field
            let mut value_row: ValueRow = ValueRow::new();
            let mut time = query_data_set.time.clone();
            if !time.is_empty() {
                let mut time_buffer = Cursor::new(time.drain(..8).collect::<Vec<u8>>());
                value_row.timestamp(time_buffer.read_i64::<BigEndian>().unwrap());
            }

            // construct value field
            for col_index in 0..columns.len() {
                // add a new field
                let column_name = columns[col_index].clone();
                let data_type = data_types[col_index].clone();
                let mut field = Field::new(data_type);

                // reset column name index
                let col_index = match resp.column_name_index_map.clone() {
                    None => col_index,
                    Some(column_name_index_map) => column_name_index_map
                        .get(column_name.as_str())
                        .unwrap_or(&(col_index as i32))
                        .clone() as usize,
                };

                // is null value
                let bitmap = bitmap_buffer[col_index][0] as i32;
                let is_null = ((FLAG >> (row_num % 8)) & (bitmap & 0xFF)) == 0;

                if !is_null {
                    match data_type {
                        DataType::BOOLEAN => {
                            field.bool_value = Some(value_list[col_index][0].eq(&1));
                            value_list[col_index].remove(0);
                        }
                        DataType::INT32 => {
                            let mut buffer =
                                Cursor::new(value_list[col_index].drain(..4).collect::<Vec<u8>>());
                            field.int_value = Some(buffer.read_i32::<BigEndian>().unwrap());
                        }
                        DataType::INT64 => {
                            let mut buffer =
                                Cursor::new(value_list[col_index].drain(..8).collect::<Vec<u8>>());
                            field.long_value = Some(buffer.read_i64::<BigEndian>().unwrap());
                        }
                        DataType::FLOAT => {
                            let mut buffer =
                                Cursor::new(value_list[col_index].drain(..4).collect::<Vec<u8>>());
                            field.float_value = Some(buffer.read_f32::<BigEndian>().unwrap());
                        }
                        DataType::DOUBLE => {
                            let mut buffer =
                                Cursor::new(value_list[col_index].drain(..8).collect::<Vec<u8>>());
                            field.double_value = Some(buffer.read_f64::<BigEndian>().unwrap());
                        }
                        DataType::TEXT => {
                            let mut length_buffer =
                                Cursor::new(value_list[col_index].drain(..4).collect::<Vec<u8>>());
                            let length = length_buffer.read_i32::<BigEndian>().unwrap() as usize;
                            let binary = value_list[col_index].drain(..length).collect::<Vec<u8>>();
                            field.binary_value = Some(binary);
                        }
                    }
                }
                value_row.add_field(field);
            }
            row_num = row_num + 1;
            values.push(value_row);
        }

        RecordBatch::new(columns, values)
    }

    pub fn show(&mut self) {
        let mut batch = self.record_batch.clone();
        debug!("{:?}", &batch);

        let mut table: Table = Table::new();
        if !batch.is_empty {
            let ignore_time_stamp = self.ignore_time_stamp.unwrap_or(false);

            // add col name row
            if !ignore_time_stamp {
                batch.columns.insert(0, "Time".to_string());
            }
            let mut col_name_cells: Vec<Cell> = Vec::new();
            batch
                .columns
                .iter()
                .for_each(|col_name| col_name_cells.push(cell!(col_name)));
            table.set_titles(PrettyRow::new(col_name_cells));

            // add value rows
            batch.values.iter().for_each(|row| {
                let mut value_cells: Vec<Cell> = Vec::new();
                if !ignore_time_stamp {
                    let dt: DateTime<Local> = Local.timestamp_millis(row.timestamp.clone());
                    value_cells.push(cell!(dt.to_string()));
                }

                row.fields.iter().for_each(|field| match field.data_type {
                    DataType::BOOLEAN => match field.bool_value {
                        None => value_cells.push(cell!("null")),
                        Some(bool_value) => value_cells.push(cell!(bool_value)),
                    },
                    DataType::INT32 => match field.int_value {
                        None => value_cells.push(cell!("null")),
                        Some(int_value) => value_cells.push(cell!(int_value)),
                    },
                    DataType::INT64 => match field.long_value {
                        None => value_cells.push(cell!("null")),
                        Some(long_value) => value_cells.push(cell!(long_value)),
                    },
                    DataType::FLOAT => match field.float_value {
                        None => value_cells.push(cell!("null")),
                        Some(float_value) => value_cells.push(cell!(float_value)),
                    },
                    DataType::DOUBLE => match field.double_value {
                        None => value_cells.push(cell!("null")),
                        Some(double_value) => value_cells.push(cell!(double_value)),
                    },
                    DataType::TEXT => {
                        match field.clone().binary_value {
                            None => value_cells.push(cell!("null")),
                            Some(binary) => {
                                value_cells.push(cell!(String::from_utf8(binary).unwrap()))
                            }
                        };
                    }
                });

                table.add_row(PrettyRow::new(value_cells));
            })
        }
        table.printstd();
    }
}
