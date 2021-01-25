use crate::rpc::TSExecuteStatementResp;
use prettytable::{Cell, Row, Table};

pub fn result_set(resp: TSExecuteStatementResp) {
    let mut table = Table::new();

    // Add Columns
    let mut cells: Vec<Cell> = vec![];
    for cell in resp.columns.unwrap() {
        cells.push(Cell::new(cell.as_str()))
    }
    table.add_row(Row::new(cells));

    //TODO Add values rows

    // Print the table to stdout
    table.printstd();
}
