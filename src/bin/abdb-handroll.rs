use abdb::{print_state_column, query_1_column};
fn main() {
    let result = query_1_column("lineitems_column.bin");
    print_state_column(result);
}
