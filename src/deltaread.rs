use std::sync::Arc;
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::open_table;

pub async fn query_1_delta() {
   let ctx = SessionContext::new();
   let table = open_table("./output3.parquet")
       .await
       .unwrap();
   ctx.register_table("lineitem", Arc::new(table)).unwrap();

    // Execute TPC-H Query 1
    let sql = "
        SELECT 
            l_returnflag,
            l_linestatus,
            COUNT(*) as count,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            AVG(l_quantity) as avg_qty,
            AVG(l_extendedprice) as avg_price,
            AVG(l_discount) as avg_disc
        FROM lineitem
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    ";

    let df = ctx.sql(sql).await.expect("Failed to execute query");
    let results = df.collect().await.expect("Failed to collect results");

    // Print results
    for batch in results {
        println!("{:?}", batch);
    }
}