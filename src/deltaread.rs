use deltalake::datafusion::execution::context::SessionContext;
use deltalake::open_table;
use std::sync::Arc;

pub async fn query_1_delta(sql: &str) {
    let ctx = SessionContext::new();
    let table = open_table("./output3.parquet").await.unwrap();
    ctx.register_table("lineitem", Arc::new(table)).unwrap();

    let df = ctx.sql(sql).await.expect("Failed to execute query");
    let results = df.collect().await.expect("Failed to collect results");

    // Print results
    for batch in results {
        println!("{:?}", batch);
    }
}
