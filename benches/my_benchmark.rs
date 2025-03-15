use abdb::{string_column::StringColumnReader, write_batch, LineItem, TrackedWriter};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::arch::aarch64::*;

// Basic sum implementation
fn sum_basic(arr: &[i32]) -> i32 {
    arr.iter().sum()
}

fn sum_non_vectorized(arr: &[i32]) -> i32 {
    let mut sum = 0;
    for &x in arr {
        // Conditional logic breaks vectorization
        if x > 0 {
            sum += x;
        } else {
            sum -= x;
        }
    }
    sum
}

// Vectorization-friendly implementation
fn sum_vectorized(arr: &[i32]) -> i32 {
    arr.iter().copied().reduce(|a, b| a + b).unwrap_or(0)
}


#[target_feature(enable = "neon")]
pub unsafe fn sum_intrinsics_optimized(arr: &[i32]) -> i32 {
    let len = arr.len();
    let mut sum = 0;

    // Process 16 integers at a time using 4 accumulators
    let chunk_size = 16;
    let chunks = len / chunk_size;
    
    if chunks > 0 {
        // Initialize 4 accumulator vectors
        let mut acc1 = vdupq_n_s32(0);
        let mut acc2 = vdupq_n_s32(0);
        let mut acc3 = vdupq_n_s32(0);
        let mut acc4 = vdupq_n_s32(0);
        
        // Process chunks of 16 integers
        for i in 0..chunks {
            let base_ptr = arr.as_ptr().add(i * chunk_size);
            
            // Load 4 sets of 4 integers
            let chunk1 = vld1q_s32(base_ptr);
            let chunk2 = vld1q_s32(base_ptr.add(4));
            let chunk3 = vld1q_s32(base_ptr.add(8));
            let chunk4 = vld1q_s32(base_ptr.add(12));
            
            // Add to respective accumulators
            acc1 = vaddq_s32(acc1, chunk1);
            acc2 = vaddq_s32(acc2, chunk2);
            acc3 = vaddq_s32(acc3, chunk3);
            acc4 = vaddq_s32(acc4, chunk4);
        }
        
        // Combine accumulators
        let mut temp = [0i32; 16];
        vst1q_s32(temp.as_mut_ptr(), acc1);
        vst1q_s32(temp.as_mut_ptr().add(4), acc2);
        vst1q_s32(temp.as_mut_ptr().add(8), acc3);
        vst1q_s32(temp.as_mut_ptr().add(12), acc4);
        sum += temp.iter().sum::<i32>();
    }
    
    // Handle remaining elements
    for i in (chunks * chunk_size)..len {
        sum += arr[i];
    }
    
    sum
}

fn write_column_data() {
    let file = std::fs::File::create("lineitems_column_criterion.bin").expect("Failed to create file");
    let mut writer = TrackedWriter::new(std::io::BufWriter::new(file));
    for _ in 0..10 {
        let mut batch = vec![
            LineItem {
                l_returnflag: "A".to_string(),
                l_linestatus: "B".to_string(),
                l_quantity: 1.0,
                l_extendedprice: 2.0,
                l_discount: 3.0,
                l_tax: 4.0,
            },
            LineItem {
                l_returnflag: "C".to_string(),
                l_linestatus: "D".to_string(),
                l_quantity: 5.0,
                l_extendedprice: 6.0,
                l_discount: 7.0,
                l_tax: 8.0,
            },
        ];
        batch.extend(std::iter::repeat(batch.clone()).take(3999).flatten());
        write_batch(&mut writer, &mut batch);
    }
}

fn query_1_column() -> Vec<Option<abdb::QueryOneStateColumn>> {
    abdb::query_1_column("lineitems_column_criterion.bin")
}

fn sum_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sum array");
    // Create test data
    //let data: Vec<i32> = (0..10_000).collect();
    let mut data: [i32; 10_000] = [0; 10_000];
    for i in 0..10_000 {
        data[i] = i as i32;
    }

    // Benchmark non vectorized sum
    group.bench_function("sum non vectorized", |b| {
        b.iter(|| black_box(sum_non_vectorized(black_box(&data))))
    });

    // Benchmark basic sum
    group.bench_function("sum basic", |b| b.iter(|| black_box(sum_basic(black_box(&data)))));

    // Benchmark vectorized sum
    group.bench_function("sum vectorized", |b| {
        b.iter(|| black_box(sum_vectorized(black_box(&data))))
    });

    // Benchmark vectorized sum intrinsics
    group.bench_function("sum vectorized intrinsics", |b| {
        unsafe {
            b.iter(|| black_box(sum_intrinsics_optimized(black_box(&data))));
        }
    });

    group.finish();

}

fn group_by_sum_benchmark(c: &mut Criterion) {
    write_column_data();
    c.bench_function("query_1_column", |b| b.iter(|| black_box(query_1_column())));
}

fn write_string_column() {
    let file = std::fs::File::create("string_column_criterion.bin").expect("Failed to create file");
    let mut writer = TrackedWriter::new(std::io::BufWriter::new(file));
    let data = vec!["a", "a", "b", "b", "b", "c"].repeat(1000);

    for _ in 0..99 {  // 1 time already called above, so 99 more
        let col = StringColumnReader::new_from_strings(data.clone());
        col.write(&mut writer);
    }
}

fn read_all_strings() -> u64 {
    let file = std::fs::File::open("string_column_criterion.bin").expect("Failed to open file");
    let mut reader = std::io::BufReader::new(file);
    let mut col = StringColumnReader::empty();
    let mut count = 0;
    for _ in 0..99 {
        col.read(&mut reader);
        count += col.count_strings(); // Using count() to get the number of strings
    }
    count
}

fn read_and_write_strings_benchmark(c: &mut Criterion) {
    write_string_column();
    c.bench_function("read_string_column", |b| b.iter(|| black_box(read_all_strings())));
}

criterion_group!(benches, sum_benchmark, group_by_sum_benchmark, read_and_write_strings_benchmark);
criterion_main!(benches);