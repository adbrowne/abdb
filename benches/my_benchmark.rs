use criterion::{black_box, criterion_group, criterion_main, Criterion};

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
    arr.iter()
        .copied()
        .reduce(|a, b| a + b)
        .unwrap_or(0)
}

fn criterion_benchmark(c: &mut Criterion) {
    // Create test data
    //let data: Vec<i32> = (0..10_000).collect();
    let mut data: [i32; 10_000] = [0; 10_000];
    for i in 0..10_000 {
        data[i] = i as i32;
    }
    
    // Benchmark non vectorized sum
    c.bench_function("sum non vectorized", |b| {
        b.iter(|| sum_non_vectorized(black_box(&data)))
    });

    // Benchmark basic sum
    c.bench_function("sum basic", |b| {
        b.iter(|| sum_basic(black_box(&data)))
    });
    
    // Benchmark vectorized sum
    c.bench_function("sum vectorized", |b| {
        b.iter(|| sum_vectorized(black_box(&data)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);