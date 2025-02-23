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

criterion_group!(benches, sum_benchmark);
criterion_main!(benches);

