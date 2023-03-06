#![allow(unused)]

use criterion::measurement::WallTime;
use criterion::BatchSize;
use criterion::Bencher;
use criterion::BenchmarkGroup;
use kvs::SledKvsEngine;
use rand::rngs::ThreadRng;
use rand::{Rng, SeedableRng};
use std::convert::TryFrom;
use tempfile::TempDir;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kvs::KvStore;
use kvs::KvsEngine;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use tempfile::tempdir;

pub fn test_writes<Engine: KvsEngine>(b: &mut Bencher, thread_rng: ThreadRng) {
    let mut rng = SmallRng::from_rng(thread_rng).unwrap();
    let temp_dir = tempfile::TempDir::new().unwrap().into_path();
    let mut store = Engine::open(temp_dir).unwrap();

    b.iter(|| {
        store
            .set(format!("key{}", rng.gen::<u32>()), "value".to_string())
            .unwrap();
    });
}

pub fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes");
    group.sample_size(100);
    let thread_rng = rand::thread_rng();

    group.bench_function("kvs", |b| test_writes::<KvStore>(b, thread_rng.clone()));

    group.bench_function("sled", |b| {
        test_writes::<SledKvsEngine>(b, thread_rng.clone())
    });
}

pub fn test_reads<Engine: KvsEngine>(b: &mut Bencher, thread_rng: ThreadRng) {
    let mut rng = SmallRng::from_rng(thread_rng).unwrap();
    let temp_dir = TempDir::new().unwrap().into_path();
    let mut store = Engine::open(temp_dir).unwrap();
    let key_count = 1 << 8;

    for key_i in 1..key_count {
        store
            .set(format!("key{}", key_i), "value".to_string())
            .unwrap();
    }

    b.iter(|| {
        store
            .get(format!("key{}", rng.gen_range(1..key_count)))
            .unwrap();
    })
}

pub fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("reads");
    group.sample_size(1000);
    let thread_rng = rand::thread_rng();

    group.bench_function("kvs", |b| test_reads::<KvStore>(b, thread_rng.clone()));
    group.bench_function("sled", |b| {
        test_reads::<SledKvsEngine>(b, thread_rng.clone())
    });
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);
