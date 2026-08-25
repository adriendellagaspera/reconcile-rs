// The benchmark drives the range-fingerprint via `FingerprintTreeMap::aggregate`, which is public
// on the standalone `rsos` crate — so, unlike when it went through the gated `reconcile::testing`
// seam, the bench body needs no feature gate at all.
use imp::main;

// `service_reconcile_rtt` below composes `just_insert`/`just_remove` (`reconcile_internal_testing`
// seams, AGENTS.md §6) with the injected-RTT decorator, so it lives here rather than in
// `system.rs`, which is deliberately feature-gate-free (`benches/README.md` "Pricing that
// end-to-end...").

mod imp {
    use std::collections::BTreeMap;
    use std::hint::black_box;
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use rand::{distributions::Standard, Rng, SeedableRng};

    use criterion::{
        criterion_group, AxisScale, BenchmarkId, Criterion, PlotConfiguration, SamplingMode,
        Throughput,
    };

    use tokio_util::sync::CancellationToken;

    use reconcile::{
        replicated_map::Config, Entry, FingerprintTreeMap, Hlc, InMemoryNetwork, LogicalCounter,
        NodeId, PhysicalTime, ReplicatedMap, State, Timestamp, Transport,
    };

    use gossip::netem::{Link, Netem, NetemTransport, Rtt, Seed};

    fn fingerprint_tree_map_new(c: &mut Criterion) {
        let mut group = c.benchmark_group("FingerprintTreeMap::new");
        group.bench_function("BTreeMap::new()", |b| b.iter(BTreeMap::<u32, u32>::new));
        group.bench_function("FingerprintTreeMap::new()", |b| {
            b.iter(FingerprintTreeMap::<u32, u32>::new)
        });
    }

    fn fingerprint_tree_map_fill(c: &mut Criterion) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let mut key_values = Vec::new();
        for _ in 0..1_000_000 {
            let key: u32 = rng.gen();
            let value: u32 = rng.gen();
            key_values.push((key, value));
        }
        let key_values = &key_values;

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("FingerprintTreeMap::fill");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.throughput(Throughput::Elements(size as u64));
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(
                BenchmarkId::new("BTreeMap::fill", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut tree = BTreeMap::<u32, u32>::new();
                        for (k, v) in key_values[..size].iter().copied() {
                            tree.insert(k, v);
                        }
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("FingerprintTreeMap::fill", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut tree = FingerprintTreeMap::<u32, u32>::new();
                        for (k, v) in key_values[..size].iter().copied() {
                            tree.insert(k, v);
                        }
                    })
                },
            );
            size *= 10;
        }
    }

    fn fingerprint_tree_map_insert(c: &mut Criterion) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let mut key_values = Vec::new();
        for _ in 0..1_000_000 {
            let key: u32 = rng.gen();
            let value: u32 = rng.gen();
            key_values.push((key, value));
        }
        let key_values = &key_values;

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("FingerprintTreeMap::insert");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.throughput(Throughput::Elements(size as u64));
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(
                BenchmarkId::new("BTreeMap::insert", size),
                &size,
                |b, &size| {
                    let mut tree = BTreeMap::<u32, u32>::new();
                    for (k, v) in key_values[..size].iter().copied() {
                        tree.insert(k, v);
                    }
                    b.iter(|| {
                        // NOTE: do the insertion first because inserting a just-removed element is
                        // likely easier; do not reuse the same key, since it was just removed during
                        // the last iteration
                        let k = rng.gen();
                        let v = rng.gen();
                        tree.insert(k, v);
                        tree.remove(&k);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("FingerprintTreeMap::insert", size),
                &size,
                |b, &size| {
                    let mut tree = FingerprintTreeMap::<u32, u32>::new();
                    for (k, v) in key_values[..size].iter().copied() {
                        tree.insert(k, v);
                    }
                    b.iter(|| {
                        // NOTE: do the insertion first because inserting a just-removed element is
                        // likely easier; do not reuse the same key, since it was just removed during
                        // the last iteration
                        let k = rng.gen();
                        let v = rng.gen();
                        tree.insert(k, v);
                        tree.remove(&k);
                    })
                },
            );
            size *= 10;
        }
    }

    fn fingerprint_tree_map_remove(c: &mut Criterion) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let mut key_values = Vec::new();
        for _ in 0..1_000_000 {
            let key: u32 = rng.gen();
            let value: u32 = rng.gen();
            key_values.push((key, value));
        }
        let key_values = &key_values;

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("FingerprintTreeMap::remove");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.throughput(Throughput::Elements(size as u64));
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(
                BenchmarkId::new("BTreeMap::remove", size),
                &size,
                |b, &size| {
                    let mut tree = BTreeMap::<u32, u32>::new();
                    for (k, v) in key_values[..size].iter().copied() {
                        tree.insert(k, v);
                    }
                    b.iter(|| {
                        // NOTE: do the removal first because removing a just-inserted element is
                        // likely easier; do not reuse the same key, since it was just reinserted
                        // during the last iteration
                        let idx = rng.gen_range(0..size);
                        let (k, v) = &key_values[idx];
                        tree.remove(k);
                        tree.insert(*k, *v);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("FingerprintTreeMap::remove", size),
                &size,
                |b, &size| {
                    let mut tree = FingerprintTreeMap::<u32, u32>::new();
                    for (k, v) in key_values[..size].iter().copied() {
                        tree.insert(k, v);
                    }
                    b.iter(|| {
                        // NOTE: do the removal first because removing a just-inserted element is
                        // likely easier; do not reuse the same key, since it was just reinserted
                        // during the last iteration
                        let idx = rng.gen_range(0..size);
                        let (k, v) = &key_values[idx];
                        tree.remove(k);
                        tree.insert(*k, *v);
                    })
                },
            );
            size *= 10;
        }
    }

    fn fingerprint_tree_map_range_fingerprint(c: &mut Criterion) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        let mut key_values = Vec::new();
        for _ in 0..1_000_000 {
            let key: u32 = rng.gen();
            let value: u32 = rng.gen();
            key_values.push((key, value));
        }
        let key_values = &key_values;

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("FingerprintTreeMap::aggregate");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                let mut tree = FingerprintTreeMap::<u32, u32>::new();
                for (k, v) in key_values[..size].iter().copied() {
                    tree.insert(k, v);
                }
                b.iter(|| {
                    let k1: u32 = rng.gen();
                    let k2: u32 = rng.gen();
                    let range = if k1 < k2 { k1..k2 } else { k2..k1 };
                    tree.aggregate(range);
                })
            });
            size *= 10;
        }
    }

    /// In-memory cost of a dated replica (`FingerprintTreeMap<K, Entry<Timestamp, V>>`) against
    /// the value-only one (`FingerprintTreeMap<K, State<V>>`).
    ///
    /// Criterion times the fill at growing sizes; the report below adds bytes per entry.
    fn read_replica_memory(c: &mut Criterion) {
        let dated = std::mem::size_of::<Entry<Timestamp, u32>>();
        let light = std::mem::size_of::<State<u32>>();
        println!(
            "[read replica memory] per-entry value size: dated Entry<Timestamp, u32> = {dated} B, \
         value-only State<u32> = {light} B, saved = {} B/entry",
            dated - light
        );

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut keys = Vec::new();
        for _ in 0..1_000_000 {
            keys.push(rng.gen::<u32>());
        }
        let keys = &keys;

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("read_replica_memory::fill");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= keys.len() {
            group.throughput(Throughput::Elements(size as u64));
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(
                BenchmarkId::new("dated Entry<Timestamp, u32>", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut tree = FingerprintTreeMap::<u32, Entry<Timestamp, u32>>::new();
                        for &k in keys[..size].iter() {
                            tree.insert(
                                k,
                                Entry::present(
                                    Timestamp::new(
                                        Hlc::new(
                                            PhysicalTime::from_millis(k as u64),
                                            LogicalCounter::new(0),
                                        ),
                                        NodeId::new(0),
                                    ),
                                    k,
                                ),
                            );
                        }
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("value-only State<u32>", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut tree = FingerprintTreeMap::<u32, State<u32>>::new();
                        for &k in keys[..size].iter() {
                            tree.insert(k, State::Present(k));
                        }
                    })
                },
            );
            size *= 10;
        }
    }

    /// Dataset sizes for `bulk_load_just_insert`, matching `system.rs`'s `bulk_load`/`point_read`
    /// default sweep — duplicated rather than shared for the reason `rtt_sweep` above states (each
    /// bench binary is a separate compilation unit). Extendable the same way, via
    /// `RECONCILE_BENCH_SIZES` (`benches/README.md`).
    fn bulk_load_sizes() -> Vec<usize> {
        match std::env::var("RECONCILE_BENCH_SIZES") {
            Ok(v) => v
                .split(',')
                .map(|s| {
                    s.trim()
                        .parse()
                        .expect("RECONCILE_BENCH_SIZES: not a list of usize")
                })
                .collect(),
            Err(_) => vec![10, 100, 1_000, 10_000, 100_000],
        }
    }

    /// Deterministic `(key, value)` corpus, identical to `system.rs`'s `corpus` — duplicated for
    /// the same cross-binary reason as [`bulk_load_sizes`].
    fn just_insert_corpus(n: usize) -> Vec<(u32, u32)> {
        (0..n as u32)
            .map(|k| (k, k.wrapping_mul(2_654_435_761)))
            .collect()
    }

    /// Cycles through `20_000..60_000` rather than a raw `fetch_add` on a `u16` counter, matching
    /// `system.rs`'s `next_bench_port` (its own docs explain why: enough warm-up iterations
    /// overflow a bare `u16` and hand `Config::port` a wrapped `0`, which it rejects).
    fn next_just_insert_port() -> u16 {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let i = NEXT.fetch_add(1, Ordering::Relaxed);
        20_000 + (i % 40_000) as u16
    }

    /// Per-entry `just_insert` throughput — #51's own external-prototype metric (`just_insert` of
    /// `String -> Vec<u8>`, "~190k-430k inserts/s") gets an in-repo counterpart at last. `just_insert`
    /// is local-only, no broadcast (`src/replicated_map/write.rs`'s own docs), and a
    /// `reconcile_internal_testing` seam (AGENTS.md §6) unreachable from the feature-gate-free
    /// `system.rs` — hence living here, next to `service_reconcile_rtt`'s own `just_insert` use.
    /// Isolates the per-call cost of inserting one entry at a time, fingerprint maintenance and
    /// all, from `bulk_load`'s (`system.rs`) `insert_bulk`, which amortises setup across the whole
    /// batch — both #51's metric and the in-repo bulk-write path, at the same sizes, side by side.
    fn bulk_load_just_insert(c: &mut Criterion) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut group = c.benchmark_group("bulk_load_just_insert");
        group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
        for size in bulk_load_sizes() {
            let kvs = just_insert_corpus(size);
            group.throughput(Throughput::Elements(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            ReplicatedMap::<u32, u32>::new(
                                Config::default()
                                    .with_port(next_just_insert_port())
                                    .with_listen_addr("127.0.0.1".parse().unwrap())
                                    .with_net("127.0.0.1/8".parse().unwrap())
                                    .with_insecure_no_key(),
                            )
                            .await
                            .expect("bind failed")
                        })
                    },
                    |store| {
                        for &(k, v) in &kvs {
                            black_box(store.just_insert(k, v));
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
        group.finish();
    }

    fn service_send(c: &mut Criterion) {
        let port = 8080;
        let net = "127.0.0.1/8".parse().unwrap();
        let addr1 = "127.0.0.44".parse().unwrap();
        let addr2 = "127.0.0.45".parse().unwrap();
        let cfg1 = Config::default()
            .with_port(port)
            .with_listen_addr(addr1)
            .with_net(net)
            .with_insecure_no_key();
        let cfg2 = Config::default()
            .with_port(port)
            .with_listen_addr(addr2)
            .with_net(net)
            .with_insecure_no_key();

        let mut rng = rand::rngs::ThreadRng::default();

        let key_values: Vec<(u32, u32)> =
            (&mut rng).sample_iter(Standard).take(1_000_000).collect();

        let rt = tokio::runtime::Runtime::new().unwrap();

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("ReplicatedMap::send");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                rt.block_on(async {
                    let store1 = ReplicatedMap::new(cfg1.clone())
                        .await
                        .expect("bind failed")
                        .with_seed(addr2);
                    store1.insert_bulk(&key_values[..size]);
                    let store2 = ReplicatedMap::new(cfg2.clone())
                        .await
                        .expect("bind failed")
                        .with_seed(addr1);
                    store2.insert_bulk(&key_values[..size]);
                    let task1 = tokio::spawn(store1.clone().run(CancellationToken::new()));
                    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));

                    b.iter(|| {
                        let k: u32 = rng.gen();
                        let v: u32 = rng.gen();
                        store1.insert(k, v);
                        while store2.get(&k).is_none() {
                            std::thread::sleep(Duration::from_micros(1));
                        }
                        store1.remove(&k);
                        while store2.get(&k).is_some() {
                            std::thread::sleep(Duration::from_micros(1));
                        }
                    });

                    task2.abort();
                    task1.abort();
                    let _ = tokio::join!(task1, task2);
                })
            });
            size *= 10;
        }
    }

    fn service_reconcile(c: &mut Criterion) {
        let port = 8080;
        let net = "127.0.0.1/8".parse().unwrap();
        let addr1 = "127.0.0.44".parse().unwrap();
        let addr2 = "127.0.0.45".parse().unwrap();
        let cfg1 = Config::default()
            .with_port(port)
            .with_listen_addr(addr1)
            .with_net(net)
            .with_insecure_no_key();
        let cfg2 = Config::default()
            .with_port(port)
            .with_listen_addr(addr2)
            .with_net(net)
            .with_insecure_no_key();

        let mut rng = rand::rngs::ThreadRng::default();

        let key_values: Vec<(u32, u32)> =
            (&mut rng).sample_iter(Standard).take(1_000_000).collect();

        let rt = tokio::runtime::Runtime::new().unwrap();

        let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
        let mut group = c.benchmark_group("ReplicatedMap::reconcile");
        group.plot_config(plot_config);
        let mut size = 10;
        while size <= key_values.len() {
            group.sample_size(10.max(1_000_000 / size).min(100));
            group.sampling_mode(SamplingMode::Linear);
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                rt.block_on(async {
                    let store1 = ReplicatedMap::new(cfg1.clone())
                        .await
                        .expect("bind failed")
                        .with_seed(addr2);
                    store1.insert_bulk(&key_values[..size]);
                    let store2 = ReplicatedMap::new(cfg2.clone())
                        .await
                        .expect("bind failed")
                        .with_seed(addr1);
                    store2.insert_bulk(&key_values[..size]);
                    let task1 = tokio::spawn(store1.clone().run(CancellationToken::new()));
                    let task2 = tokio::spawn(store2.clone().run(CancellationToken::new()));

                    b.iter(|| {
                        let k: u32 = rng.gen();
                        let v: u32 = rng.gen();
                        store1.just_insert(k, v);
                        let clone = store1.clone();
                        let task = tokio::spawn(async move { clone.start_reconciliation().await });
                        while store2.get(&k).is_none() {
                            std::thread::sleep(Duration::from_micros(1));
                        }
                        store1.just_remove(&k);
                        task.abort();
                        let clone = store1.clone();
                        let task = tokio::spawn(async move { clone.start_reconciliation().await });
                        while store2.get(&k).is_some() {
                            std::thread::sleep(Duration::from_micros(1));
                        }
                        task.abort();
                    });

                    task2.abort();
                    task1.abort();
                    let _ = tokio::join!(task1, task2);
                })
            });
            size *= 10;
        }
    }

    /// RTT sweep for `service_reconcile_rtt`: the same grid `system.rs`'s injected-RTT lane
    /// sweeps (`rtt_sweep` there — duplicated here rather than shared: each bench binary is a
    /// separate compilation unit, and no target here imports another's. `gossip::netem` itself is
    /// genuinely shared, being a library module rather than a per-binary `mod`.)
    fn rtt_sweep() -> Vec<Rtt> {
        [0.0, 0.1, 1.0, 10.0, 50.0]
            .into_iter()
            .map(Rtt::from_millis)
            .collect()
    }

    /// Store sizes for `service_reconcile_rtt`: #461's grid, `n` = 10³…10⁶ — the same range
    /// `benches/protocol.rs`'s counted tables sweep, so the measured and counted columns line up.
    const RECONCILE_RTT_SIZES: &[usize] = &[1_000, 10_000, 100_000, 1_000_000];

    /// How the `d` differing keys are laid out — the same two layouts `benches/protocol.rs`'s
    /// (private) `Clustering` sweeps, duplicated here for the reason `rtt_sweep` above states.
    #[derive(Clone, Copy, Debug)]
    enum Clustering {
        /// Spread evenly, so every subtree refines.
        Scattered,
        /// One contiguous block, centred in the key space.
        Clustered,
    }

    impl Clustering {
        fn label(self) -> &'static str {
            match self {
                Clustering::Scattered => "scattered",
                Clustering::Clustered => "clustered",
            }
        }
    }

    /// `(d, clustering)` pairs #461 asks for. `d = 0` is the in-sync baseline — no layout to vary.
    /// `d = 1` only scatters, as in `protocol.rs::DIFFERENCES` — a single key has no layout either
    /// way. 10/100/1000 sweep both, past a 256-cell sketch's capacity at the top end.
    const D_CLUSTERINGS: &[(usize, Clustering)] = &[
        (0, Clustering::Scattered),
        (1, Clustering::Scattered),
        (10, Clustering::Scattered),
        (10, Clustering::Clustered),
        (100, Clustering::Scattered),
        (100, Clustering::Clustered),
        (1_000, Clustering::Scattered),
        (1_000, Clustering::Clustered),
    ];

    /// The `d` keys (out of `0..n`) one peer diverges on, laid out per `clustering` — the same
    /// layout `benches/protocol.rs::missing_keys` uses, so a round here and a round there refine
    /// over the same-shaped difference.
    fn diverging_keys(n: usize, d: usize, clustering: Clustering) -> Vec<u32> {
        if d == 0 {
            return Vec::new();
        }
        match clustering {
            Clustering::Scattered => (1..=d as u64)
                .map(|i| ((n as u64 / (d as u64 + 1)) * i) as u32)
                .collect(),
            // Centred so the block is not adjacent to either end of the key space, where a
            // partition's outermost child would absorb it for free.
            Clustering::Clustered => {
                let start = (n / 2 - d / 2) as u64;
                (start..start + d as u64).map(|k| k as u32).collect()
            }
        }
    }

    /// A fresh loopback address pair per `(n, rtt)` build — one build per combination, not per
    /// Criterion sample, so a handful suffice; a fresh pair still avoids any rebind collision.
    fn fresh_reconcile_rtt_pair() -> (IpAddr, IpAddr) {
        static N: AtomicU32 = AtomicU32::new(0);
        let i = N.fetch_add(1, Ordering::Relaxed);
        let hi = ((i >> 6) & 0xff) as u8;
        let lo = ((i & 0x3f) as u8) * 2 + 1;
        (
            format!("127.6.{hi}.{lo}").parse().unwrap(),
            format!("127.6.{hi}.{}", lo + 1).parse().unwrap(),
        )
    }

    /// Tallies datagrams a wrapped transport has *received*. The `d = 0` baseline round is
    /// otherwise unobservable: a root-fingerprint match makes `rbsr::protocol_round` return no
    /// comparison items and no differences (`src/replica/dispatch.rs`), so the responder sends
    /// nothing back and no local state changes anywhere — the only sign a round happened at all is
    /// that the initiator's message arrived at the responder's transport.
    struct RecvCountingTransport<T> {
        inner: T,
        received: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl<T: Transport> Transport for RecvCountingTransport<T> {
        async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            let result = self.inner.recv_from(buf).await;
            if result.is_ok() {
                self.received.fetch_add(1, Ordering::Relaxed);
            }
            result
        }

        async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
            self.inner.send_to(buf, dst).await
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.local_addr()
        }
    }

    /// Retries permitted for building and settling a fresh `(store1, store2)` pair before a
    /// `(n, rtt)` sweep point gives up — see [`service_reconcile_rtt`]'s docs for why a fresh pair
    /// occasionally needs one.
    const MAX_BUILD_ATTEMPTS: u32 = 20;

    /// Poll `condition` until it holds or a generous wall-clock deadline passes, returning
    /// whether it held. Does not retrigger — [`trigger_and_converge`] is the retriggering wrapper
    /// most callers below actually want; this is the single-round primitive it builds on.
    ///
    /// Bounded by elapsed time, not a spin count: some callers' `condition` is O(d) (checking up
    /// to 1000 keys), and a fixed spin ceiling costs that much more real time per spin the more
    /// expensive `condition` is — at `d = 1000` a ceiling sized for an O(1) check turned a
    /// genuine non-convergence into an effectively unbounded stall instead of a prompt failure.
    /// Checking the deadline every iteration keeps the give-up time predictable regardless of
    /// `condition`'s cost, while the tight `yield_now` spin (no added sleep) keeps the
    /// microsecond-scale convergent path exactly as fast as before.
    async fn converge(mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !condition() {
            if Instant::now() >= deadline {
                return false;
            }
            tokio::task::yield_now().await;
        }
        true
    }

    /// Extra `start_reconciliation` triggers [`trigger_and_converge`] allows past the first, for a
    /// round that does not converge on its own — see `service_reconcile_rtt`'s docs for why a
    /// large scattered divergence genuinely needs this.
    const MAX_ROUND_RETRIGGERS: u32 = 20;

    /// Trigger a reconciliation round on `store1` and poll `condition`, retriggering (up to
    /// [`MAX_ROUND_RETRIGGERS`] more times) if it does not converge on its own. Panics — loud, not
    /// silent — if even that many retriggers do not converge, since at that point it is a genuine
    /// stall (`service_reconcile_rtt`'s docs), not merely a large divergence needing more rounds.
    async fn trigger_and_converge(
        store1: &ReplicatedMap<u32, u32>,
        mut condition: impl FnMut() -> bool,
        what: impl std::fmt::Display,
    ) {
        for _ in 0..=MAX_ROUND_RETRIGGERS {
            store1.start_reconciliation().await;
            if converge(&mut condition).await {
                return;
            }
        }
        panic!(
            "service_reconcile_rtt: {what} did not converge after {} triggers",
            MAX_ROUND_RETRIGGERS + 1
        );
    }

    /// The refinement chain, timed, over an injected-RTT link: composes
    /// `ReplicatedMap::new_with_transport`, `gossip::netem::NetemTransport` and the existing `rtt_sweep`
    /// (`system.rs`'s, duplicated above) with `service_reconcile`'s own divergence mechanism — the
    /// only one that exercises refinement rather than the outer-range mismatch `cold_sync_rtt`
    /// builds (`benches/README.md` "Pricing that end-to-end...").
    ///
    /// One peer loads the `n`-entry corpus and seeds the other, which starts empty and pulls the
    /// whole dataset via cold-sync (`cold_sync_rtt`'s own bootstrap, proven reliable across this
    /// same `n` × `NetemTransport` × RTT matrix) — loading both peers independently and assuming
    /// they already match at the root does not converge reliably at larger `n` under nonzero RTT: a
    /// `NetemTransport` queue/pump artifact under high datagram volume, not a protocol issue.
    /// `reconcile_interval` is fixed far longer than any sample, so every round below is the one
    /// `start_reconciliation` explicitly triggers, never a background tick.
    ///
    /// Per sample: `just_remove` the `d` chosen keys on the initiator (a genuine content
    /// difference, not a timestamp race), trigger one round, poll until the responder reflects the
    /// removal; then `just_insert` them back and repeat, so the pair returns to baseline for the
    /// next sample. `d = 0` has no keys to remove — its round finds nothing to refine, so it is
    /// timed via `RecvCountingTransport` instead (see that type's docs).
    ///
    /// Two failure modes this harness works around, both reproduced outside Criterion too
    /// (`benches/README.md`'s "past a fixed-capacity sketch"/"n = 10⁶" sections carry the measured
    /// consequences):
    /// - A round can stall outright — not slow, permanently stuck — under the two `run()` loops'
    ///   genuine concurrency on a multi-threaded runtime. Pinning this benchmark's own runtime to
    ///   `current_thread` (below) cuts the rate sharply but not to zero, so settling a fresh pair
    ///   retries wholesale ([`MAX_BUILD_ATTEMPTS`]) rather than assuming `current_thread` alone
    ///   suffices; rebuilding happens once per `(n, rtt)`, not per sample, which keeps the retry
    ///   cheap regardless.
    /// - A single `start_reconciliation` does not reliably drive a large *scattered* divergence
    ///   (many separate leaf-level differences, e.g. `d = 1000` at `n = 10_000`) to completion: the
    ///   round can plateau part-way and only a fresh trigger resumes it. This matches #185's "N
    ///   round trips" model rather than contradicting it: [`trigger_and_converge`] retriggers
    ///   (bounded, [`MAX_ROUND_RETRIGGERS`]) when a round does not converge on its own, and the
    ///   total elapsed time across every retrigger counts toward the sample — that total *is* the
    ///   measurement. A *clustered* divergence of the same `d` converges in one round even at
    ///   `d = 1000`, so this is specific to how many separate ranges must resolve, not to `d`
    ///   alone.
    fn service_reconcile_rtt(c: &mut Criterion) {
        let net = "127.0.0.1/8".parse().unwrap();
        let port = 9_990;

        let mut group = c.benchmark_group("service_reconcile_rtt");
        group.sample_size(10);
        group.sampling_mode(SamplingMode::Flat);
        group.warm_up_time(Duration::from_millis(500));
        // Criterion's 5 s default `measurement_time` is sized for a handful of benchmark ids;
        // `RECONCILE_RTT_SIZES × rtt_sweep() × D_CLUSTERINGS` is 160 of them, so left at the
        // default this group alone would take at least 160 × 5 s ≈ 13 minutes regardless of how
        // cheap any one round is. 1 s keeps `sample_size(10)`'s ten samples meaningful without
        // padding a microsecond-scale round (the `d = 0` baseline at `rtt = 0ms`) out to 5 s.
        group.measurement_time(Duration::from_millis(1_000));

        for &n in RECONCILE_RTT_SIZES {
            let key_values: Vec<(u32, u32)> = (0..n as u32)
                .map(|k| (k, k.wrapping_mul(2_654_435_761)))
                .collect();

            for rtt in rtt_sweep() {
                // Fresh per `(n, rtt)`, not shared across the sweep: a background task this
                // harness does not track — `Replica::spawn_paced_send`'s detached bulk-dump
                // sends, paced by `bulk_send_rate` and outlasting the `run()` loops this function
                // does abort/await between pairs (below) — can still be draining when the next
                // pair starts, and on this single-threaded runtime it competes with that pair's
                // own tasks for the same one OS thread. Dropping the whole runtime is the only
                // way to guarantee such orphaned work is gone rather than merely aborted-and-
                // maybe-still-scheduled; recreating it here traded one construction per `(n,
                // rtt)` (cheap, ~20 total) for that guarantee.
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let link = Link::at(rtt);
                let cfg = |addr: IpAddr| {
                    Config::default()
                        .with_port(port)
                        .with_listen_addr(addr)
                        .with_net(net)
                        .with_insecure_no_key()
                        .with_reconcile_interval(Duration::from_secs(3600))
                };

                // Building this pair and driving it through settling and warm-up is a one-time
                // cost per `(n, rtt)`, not per Criterion sample (`service_reconcile_rtt`'s docs),
                // so retrying it wholesale on a stall — rebuilding from a fresh network and
                // addresses — is cheap: at most a couple of dozen extra pairs across the whole
                // sweep.
                let (store1, store2, _received1, received2, tasks) = rt.block_on(async {
                    let mut attempt = 0u32;
                    loop {
                        attempt += 1;
                        let (addr1, addr2) = fresh_reconcile_rtt_pair();
                        let network = InMemoryNetwork::new();
                        let received1 = Arc::new(AtomicU64::new(0));
                        let transport1 = RecvCountingTransport {
                            inner: NetemTransport::new(
                                Arc::new(network.bind(SocketAddr::new(addr1, port))),
                                Netem::uniform(link, Seed::DEFAULT),
                            ),
                            received: Arc::clone(&received1),
                        };
                        let received2 = Arc::new(AtomicU64::new(0));
                        let transport2 = RecvCountingTransport {
                            inner: NetemTransport::new(
                                Arc::new(network.bind(SocketAddr::new(addr2, port))),
                                Netem::uniform(link, Seed::DEFAULT),
                            ),
                            received: Arc::clone(&received2),
                        };
                        let store1 = ReplicatedMap::<u32, u32>::new_with_transport(
                            cfg(addr1),
                            Arc::new(transport1),
                        );
                        let store2 = ReplicatedMap::<u32, u32>::new_with_transport(
                            cfg(addr2),
                            Arc::new(transport2),
                        );
                        // Only `store1` is pre-loaded; `store2` starts empty and pulls the whole
                        // corpus via cold-sync — see `service_reconcile_rtt`'s docs for why
                        // (`cold_sync_rtt`'s own bootstrap, proven reliable across this same `n` ×
                        // `NetemTransport` × RTT matrix).
                        store1.insert_bulk(&key_values);
                        store1.seed_peer(addr2);
                        store2.seed_peer(addr1);
                        let tasks = [
                            tokio::spawn(store1.clone().run(CancellationToken::new())),
                            tokio::spawn(store2.clone().run(CancellationToken::new())),
                        ];
                        let target_fingerprint = store1.fingerprint(..);
                        let settled =
                            converge(|| store2.fingerprint(..) == target_fingerprint).await;
                        // The first couple of content-divergence round trips a fresh pair ever
                        // exchanges — one for a tombstoning round, one for a live-value round,
                        // each the first time that *kind* of round happens on this pair — pay a
                        // one-time warm-up cost of a few seconds, decaying to the steady-state
                        // microseconds every round costs after; a boundary key (one past the
                        // corpus, becoming the tree's new rightmost entry) made it far worse
                        // still. Left in a timed sample it would dominate that sample's mean, so
                        // pay it here, untimed, once per `(n, rtt)` pair, removing and restoring
                        // an interior corpus key well away from anything `D_CLUSTERINGS` touches.
                        let warm_up_key = (n / 4) as u32;
                        let warm_up_value = warm_up_key.wrapping_mul(2_654_435_761);
                        let mut warmed = settled;
                        for _ in 0..4 {
                            if !warmed {
                                break;
                            }
                            store1.just_remove(&warm_up_key);
                            store1.start_reconciliation().await;
                            warmed = converge(|| store2.get(&warm_up_key).is_none()).await;
                            if !warmed {
                                break;
                            }
                            store1.just_insert(warm_up_key, warm_up_value);
                            store1.start_reconciliation().await;
                            warmed =
                                converge(|| store2.get_cloned(&warm_up_key) == Some(warm_up_value))
                                    .await;
                        }
                        if warmed {
                            break (store1, store2, received1, received2, tasks);
                        }
                        for task in tasks {
                            task.abort();
                            let _ = task.await;
                        }
                        assert!(
                            attempt < MAX_BUILD_ATTEMPTS,
                            "service_reconcile_rtt: settling/warming up a fresh pair did not \
                             converge in {MAX_BUILD_ATTEMPTS} attempts"
                        );
                    }
                });

                for &(d, clustering) in D_CLUSTERINGS {
                    let missing = diverging_keys(n, d, clustering);
                    let restored: Vec<u32> = missing
                        .iter()
                        .map(|&k| k.wrapping_mul(2_654_435_761))
                        .collect();
                    let id = if d == 0 {
                        format!("n={n}/d=0")
                    } else {
                        format!("n={n}/d={d}/{}", clustering.label())
                    };

                    group.bench_with_input(BenchmarkId::new(&id, rtt.label()), &rtt, |b, _| {
                        b.iter_custom(|iters| {
                            rt.block_on(async {
                                let mut total = Duration::ZERO;
                                for _ in 0..iters {
                                    if missing.is_empty() {
                                        let before = received2.load(Ordering::Relaxed);
                                        let start = Instant::now();
                                        trigger_and_converge(
                                            &store1,
                                            || received2.load(Ordering::Relaxed) > before,
                                            "d=0 round",
                                        )
                                        .await;
                                        total += start.elapsed();
                                        continue;
                                    }

                                    let start = Instant::now();
                                    for &k in &missing {
                                        store1.just_remove(&k);
                                    }
                                    trigger_and_converge(
                                        &store1,
                                        || missing.iter().all(|k| store2.get(k).is_none()),
                                        format_args!("d={} remove round", missing.len()),
                                    )
                                    .await;

                                    for (&k, &v) in missing.iter().zip(restored.iter()) {
                                        store1.just_insert(k, v);
                                    }
                                    trigger_and_converge(
                                        &store1,
                                        || {
                                            missing
                                                .iter()
                                                .zip(restored.iter())
                                                .all(|(k, &v)| store2.get_cloned(k) == Some(v))
                                        },
                                        format_args!("d={} insert round", missing.len()),
                                    )
                                    .await;
                                    total += start.elapsed();
                                }
                                total
                            })
                        });
                    });
                }

                rt.block_on(async {
                    for task in tasks {
                        task.abort();
                        let _ = task.await;
                    }
                });
            }
        }
        group.finish();
    }

    /// `Config::reconcile_interval` values this lane sweeps: production's own default (1 s) down
    /// to a much shorter floor. `service_reconcile_rtt` fixes this at 3600 s specifically to
    /// disable the idle timer and substitute its own manual retrigger
    /// (`trigger_and_converge`, 15 s cadence); this lane does the opposite -- the real `run()`
    /// idle timeout (`src/replica/run.rs`) is the *only* thing allowed to retrigger
    /// `start_reconciliation` after the initial divergence below, so what gets timed is however
    /// many `reconcile_interval` cycles recovery actually costs, not the RTT
    /// `service_reconcile_rtt` already prices.
    fn reconcile_interval_sweep() -> Vec<Duration> {
        [10, 100, 1_000]
            .into_iter()
            .map(Duration::from_millis)
            .collect()
    }

    /// `n` this lane fixes at: #516's own repro size, so a reader can cross-reference the two
    /// directly.
    const INTERVAL_N: usize = 10_000;

    /// `(d, clustering)` cases this lane sweeps: `d = 10` scattered is a single-round case at
    /// this `n` (`service_reconcile_rtt`'s own "clean" cell), so its cost should track
    /// `reconcile_interval` almost exactly (one idle-timeout cycle, whatever that cycle's length
    /// is). `d = 1000` scattered is #516's own repro -- a divergence a single round does not
    /// fully resolve, so its cost is however many *extra* `reconcile_interval` cycles the idle
    /// timer needs to notice and retry the batch the first round's dump-slot race dropped.
    const INTERVAL_D_CLUSTERINGS: &[(usize, Clustering)] =
        &[(10, Clustering::Scattered), (1_000, Clustering::Scattered)];

    /// Times recovery through the real `reconcile_interval` idle-timeout path (`src/replica/
    /// run.rs`) instead of `service_reconcile_rtt`'s manual retrigger, across a
    /// `reconcile_interval` sweep -- motivated by #516: a `differences` batch that loses the
    /// per-peer dump-slot race is silently dropped, so a single round does not always resolve a
    /// large/scattered divergence, and the real repair cost in that case is however many
    /// `reconcile_interval` cycles it takes the idle timer to notice and retry, not the RTT
    /// `service_reconcile_rtt` already times.
    ///
    /// One pair, built once: unlike `service_reconcile_rtt`, RTT is fixed (this transport injects
    /// none) and `reconcile_interval` is retunable at runtime
    /// (`ReplicatedMap::set_reconcile_interval`, re-read every `run()` loop iteration), so
    /// there is no per-sweep-point transport to rebuild.
    ///
    /// Per sample: `just_remove` the `d` chosen keys on the initiator, same as
    /// `service_reconcile_rtt`, but poll for repair with [`converge`] alone -- no
    /// `start_reconciliation` call after it, so recovery can only come from the idle timer.
    /// Restoring the pair to baseline afterward is untimed and uses [`trigger_and_converge`] (the
    /// manual-retrigger helper): resetting state between samples is not what this lane measures,
    /// and bounding it by the idle timer too would double-count the very cost being priced.
    fn service_reconcile_interval(c: &mut Criterion) {
        let net = "127.0.0.1/8".parse().unwrap();
        let port = 9_990;

        let mut group = c.benchmark_group("service_reconcile_interval");
        group.sample_size(10);
        group.sampling_mode(SamplingMode::Flat);
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_millis(1_000));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let key_values: Vec<(u32, u32)> = (0..INTERVAL_N as u32)
            .map(|k| (k, k.wrapping_mul(2_654_435_761)))
            .collect();
        let cfg = |addr: IpAddr| {
            Config::default()
                .with_port(port)
                .with_listen_addr(addr)
                .with_net(net)
                .with_insecure_no_key()
        };

        let (store1, store2, tasks) = rt.block_on(async {
            let mut attempt = 0u32;
            loop {
                attempt += 1;
                let (addr1, addr2) = fresh_reconcile_rtt_pair();
                let network = InMemoryNetwork::new();
                let store1 = ReplicatedMap::<u32, u32>::new_with_transport(
                    cfg(addr1),
                    Arc::new(network.bind(SocketAddr::new(addr1, port))),
                );
                let store2 = ReplicatedMap::<u32, u32>::new_with_transport(
                    cfg(addr2),
                    Arc::new(network.bind(SocketAddr::new(addr2, port))),
                );
                store1.insert_bulk(&key_values);
                store1.seed_peer(addr2);
                store2.seed_peer(addr1);
                let tasks = [
                    tokio::spawn(store1.clone().run(CancellationToken::new())),
                    tokio::spawn(store2.clone().run(CancellationToken::new())),
                ];
                let target_fingerprint = store1.fingerprint(..);
                if converge(|| store2.fingerprint(..) == target_fingerprint).await {
                    break (store1, store2, tasks);
                }
                for task in tasks {
                    task.abort();
                    let _ = task.await;
                }
                assert!(
                    attempt < MAX_BUILD_ATTEMPTS,
                    "service_reconcile_interval: cold-sync bootstrap did not converge in \
                     {MAX_BUILD_ATTEMPTS} attempts"
                );
            }
        });

        for &reconcile_interval in &reconcile_interval_sweep() {
            store1.set_reconcile_interval(reconcile_interval);
            store2.set_reconcile_interval(reconcile_interval);

            for &(d, clustering) in INTERVAL_D_CLUSTERINGS {
                let missing = diverging_keys(INTERVAL_N, d, clustering);
                let restored: Vec<u32> = missing
                    .iter()
                    .map(|&k| k.wrapping_mul(2_654_435_761))
                    .collect();
                let id = format!("d={d}/{}", clustering.label());

                group.bench_with_input(
                    BenchmarkId::new(&id, format!("{}ms", reconcile_interval.as_millis())),
                    &reconcile_interval,
                    |b, _| {
                        b.iter_custom(|iters| {
                            rt.block_on(async {
                                let mut total = Duration::ZERO;
                                for _ in 0..iters {
                                    let start = Instant::now();
                                    for &k in &missing {
                                        store1.just_remove(&k);
                                    }
                                    // No manual start_reconciliation() call: recovery must come
                                    // from the real reconcile_interval idle timeout alone.
                                    let repaired = converge(|| {
                                        missing.iter().all(|k| store2.get(k).is_none())
                                    })
                                    .await;
                                    total += start.elapsed();
                                    assert!(
                                        repaired,
                                        "service_reconcile_interval: {id} did not repair via \
                                         the idle timer alone"
                                    );

                                    // Untimed restore, via the robust manual-retrigger helper.
                                    for (&k, &v) in missing.iter().zip(restored.iter()) {
                                        store1.just_insert(k, v);
                                    }
                                    trigger_and_converge(
                                        &store1,
                                        || {
                                            missing
                                                .iter()
                                                .zip(restored.iter())
                                                .all(|(k, &v)| store2.get_cloned(k) == Some(v))
                                        },
                                        format_args!("{id} restore"),
                                    )
                                    .await;
                                }
                                total
                            })
                        });
                    },
                );
            }
        }

        rt.block_on(async {
            for task in tasks {
                task.abort();
                let _ = task.await;
            }
        });
        group.finish();
    }

    criterion_group!(
        benches,
        fingerprint_tree_map_new,
        fingerprint_tree_map_fill,
        fingerprint_tree_map_insert,
        fingerprint_tree_map_remove,
        fingerprint_tree_map_range_fingerprint,
        read_replica_memory,
        bulk_load_just_insert,
        service_send,
        service_reconcile,
        service_reconcile_rtt,
        service_reconcile_interval,
    );
    // Equivalent to `criterion_main!(benches)`, but exposed as a named fn so the top-level `main`
    // (defined outside this feature-gated module) can drive it.
    pub fn main() {
        benches();
        Criterion::default().configure_from_args().final_summary();
    }
} // mod imp
