#[cfg(feature = "metrics")]
mod metrics_impl {
    use anyhow::{Context, Result};

    #[cfg(feature = "metrics_process")]
    use prometheus::process_collector::ProcessCollector;

    use prometheus::{self, Encoder, HistogramOpts, HistogramVec, Registry, TEXT_FORMAT, Opts};
    use tiny_http::{Header as HttpHeader, Response, Server};

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    pub struct Metrics {
        reg: Arc<Registry>,
        pub mempool_count: prometheus::GaugeVec,
        pub mempool_vsize: prometheus::GaugeVec,
        pub mempool_pending_operations_count: prometheus::GaugeVec,
    }

    impl Metrics {
        pub fn new(addr: SocketAddr) -> Result<Self> {
            let reg = Arc::new(Registry::new());

            #[cfg(feature = "metrics_process")]
            reg.register(Box::new(ProcessCollector::for_self()))
                .expect("failed to register ProcessCollector");

            let mempool_count = prometheus::GaugeVec::new(
                Opts::new("mempool_count", "Total number of mempool transactions"),
                &["type"],
            )?;
            let mempool_vsize = prometheus::GaugeVec::new(
                Opts::new("mempool_vsize", "Total vsize of mempool transactions"),
                &["type"],
            )?;
            let mempool_pending_operations_count = prometheus::GaugeVec::new(
                Opts::new(
                    "mempool_pending_operations_count",
                    "Total number of pending operations in mempool",
                ),
                &["type"],
            )?;

            reg.register(Box::new(mempool_count.clone()))?;
            reg.register(Box::new(mempool_vsize.clone()))?;
            reg.register(Box::new(mempool_pending_operations_count.clone()))?;

            let result = Self { 
                reg, 
                mempool_count, 
                mempool_vsize, 
                mempool_pending_operations_count, 
            };
            let reg = result.reg.clone();

            let server = match Server::http(addr) {
                Ok(server) => server,
                Err(err) => bail!("failed to start HTTP server on {}: {}", addr, err),
            };

            thread::spawn(move || {
                let content_type = HttpHeader::from_bytes(&b"Content-Type"[..], TEXT_FORMAT)
                    .expect("failed to create HTTP header for Prometheus text format");
                for request in server.incoming_requests() {
                    let mut buffer = vec![];
                    prometheus::TextEncoder::new()
                        .encode(&reg.gather(), &mut buffer)
                        .context("failed to encode metrics")?;
                    request
                        .respond(Response::from_data(buffer).with_header(content_type.clone()))
                        .context("failed to send HTTP response")?;
                }
                Ok::<(), anyhow::Error>(())
            });

            info!("serving Prometheus metrics on {}", addr);
            Ok(result)
        }

        pub fn dummy() -> Self {
            Self { 
                reg: Arc::new(Registry::new()), 
                mempool_count: prometheus::GaugeVec::new(
                    Opts::new("mempool_count", "Total number of mempool transactions"),
                    &["type"],
                ).unwrap(),
                mempool_vsize: prometheus::GaugeVec::new(
                    Opts::new("mempool_vsize", "Total vsize of mempool transactions"),
                    &["type"],
                ).unwrap(),
                mempool_pending_operations_count: prometheus::GaugeVec::new(
                    Opts::new(
                        "mempool_pending_operations_count",
                        "Total number of pending operations in mempool",
                    ),
                    &["type"],
                ).unwrap(),
            }
        }

        pub fn histogram_vec(
            &self,
            name: &str,
            desc: &str,
            label: &str,
            buckets: Vec<f64>,
        ) -> Histogram {
            let name = String::from("electrs_") + name;
            let opts = HistogramOpts::new(name, desc).buckets(buckets);
            let hist = HistogramVec::new(opts, &[label]).unwrap();
            self.reg
                .register(Box::new(hist.clone()))
                .expect("failed to register Histogram");
            Histogram { hist }
        }

        pub fn gauge(&self, name: &str, help: &str, label: &str) -> prometheus::GaugeVec {
            let opts = Opts::new(name, help);
            let gauge = prometheus::GaugeVec::new(opts, &[label]).unwrap();
            self.reg.register(Box::new(gauge.clone())).unwrap();
            gauge
        }

        pub fn gauge_vec(&self, name: &str, help: &str, labels: &[&str]) -> prometheus::GaugeVec {
            let opts = Opts::new(name, help);
            let gauge = prometheus::GaugeVec::new(opts, labels).unwrap();
            self.reg.register(Box::new(gauge.clone())).unwrap();
            gauge
        }
    }

    #[derive(Clone)]
    pub struct Histogram {
        hist: HistogramVec,
    }

    impl Histogram {
        pub fn observe(&self, label: &str, value: f64) {
            self.hist.with_label_values(&[label]).observe(value);
        }

        pub fn observe_duration<F, T>(&self, label: &str, func: F) -> T
        where
            F: FnOnce() -> T,
        {
            self.hist
                .with_label_values(&[label])
                .observe_closure_duration(func)
        }
    }
}

#[cfg(feature = "metrics")]
pub use metrics_impl::{Histogram, Metrics};

#[cfg(not(feature = "metrics"))]
mod metrics_fake {
    use anyhow::Result;

    use std::net::SocketAddr;

    pub struct Metrics {}

    impl Metrics {
        pub fn new(_addr: SocketAddr) -> Result<Self> {
            debug!("metrics collection is disabled");
            Ok(Self {})
        }

        pub fn histogram_vec(
            &self,
            _name: &str,
            _desc: &str,
            _label: &str,
            _buckets: Vec<f64>,
        ) -> Histogram {
            Histogram {}
        }

        pub fn gauge(&self, _name: &str, _desc: &str, _label: &str) -> prometheus::GaugeVec {
            prometheus::GaugeVec::new(
                prometheus::Opts::new("dummy", "dummy"),
                &["dummy"],
            ).unwrap()
        }

        pub fn gauge_vec(&self, _name: &str, _desc: &str, _labels: &[&str]) -> prometheus::GaugeVec {
            prometheus::GaugeVec::new(
                prometheus::Opts::new("dummy", "dummy"),
                &["dummy"],
            ).unwrap()
        }
    }

    #[derive(Clone)]
    pub struct Histogram {}

    impl Histogram {
        pub fn observe(&self, _label: &str, _value: f64) {}

        pub fn observe_duration<F, T>(&self, _label: &str, func: F) -> T
        where
            F: FnOnce() -> T,
        {
            func()
        }
    }
}

#[cfg(not(feature = "metrics"))]
pub use metrics_fake::{Histogram, Metrics};

pub(crate) fn default_duration_buckets() -> Vec<f64> {
    vec![
        1e-6, 2e-6, 5e-6, 1e-5, 2e-5, 5e-5, 1e-4, 2e-4, 5e-4, 1e-3, 2e-3, 5e-3, 1e-2, 2e-2, 5e-2,
        1e-1, 2e-1, 5e-1, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0,
    ]
}

pub(crate) fn default_size_buckets() -> Vec<f64> {
    vec![
        1.0, 2.0, 5.0, 1e1, 2e1, 5e1, 1e2, 2e2, 5e2, 1e3, 2e3, 5e3, 1e4, 2e4, 5e4, 1e5, 2e5, 5e5,
        1e6, 2e6, 5e6, 1e7,
    ]
}
