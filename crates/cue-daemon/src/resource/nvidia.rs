//! Built-in NVIDIA GPU resource provider.
//!
//! The provider is available when daemon config enables `[resources.nvidia]`.
//! Actual NVML probing is compiled only behind the `gpu-nvidia` feature; the
//! selection/reservation logic is pure Rust and testable without NVIDIA drivers.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Result;
use tracing::warn;

use cue_core::{
    JobId,
    resource::{
        Grant, Need, ProviderId, Reject, Reservation, ReservationId, ResourceQuantity,
        ResourceUnit, Snapshot,
    },
};

use crate::config::NvidiaResourceConfig;

use super::provider::{Provider, ReserveRequest};

#[derive(Debug, Clone)]
struct GpuSample {
    index: u32,
    total_mem: u64,
    free_mem: u64,
}

trait NvidiaBackend: Send + Sync {
    fn probe(&self) -> Result<Vec<GpuSample>>;
}

pub struct NvidiaGpuProvider {
    id: ProviderId,
    gpu_key: String,
    gpu_mem_key: String,
    safety_margin_bytes: u64,
    probe_ttl: Duration,
    backend: Box<dyn NvidiaBackend>,
    state: Mutex<NvidiaState>,
}

#[derive(Default, Debug)]
struct NvidiaState {
    cached: Option<CachedProbe>,
    reservations: HashMap<ReservationId, Vec<GpuReservation>>,
}

#[derive(Debug, Clone)]
struct CachedProbe {
    sampled_at: Instant,
    samples: Vec<GpuSample>,
}

#[derive(Debug, Clone)]
struct GpuReservation {
    index: u32,
    bytes: u64,
    exclusive: bool,
}

impl NvidiaGpuProvider {
    pub fn try_from_config(config: &NvidiaResourceConfig) -> Option<Self> {
        if !config.enabled {
            return None;
        }
        Self::try_backend().map(|backend| Self::from_backend(config, backend))
    }

    fn from_backend(config: &NvidiaResourceConfig, backend: Box<dyn NvidiaBackend>) -> Self {
        Self {
            id: ProviderId::new(config.provider_id.clone()),
            gpu_key: config.gpu_key.clone(),
            gpu_mem_key: config.gpu_mem_key.clone(),
            safety_margin_bytes: config.safety_margin_bytes,
            probe_ttl: Duration::from_millis(config.probe_ttl_ms),
            backend,
            state: Mutex::new(NvidiaState::default()),
        }
    }

    #[cfg(test)]
    fn for_test(samples: Vec<(u32, u64, u64)>, safety_margin_bytes: u64) -> Self {
        let config = NvidiaResourceConfig {
            enabled: true,
            provider_id: "nvidia".into(),
            gpu_key: "gpu".into(),
            gpu_mem_key: "gpu_mem".into(),
            safety_margin_bytes,
            probe_ttl_ms: 1_000,
        };
        Self::from_backend(
            &config,
            Box::new(StaticBackend {
                samples: samples
                    .into_iter()
                    .map(|(index, total_mem, free_mem)| GpuSample {
                        index,
                        total_mem,
                        free_mem,
                    })
                    .collect(),
            }),
        )
    }

    fn probe_samples(&self, state: &mut NvidiaState) -> Result<Vec<GpuSample>> {
        if let Some(cached) = &state.cached
            && cached.sampled_at.elapsed() < self.probe_ttl
        {
            return Ok(cached.samples.clone());
        }
        let samples = self.backend.probe()?;
        state.cached = Some(CachedProbe {
            sampled_at: Instant::now(),
            samples: samples.clone(),
        });
        Ok(samples)
    }

    fn reserved_for_index(state: &NvidiaState, index: u32) -> (u64, bool) {
        let mut bytes = 0u64;
        let mut exclusive = false;
        for units in state.reservations.values() {
            for unit in units {
                if unit.index == index {
                    bytes = bytes.saturating_add(unit.bytes);
                    exclusive |= unit.exclusive;
                }
            }
        }
        (bytes, exclusive)
    }

    fn effective_free(&self, sample: &GpuSample, state: &NvidiaState) -> Option<u64> {
        let (reserved, exclusive) = Self::reserved_for_index(state, sample.index);
        if exclusive {
            return None;
        }
        Some(
            sample
                .free_mem
                .min(sample.total_mem.saturating_sub(reserved))
                .saturating_sub(self.safety_margin_bytes),
        )
    }

    fn reserve_inner(&self, req: &ReserveRequest) -> Result<Grant, Reject> {
        let gpu_count = match req.need.get(&self.gpu_key) {
            Some(ResourceQuantity::Count(n)) => n as usize,
            Some(other) => {
                return Err(Reject::new(format!(
                    "{} must be a count, got {other}",
                    self.gpu_key
                )));
            }
            None => 0,
        };
        let mem_per_gpu = match req.need.get(&self.gpu_mem_key) {
            Some(ResourceQuantity::Bytes(bytes)) => bytes,
            Some(other) => {
                return Err(Reject::new(format!(
                    "{} must be bytes, got {other}",
                    self.gpu_mem_key
                )));
            }
            None => 0,
        };
        let needed_gpus = if gpu_count > 0 {
            gpu_count
        } else if mem_per_gpu > 0 {
            1
        } else {
            0
        };
        if needed_gpus == 0 {
            return Ok(Reservation::new(
                format!("{}-{}", self.id, req.job_id),
                req.job_id,
                self.id.clone(),
            ));
        }

        let mut state = self.state.lock().expect("nvidia provider state poisoned");
        let mut samples = self
            .probe_samples(&mut state)
            .map_err(|error| Reject::new(format!("NVML probe failed: {error:#}")))?;
        samples.sort_by_key(|sample| Reverse(sample.index));

        let mut selected = Vec::new();
        let mut max_effective_free = 0u64;
        for sample in &samples {
            let Some(effective_free) = self.effective_free(sample, &state) else {
                continue;
            };
            max_effective_free = max_effective_free.max(effective_free);
            if effective_free >= mem_per_gpu {
                selected.push(sample.index);
                if selected.len() == needed_gpus {
                    break;
                }
            }
        }

        if selected.len() < needed_gpus {
            let mut reject = Reject::new(format!(
                "waiting GPU: need {needed_gpus} x {}, max effective free {}",
                ResourceQuantity::Bytes(mem_per_gpu),
                ResourceQuantity::Bytes(max_effective_free)
            ));
            if mem_per_gpu > 0 {
                reject = reject
                    .with_demand(
                        self.gpu_mem_key.clone(),
                        ResourceQuantity::Bytes(mem_per_gpu),
                    )
                    .with_available(ResourceQuantity::Bytes(max_effective_free));
            }
            return Err(reject);
        }

        let grant_id = ReservationId::new(format!("{}-{}", self.id, req.job_id));
        let exclusive = mem_per_gpu == 0 && req.need.contains(&self.gpu_key);
        let units: Vec<GpuReservation> = selected
            .iter()
            .copied()
            .map(|index| GpuReservation {
                index,
                bytes: mem_per_gpu,
                exclusive,
            })
            .collect();
        state.reservations.insert(grant_id.clone(), units);

        let visible_devices = selected
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut env = BTreeMap::new();
        env.insert("CUDA_DEVICE_ORDER".to_string(), "PCI_BUS_ID".to_string());
        env.insert("CUDA_VISIBLE_DEVICES".to_string(), visible_devices);

        let mut info = BTreeMap::new();
        info.insert(
            self.gpu_key.clone(),
            ResourceQuantity::Count(selected.len() as u64),
        );
        if mem_per_gpu > 0 {
            info.insert(
                self.gpu_mem_key.clone(),
                ResourceQuantity::Bytes(mem_per_gpu.saturating_mul(selected.len() as u64)),
            );
        }

        Ok(Reservation {
            id: grant_id,
            job_id: req.job_id,
            provider_id: self.id.clone(),
            env,
            info,
            acquired_at: std::time::SystemTime::now(),
        })
    }
}

impl Provider for NvidiaGpuProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn keys(&self) -> Vec<String> {
        vec![self.gpu_key.clone(), self.gpu_mem_key.clone()]
    }

    fn probe(&self) -> Snapshot {
        let mut state = self.state.lock().expect("nvidia provider state poisoned");
        let samples = match self.probe_samples(&mut state) {
            Ok(samples) => samples,
            Err(error) => {
                warn!(provider = %self.id, "nvidia probe failed: {error:#}");
                Vec::new()
            }
        };
        let units = samples
            .iter()
            .map(|sample| {
                let (reserved_mem, exclusive) = Self::reserved_for_index(&state, sample.index);
                let mut unit = ResourceUnit::new(sample.index.to_string())
                    .with_attr("total_mem", ResourceQuantity::Bytes(sample.total_mem))
                    .with_attr("free_mem", ResourceQuantity::Bytes(sample.free_mem))
                    .with_attr("reserved_mem", ResourceQuantity::Bytes(reserved_mem));
                if let Some(effective_free) = self.effective_free(sample, &state) {
                    unit = unit.with_attr(
                        "effective_free_mem",
                        ResourceQuantity::Bytes(effective_free),
                    );
                }
                if exclusive {
                    unit = unit.with_attr("exclusive", ResourceQuantity::Count(1));
                }
                unit
            })
            .collect();
        Snapshot::new(self.id.clone(), units)
    }

    fn reserve(&self, req: &ReserveRequest) -> Result<Grant, Reject> {
        self.reserve_inner(req)
    }

    fn release(&self, grant_id: &ReservationId) {
        let mut state = self.state.lock().expect("nvidia provider state poisoned");
        state.reservations.remove(grant_id);
    }
}

#[cfg(feature = "gpu-nvidia")]
impl NvidiaGpuProvider {
    fn try_backend() -> Option<Box<dyn NvidiaBackend>> {
        match NvmlBackend::new() {
            Ok(backend) => Some(Box::new(backend)),
            Err(error) => {
                warn!("NVIDIA resource provider disabled: NVML init failed: {error:#}");
                None
            }
        }
    }
}

#[cfg(not(feature = "gpu-nvidia"))]
impl NvidiaGpuProvider {
    fn try_backend() -> Option<Box<dyn NvidiaBackend>> {
        warn!("NVIDIA resource provider disabled: cued was built without the `gpu-nvidia` feature");
        None
    }
}

#[cfg(feature = "gpu-nvidia")]
struct NvmlBackend {
    nvml: nvml_wrapper::Nvml,
}

#[cfg(feature = "gpu-nvidia")]
impl NvmlBackend {
    fn new() -> Result<Self> {
        Ok(Self {
            nvml: nvml_wrapper::Nvml::init()?,
        })
    }
}

#[cfg(feature = "gpu-nvidia")]
impl NvidiaBackend for NvmlBackend {
    fn probe(&self) -> Result<Vec<GpuSample>> {
        let count = self.nvml.device_count()?;
        let mut samples = Vec::new();
        for index in 0..count {
            let device = self.nvml.device_by_index(index)?;
            let memory = device.memory_info()?;
            samples.push(GpuSample {
                index,
                total_mem: memory.total,
                free_mem: memory.free,
            });
        }
        Ok(samples)
    }
}

#[cfg(test)]
struct StaticBackend {
    samples: Vec<GpuSample>,
}

#[cfg(test)]
impl NvidiaBackend for StaticBackend {
    fn probe(&self) -> Result<Vec<GpuSample>> {
        Ok(self.samples.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(gib: u64) -> u64 {
        gib * 1024 * 1024 * 1024
    }

    #[test]
    fn reserve_prefers_highest_index_and_injects_cuda_env() {
        let provider = NvidiaGpuProvider::for_test(
            vec![
                (0, bytes(80), bytes(80)),
                (1, bytes(80), bytes(80)),
                (2, bytes(80), bytes(80)),
            ],
            0,
        );
        let need = Need::from_pairs([
            ("gpu", ResourceQuantity::Count(2)),
            ("gpu_mem", ResourceQuantity::Bytes(bytes(24))),
        ]);

        let grant = provider
            .reserve(&ReserveRequest::new(JobId(1), need))
            .expect("grant");
        assert_eq!(
            grant.env.get("CUDA_VISIBLE_DEVICES").map(String::as_str),
            Some("2,1")
        );
        assert_eq!(
            grant.env.get("CUDA_DEVICE_ORDER").map(String::as_str),
            Some("PCI_BUS_ID")
        );
    }

    #[test]
    fn reserve_rejects_when_safety_margin_leaves_insufficient_memory() {
        let provider = NvidiaGpuProvider::for_test(vec![(0, bytes(80), bytes(25))], bytes(2));
        let need = Need::from_pairs([("gpu_mem", ResourceQuantity::Bytes(bytes(24)))]);

        let reject = provider
            .reserve(&ReserveRequest::new(JobId(1), need))
            .expect_err("should reject");
        assert!(
            reject.reason.contains("max effective free 23GiB"),
            "{}",
            reject.reason
        );
    }

    #[test]
    fn release_makes_reserved_memory_available_again() {
        let provider = NvidiaGpuProvider::for_test(vec![(0, bytes(80), bytes(80))], 0);
        let need = Need::from_pairs([("gpu_mem", ResourceQuantity::Bytes(bytes(50)))]);

        let first = provider
            .reserve(&ReserveRequest::new(JobId(1), need.clone()))
            .expect("first grant");
        assert!(
            provider
                .reserve(&ReserveRequest::new(JobId(2), need.clone()))
                .is_err()
        );
        provider.release(&first.id);
        assert!(
            provider
                .reserve(&ReserveRequest::new(JobId(3), need))
                .is_ok()
        );
    }

    #[test]
    fn count_only_reservation_is_exclusive() {
        let provider = NvidiaGpuProvider::for_test(vec![(0, bytes(80), bytes(80))], 0);
        let need = Need::from_pairs([("gpu", ResourceQuantity::Count(1))]);

        let first = provider
            .reserve(&ReserveRequest::new(JobId(1), need.clone()))
            .expect("first grant");
        assert!(
            provider
                .reserve(&ReserveRequest::new(JobId(2), need.clone()))
                .is_err()
        );
        provider.release(&first.id);
        assert!(
            provider
                .reserve(&ReserveRequest::new(JobId(3), need))
                .is_ok()
        );
    }
}
