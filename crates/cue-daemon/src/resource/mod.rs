//! Resource scheduling for the cue-daemon.
//!
#![allow(dead_code, unused_imports)]
// Some provider/diagnostic APIs are intentionally introduced before the
// follow-up scheduler-admission, NVML, CLI-provider, and CLI-surface tasks
// consume every method/re-export.

//! Public surface:
//!
//! * [`Provider`] — the pluggable resource backend trait.
//! * [`ProviderRegistry`] — atomically routes a job's `Need` map to the
//!   right providers and tracks reservations per `JobId`.
//! * [`MockProvider`] — used by registry/scheduler tests; also reachable
//!   from cross-crate integration tests via `cue_daemon::resource::mock`.
//!
//! Specific provider implementations (NVML, CLI-shelled-out) live under
//! `providers/` and are introduced by later tasks. The registry itself
//! has no GPU/NVML/TPU knowledge.

use std::sync::Arc;

use anyhow::Result;

use crate::config::ResourceConfig;

pub mod cli;
pub mod mock;
pub mod nvidia;
pub mod provider;
pub mod registry;

#[cfg(test)]
pub(crate) use mock::{MockBehaviour, MockProvider, mock_provider};
pub(crate) use provider::{Provider, RejectGroup};
pub(crate) use registry::ProviderRegistry;

pub(crate) fn registry_from_config(config: &ResourceConfig) -> Result<ProviderRegistry> {
    let mut providers: Vec<Arc<dyn Provider>> = Vec::new();
    if let Some(provider) = nvidia::NvidiaGpuProvider::try_from_config(&config.nvidia) {
        providers.push(Arc::new(provider));
    }
    for (id, provider_config) in &config.cli {
        providers.push(Arc::new(cli::CliProvider::from_config(id, provider_config)));
    }
    ProviderRegistry::from_providers(providers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CliResourceProviderConfig, ResourceConfig};
    use std::collections::BTreeMap;

    #[test]
    fn registry_from_config_adds_cli_provider_routes() {
        let config = ResourceConfig {
            nvidia: Default::default(),
            cli: BTreeMap::from([(
                "license".to_string(),
                CliResourceProviderConfig {
                    keys: vec!["license".into()],
                    probe: vec!["license-helper".into(), "probe".into()],
                    reserve: vec!["license-helper".into(), "reserve".into()],
                    release: vec!["license-helper".into(), "release".into()],
                    timeout_ms: 123,
                },
            )]),
        };

        let registry = registry_from_config(&config).expect("registry");
        assert_eq!(
            registry.provider_ids(),
            vec![cue_core::resource::ProviderId::new("license")]
        );
        assert_eq!(
            registry.key_routes(),
            vec![(
                "license".to_string(),
                cue_core::resource::ProviderId::new("license")
            )]
        );
    }
}
