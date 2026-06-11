//! Shared client connection stack for cue-shell frontends.

pub mod cli;
mod client;
mod config_paths;
pub mod daemon_lifecycle;
mod host_discovery;
mod reconnect;
mod restart;
pub mod script_runner;
mod ssh_config;
mod ssh_transport;
mod transport_config;
mod transport_settings;
pub mod version_check;

pub use client::{
    ClientReader, CuedClient, MultiplexedClient, WriterHandle, WriterSendError, default_socket_path,
};
pub use config_paths::{
    ClientConfigPaths, ClientConfigSource, ClientConfigSources, client_config_paths,
    optional_client_config_paths, read_client_config_sources,
};
pub use host_discovery::{HostDiscoveryConfig, detected_configured_hosts};
pub use reconnect::{
    ClientConnector, ConnectionControlError, ConnectionController, ConnectionEvent,
    spawn_connection_manager_controllable,
};
pub use restart::RestartHandle;
pub use ssh_transport::{connect_ssh_transport, transport_connector};
pub use transport_config::{
    ResolvedTransport, SshProfile, TransportConfig, TransportProfile, UnixProfile,
    load_transport_config, load_transport_config_from_sources, parse_transport_config,
    validate_client_config_root_sections,
};
pub use transport_settings::{
    TransportProfileKind, TransportProfileSource, TransportProfileSummary,
    TransportSettingsSnapshot, load_transport_settings_snapshot,
    load_transport_settings_snapshot_from_sources, parse_transport_snapshot,
    save_default_transport_profile,
};
