//! Shared client connection stack for cue-shell frontends.

mod client;
mod config_paths;
mod reconnect;
mod restart;
mod ssh_config;
mod ssh_transport;
mod transport_config;
mod transport_settings;

pub use client::{
    ClientReader, CuedClient, MultiplexedClient, WriterHandle, WriterSendError, default_socket_path,
};
pub use config_paths::{
    ClientConfigPaths, ClientConfigSource, ClientConfigSources, client_config_paths,
    optional_client_config_paths, read_client_config_sources,
};
pub use reconnect::{
    ClientConnector, ConnectionControlError, ConnectionController, ConnectionEvent,
    spawn_connection_manager_controllable,
};
pub use restart::RestartHandle;
pub use ssh_transport::{connect_ssh_transport, transport_connector};
pub use transport_config::{
    ResolvedTransport, TransportConfig, load_transport_config, validate_client_config_root_sections,
};
pub use transport_settings::{
    TransportProfileSource, TransportProfileSummary, TransportSettingsSnapshot,
    load_transport_settings_snapshot, save_default_transport_profile,
};
