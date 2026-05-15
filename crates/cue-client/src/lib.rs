//! Shared client connection stack for cue-shell frontends.

mod client;
mod config_paths;
mod reconnect;
mod restart;
mod ssh_config;

pub use client::{
    ClientReader, ClientWriter, CuedClient, MultiplexedClient, WriterHandle, WriterSendError,
    default_socket_path, spawn_writer_task,
};
pub use config_paths::{
    client_config_path, config_dir, home_dir, legacy_config_path, read_config_source,
};
pub use reconnect::{
    ClientConnector, ConnectionEvent, DEFAULT_RECONNECT_DELAY, ReconnectCmd,
    run_connection_manager, run_connection_manager_with_delay, run_socket_manager,
    run_socket_manager_with_delay, spawn_connection_manager, spawn_connection_manager_controllable,
    spawn_connection_manager_controllable_with_delay, spawn_connection_manager_with_delay,
    spawn_socket_manager, spawn_socket_manager_with_delay,
};
pub use restart::RestartHandle;
pub use ssh_config::detected_ssh_hosts;
