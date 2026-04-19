//! Compatibility re-exports for the shared `cue-client` crate.

pub use cue_client::{
    ClientConnector, ClientReader, ClientWriter, ConnectionEvent, CuedClient,
    DEFAULT_RECONNECT_DELAY, ReconnectCmd, WriterHandle, default_socket_path,
    run_connection_manager, run_connection_manager_with_delay, run_socket_manager,
    run_socket_manager_with_delay, spawn_connection_manager, spawn_connection_manager_controllable,
    spawn_connection_manager_controllable_with_delay, spawn_connection_manager_with_delay,
    spawn_socket_manager, spawn_socket_manager_with_delay, spawn_writer_task,
};
