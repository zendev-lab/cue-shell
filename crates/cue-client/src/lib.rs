//! Shared client connection stack for cue-shell frontends.

mod client;
mod reconnect;

pub use client::{
    ClientReader, ClientWriter, CuedClient, WriterHandle, default_socket_path, spawn_writer_task,
};
pub use reconnect::{
    ClientConnector, ConnectionEvent, DEFAULT_RECONNECT_DELAY, ReconnectCmd,
    run_connection_manager, run_connection_manager_with_delay, run_socket_manager,
    run_socket_manager_with_delay, spawn_connection_manager, spawn_connection_manager_controllable,
    spawn_connection_manager_controllable_with_delay, spawn_connection_manager_with_delay,
    spawn_socket_manager, spawn_socket_manager_with_delay,
};
