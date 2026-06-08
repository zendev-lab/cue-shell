//! Internal aliases for the shared `cue-client` crate.

pub(crate) use cue_client::{
    ClientConnector, ClientReader, ConnectionController, ConnectionEvent, RestartHandle,
    WriterHandle, spawn_connection_manager_controllable,
};

#[cfg(test)]
pub(crate) use cue_client::CuedClient;
