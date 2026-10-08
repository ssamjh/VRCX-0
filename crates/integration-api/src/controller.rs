use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::auth::{generate_integration_api_token, IntegrationApiAuthPolicy};
use crate::config::{
    IntegrationApiConfigStore, DEFAULT_INTEGRATION_API_PORT, INTEGRATION_API_ALLOW_LAN_CONFIG_KEY,
    INTEGRATION_API_ENABLED_CONFIG_KEY, INTEGRATION_API_PORT_CONFIG_KEY,
    INTEGRATION_API_TOKEN_CONFIG_KEY,
};
use crate::state::RoomState;
use crate::transport::{
    bind_integration_api_listener, build_integration_api_router, IntegrationApiRouterState,
    ServerHub,
};
use crate::types::{IntegrationApiFailure, IntegrationApiServerState, IntegrationApiStatus};
use crate::wire::ByeReason;
use crate::IntegrationApiError;

pub struct IntegrationApiController {
    config: Arc<dyn IntegrationApiConfigStore>,
    app_version: String,
    active_connections: Arc<AtomicU32>,
    next_listener_id: AtomicU64,
    state: Arc<tokio::sync::Mutex<ControllerState>>,
}

struct ControllerState {
    game_running: bool,
    token: String,
    handle: Option<ListenerHandle>,
    hub: Option<Arc<ServerHub>>,
    last_error: Option<IntegrationApiFailure>,
    session_generation: u64,
}

const LISTENER_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SESSION_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

struct ListenerHandle {
    id: u64,
    port: u16,
    accept_cancel: CancellationToken,
    session_cancel: CancellationToken,
    sessions: TaskTracker,
    join: JoinHandle<()>,
}

impl IntegrationApiController {
    pub fn new(
        config: Arc<dyn IntegrationApiConfigStore>,
        app_version: String,
    ) -> Result<Self, IntegrationApiError> {
        let token = ensure_token(config.as_ref())?;
        Ok(Self {
            config,
            app_version,
            active_connections: Arc::new(AtomicU32::new(0)),
            next_listener_id: AtomicU64::new(0),
            state: Arc::new(tokio::sync::Mutex::new(ControllerState {
                game_running: false,
                token,
                handle: None,
                hub: None,
                last_error: None,
                session_generation: 0,
            })),
        })
    }

    pub async fn start_from_config(&self) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let mut state = self.state.lock().await;
        if !self.configured_enabled()? || !state.game_running || state.handle.is_some() {
            return self.status_locked(&state);
        }
        if let Err(error) = self.start_new_session(&mut state).await {
            state.last_error = Some(IntegrationApiFailure::from_error(&error));
            return Err(error);
        }
        self.status_locked(&state)
    }

    pub async fn set_game_running(
        &self,
        game_running: bool,
    ) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let mut state = self.state.lock().await;
        if state.game_running == game_running {
            return self.status_locked(&state);
        }
        state.game_running = game_running;
        if game_running {
            if self.configured_enabled()? {
                if let Err(error) = self.start_new_session(&mut state).await {
                    state.last_error = Some(IntegrationApiFailure::from_error(&error));
                    return Err(error);
                }
            }
        } else {
            if let Some(hub) = state.hub.take() {
                hub.send_bye(ByeReason::GameStopped);
                hub.clear();
            }
            if let Some(handle) = state.handle.take() {
                self.stop_listener(handle, true).await;
            }
            state.session_generation = state.session_generation.saturating_add(1);
            state.last_error = None;
        }
        self.status_locked(&state)
    }

    pub async fn publish(&self, room: Option<RoomState>) {
        let state = self.state.lock().await;
        if state.handle.is_some() {
            if let Some(hub) = &state.hub {
                hub.publish(room);
            }
        }
    }

    pub async fn set_enabled(
        &self,
        enabled: bool,
    ) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let mut state = self.state.lock().await;
        if enabled == self.configured_enabled()? {
            return self.status_locked(&state);
        }
        if enabled {
            if state.game_running {
                let port = self.configured_port()?;
                let allow_lan_connections = self.configured_allow_lan_connections()?;
                let hub = Arc::new(ServerHub::new(self.app_version.clone()));
                let handle = match self
                    .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
                    .await
                {
                    Ok(handle) => handle,
                    Err(error) => {
                        state.last_error = Some(IntegrationApiFailure::from_error(&error));
                        return Err(error);
                    }
                };
                if let Err(error) = self
                    .config
                    .set_bool(INTEGRATION_API_ENABLED_CONFIG_KEY, true)
                {
                    self.stop_listener(handle, false).await;
                    state.last_error = Some(IntegrationApiFailure::from_error(&error));
                    return Err(error);
                }
                state.handle = Some(handle);
                state.hub = Some(hub);
                state.session_generation = state.session_generation.saturating_add(1);
            } else {
                self.config
                    .set_bool(INTEGRATION_API_ENABLED_CONFIG_KEY, true)?;
            }
        } else {
            self.config
                .set_bool(INTEGRATION_API_ENABLED_CONFIG_KEY, false)?;
            if let Some(hub) = state.hub.take() {
                hub.send_bye(ByeReason::Disabled);
                hub.clear();
            }
            if let Some(handle) = state.handle.take() {
                self.stop_listener(handle, true).await;
            }
            state.session_generation = state.session_generation.saturating_add(1);
        }
        state.last_error = None;
        self.status_locked(&state)
    }

    pub async fn set_port(&self, port: u16) -> Result<IntegrationApiStatus, IntegrationApiError> {
        validate_port(port)?;
        let mut state = self.state.lock().await;
        let previous_port = self.configured_port()?;
        if port == previous_port {
            return self.status_locked(&state);
        }
        let allow_lan_connections = self.configured_allow_lan_connections()?;
        let should_run = self.configured_enabled()? && state.game_running;
        let Some(hub) = state.hub.clone() else {
            if should_run {
                let hub = Arc::new(ServerHub::new(self.app_version.clone()));
                let handle = match self
                    .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
                    .await
                {
                    Ok(handle) => handle,
                    Err(error) => {
                        state.last_error = Some(IntegrationApiFailure::from_error(&error));
                        return Err(error);
                    }
                };
                if let Err(error) = self
                    .config
                    .set_string(INTEGRATION_API_PORT_CONFIG_KEY, &port.to_string())
                {
                    self.stop_listener(handle, false).await;
                    state.last_error = Some(IntegrationApiFailure::from_error(&error));
                    return Err(error);
                }
                state.handle = Some(handle);
                state.hub = Some(hub);
                state.session_generation = state.session_generation.saturating_add(1);
                state.last_error = None;
                return self.status_locked(&state);
            }
            drop(bind_integration_api_listener(port, allow_lan_connections)?);
            self.config
                .set_string(INTEGRATION_API_PORT_CONFIG_KEY, &port.to_string())?;
            state.last_error = None;
            return self.status_locked(&state);
        };

        let new_handle = match self
            .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                state.last_error = Some(IntegrationApiFailure::from_error(&error));
                return Err(error);
            }
        };
        if let Err(error) = self
            .config
            .set_string(INTEGRATION_API_PORT_CONFIG_KEY, &port.to_string())
        {
            self.stop_listener(new_handle, false).await;
            state.last_error = Some(IntegrationApiFailure::from_error(&error));
            return Err(error);
        }
        let previous_handle = state.handle.replace(new_handle);
        state.last_error = None;
        if let Some(previous_handle) = previous_handle {
            self.stop_listener(previous_handle, false).await;
        }
        self.status_locked(&state)
    }

    pub async fn set_allow_lan_connections(
        &self,
        allow_lan_connections: bool,
    ) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let mut state = self.state.lock().await;
        let previous_allow_lan_connections = self.configured_allow_lan_connections()?;
        if allow_lan_connections == previous_allow_lan_connections {
            return self.status_locked(&state);
        }
        let port = self.configured_port()?;
        let should_run = self.configured_enabled()? && state.game_running;
        let Some(hub) = state.hub.clone() else {
            if should_run {
                let hub = Arc::new(ServerHub::new(self.app_version.clone()));
                let handle = match self
                    .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
                    .await
                {
                    Ok(handle) => handle,
                    Err(error) => {
                        state.last_error = Some(IntegrationApiFailure::from_error(&error));
                        return Err(error);
                    }
                };
                if let Err(error) = self
                    .config
                    .set_bool(INTEGRATION_API_ALLOW_LAN_CONFIG_KEY, allow_lan_connections)
                {
                    self.stop_listener(handle, false).await;
                    state.last_error = Some(IntegrationApiFailure::from_error(&error));
                    return Err(error);
                }
                state.handle = Some(handle);
                state.hub = Some(hub);
                state.session_generation = state.session_generation.saturating_add(1);
                state.last_error = None;
                return self.status_locked(&state);
            }
            drop(bind_integration_api_listener(port, allow_lan_connections)?);
            self.config
                .set_bool(INTEGRATION_API_ALLOW_LAN_CONFIG_KEY, allow_lan_connections)?;
            state.last_error = None;
            return self.status_locked(&state);
        };
        let previous_handle = state.handle.take();
        if let Some(previous_handle) = previous_handle {
            self.stop_listener(previous_handle, false).await;
        }
        let new_handle = match self
            .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                state.handle = self
                    .start_listener(port, previous_allow_lan_connections, &state.token, hub)
                    .await
                    .ok();
                state.last_error = Some(IntegrationApiFailure::from_error(&error));
                return Err(error);
            }
        };
        if let Err(error) = self
            .config
            .set_bool(INTEGRATION_API_ALLOW_LAN_CONFIG_KEY, allow_lan_connections)
        {
            self.stop_listener(new_handle, false).await;
            state.handle = self
                .start_listener(port, previous_allow_lan_connections, &state.token, hub)
                .await
                .ok();
            state.last_error = Some(IntegrationApiFailure::from_error(&error));
            return Err(error);
        }
        state.handle = Some(new_handle);
        state.last_error = None;
        self.status_locked(&state)
    }

    pub async fn rotate_token(&self) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let token = generate_integration_api_token()?;
        let mut state = self.state.lock().await;
        let Some(hub) = state.hub.clone() else {
            self.config
                .set_string(INTEGRATION_API_TOKEN_CONFIG_KEY, &token)?;
            state.token = token;
            state.last_error = None;
            return self.status_locked(&state);
        };
        let port = self.configured_port()?;
        let allow_lan_connections = self.configured_allow_lan_connections()?;
        let previous_token = state.token.clone();
        let previous_handle = state.handle.take();
        if let Some(previous_handle) = previous_handle {
            self.stop_listener(previous_handle, false).await;
        }
        let new_handle = match self
            .start_listener(port, allow_lan_connections, &token, Arc::clone(&hub))
            .await
        {
            Ok(handle) => handle,
            Err(error) => {
                state.handle = self
                    .start_listener(port, allow_lan_connections, &previous_token, hub)
                    .await
                    .ok();
                state.last_error = Some(IntegrationApiFailure::from_error(&error));
                return Err(error);
            }
        };
        if let Err(error) = self
            .config
            .set_string(INTEGRATION_API_TOKEN_CONFIG_KEY, &token)
        {
            self.stop_listener(new_handle, false).await;
            state.handle = self
                .start_listener(port, allow_lan_connections, &previous_token, hub)
                .await
                .ok();
            state.last_error = Some(IntegrationApiFailure::from_error(&error));
            return Err(error);
        }
        state.token = token;
        state.handle = Some(new_handle);
        state.last_error = None;
        self.status_locked(&state)
    }

    pub async fn status(&self) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let state = self.state.lock().await;
        self.status_locked(&state)
    }

    pub async fn running_generation(&self) -> Option<u64> {
        let state = self.state.lock().await;
        state.handle.as_ref().map(|_| state.session_generation)
    }

    pub async fn publish_if_generation(&self, generation: u64, room: Option<RoomState>) -> bool {
        let state = self.state.lock().await;
        if state.handle.is_none() || state.session_generation != generation {
            return false;
        }
        let Some(hub) = &state.hub else {
            return false;
        };
        hub.publish(room);
        true
    }

    async fn start_new_session(
        &self,
        state: &mut ControllerState,
    ) -> Result<(), IntegrationApiError> {
        let port = self.configured_port()?;
        let allow_lan_connections = self.configured_allow_lan_connections()?;
        let hub = Arc::new(ServerHub::new(self.app_version.clone()));
        let handle = self
            .start_listener(port, allow_lan_connections, &state.token, Arc::clone(&hub))
            .await?;
        state.handle = Some(handle);
        state.hub = Some(hub);
        state.session_generation = state.session_generation.saturating_add(1);
        state.last_error = None;
        Ok(())
    }

    async fn start_listener(
        &self,
        port: u16,
        allow_lan_connections: bool,
        token: &str,
        hub: Arc<ServerHub>,
    ) -> Result<ListenerHandle, IntegrationApiError> {
        let listener = bind_integration_api_listener(port, allow_lan_connections)?;
        let bound_port = listener.local_addr()?.port();
        let id = self.next_listener_id.fetch_add(1, Ordering::AcqRel) + 1;
        let accept_cancel = CancellationToken::new();
        let session_cancel = CancellationToken::new();
        let sessions = TaskTracker::new();
        let router = build_integration_api_router(IntegrationApiRouterState {
            policy: IntegrationApiAuthPolicy {
                port: bound_port,
                token: token.into(),
                allow_lan_connections,
            },
            hub,
            active_connections: Arc::clone(&self.active_connections),
            session_cancel: session_cancel.clone(),
            sessions: sessions.clone(),
        });
        let shutdown = accept_cancel.clone();
        let expected_shutdown = accept_cancel.clone();
        let controller_state = Arc::clone(&self.state);
        let exit_session_cancel = session_cancel.clone();
        let join = tokio::spawn(async move {
            let result = axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled_owned().await })
                .await;
            if expected_shutdown.is_cancelled() {
                return;
            }
            let failure = match result {
                Ok(()) => IntegrationApiFailure {
                    code: crate::types::IntegrationApiFailureCode::Io,
                    message: "Integration API listener stopped unexpectedly".into(),
                    port: Some(bound_port),
                },
                Err(error) => {
                    tracing::warn!(error = %error, "Integration API listener stopped with error");
                    IntegrationApiFailure::from_error(&IntegrationApiError::Bind {
                        port: bound_port,
                        source: error,
                    })
                }
            };
            exit_session_cancel.cancel();
            Self::record_listener_exit(controller_state, id, failure).await;
        });
        Ok(ListenerHandle {
            id,
            port: bound_port,
            accept_cancel,
            session_cancel,
            sessions,
            join,
        })
    }

    async fn record_listener_exit(
        controller_state: Arc<tokio::sync::Mutex<ControllerState>>,
        listener_id: u64,
        failure: IntegrationApiFailure,
    ) {
        let mut state = controller_state.lock().await;
        if state
            .handle
            .as_ref()
            .is_none_or(|handle| handle.id != listener_id)
        {
            return;
        }
        if let Some(handle) = state.handle.take() {
            handle.accept_cancel.cancel();
            handle.session_cancel.cancel();
            handle.sessions.close();
        }
        if let Some(hub) = state.hub.take() {
            hub.clear();
        }
        state.session_generation = state.session_generation.saturating_add(1);
        state.last_error = Some(failure);
    }

    async fn stop_listener(&self, handle: ListenerHandle, wait_for_bye: bool) {
        handle.accept_cancel.cancel();
        if !wait_for_bye {
            handle.session_cancel.cancel();
        }
        let mut join = handle.join;
        if tokio::time::timeout(LISTENER_STOP_TIMEOUT, &mut join)
            .await
            .is_err()
        {
            join.abort();
        }
        handle.session_cancel.cancel();
        handle.sessions.close();
        if tokio::time::timeout(SESSION_DRAIN_TIMEOUT, handle.sessions.wait())
            .await
            .is_err()
        {
            tracing::warn!("Integration API sessions did not exit before the drain timeout");
        }
    }

    fn configured_enabled(&self) -> Result<bool, IntegrationApiError> {
        self.config
            .get_bool(INTEGRATION_API_ENABLED_CONFIG_KEY, false)
    }

    fn configured_allow_lan_connections(&self) -> Result<bool, IntegrationApiError> {
        self.config
            .get_bool(INTEGRATION_API_ALLOW_LAN_CONFIG_KEY, false)
    }

    fn configured_port(&self) -> Result<u16, IntegrationApiError> {
        let raw = self.config.get_string(
            INTEGRATION_API_PORT_CONFIG_KEY,
            &DEFAULT_INTEGRATION_API_PORT.to_string(),
        )?;
        let port = raw
            .trim()
            .parse::<u16>()
            .unwrap_or(DEFAULT_INTEGRATION_API_PORT);
        if port < 1024 {
            return Ok(DEFAULT_INTEGRATION_API_PORT);
        }
        Ok(port)
    }

    fn status_locked(
        &self,
        state: &ControllerState,
    ) -> Result<IntegrationApiStatus, IntegrationApiError> {
        let enabled = self.configured_enabled()?;
        let allow_lan_connections = self.configured_allow_lan_connections()?;
        let configured_port = self.configured_port()?;
        let server_state = if state.handle.is_some() {
            IntegrationApiServerState::Running
        } else if state.last_error.is_some() {
            IntegrationApiServerState::Error
        } else if enabled && !state.game_running {
            IntegrationApiServerState::WaitingForGame
        } else if enabled {
            IntegrationApiServerState::Error
        } else {
            IntegrationApiServerState::Disabled
        };
        Ok(IntegrationApiStatus {
            enabled,
            allow_lan_connections,
            state: server_state,
            port: state
                .handle
                .as_ref()
                .map(|handle| handle.port)
                .unwrap_or(configured_port),
            token: state.token.clone(),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            last_error: state.last_error.clone(),
        })
    }
}

fn ensure_token(config: &dyn IntegrationApiConfigStore) -> Result<String, IntegrationApiError> {
    let existing = config.get_string(INTEGRATION_API_TOKEN_CONFIG_KEY, "")?;
    if !existing.trim().is_empty() {
        return Ok(existing);
    }
    let token = generate_integration_api_token()?;
    config.set_string(INTEGRATION_API_TOKEN_CONFIG_KEY, &token)?;
    Ok(token)
}

fn validate_port(port: u16) -> Result<(), IntegrationApiError> {
    if port < 1024 {
        return Err(IntegrationApiError::InvalidPort { port });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
