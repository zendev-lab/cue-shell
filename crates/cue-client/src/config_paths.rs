use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const APP_DIR: &str = "cue-shell";
const CLIENT_CONFIG_FILE: &str = "client.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfigPaths {
    client: PathBuf,
}

impl ClientConfigPaths {
    fn from_dir(dir: PathBuf) -> Self {
        Self {
            client: dir.join(CLIENT_CONFIG_FILE),
        }
    }

    pub fn client(&self) -> &Path {
        &self.client
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfigSource {
    path: PathBuf,
    text: String,
}

impl ClientConfigSource {
    fn new(path: PathBuf, text: String) -> Self {
        Self { path, text }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfigSources {
    client: Option<ClientConfigSource>,
}

impl ClientConfigSources {
    fn new(client: Option<ClientConfigSource>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> Option<&ClientConfigSource> {
        self.client.as_ref()
    }

    pub fn primary(&self) -> Option<&ClientConfigSource> {
        self.client()
    }
}

fn config_dir() -> Result<PathBuf> {
    config_dir_from_env(
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
    )
}

pub fn client_config_paths() -> Result<ClientConfigPaths> {
    Ok(ClientConfigPaths::from_dir(config_dir()?))
}

pub fn optional_client_config_paths() -> Option<ClientConfigPaths> {
    optional_config_dir_from_env(
        std::env::var_os("XDG_CONFIG_HOME"),
        std::env::var_os("HOME"),
    )
    .map(ClientConfigPaths::from_dir)
}

pub fn read_client_config_sources(paths: &ClientConfigPaths) -> Result<ClientConfigSources> {
    Ok(ClientConfigSources::new(
        read_config_source(paths.client())?
            .map(|text| ClientConfigSource::new(paths.client().to_path_buf(), text)),
    ))
}

fn config_dir_from_env(
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    let Some(dir) = optional_config_dir_from_env(xdg_config_home, home) else {
        bail!("HOME is not set; set HOME or XDG_CONFIG_HOME to resolve cue-shell config paths");
    };
    Ok(dir)
}

fn optional_config_dir_from_env(
    xdg_config_home: Option<OsString>,
    home: Option<OsString>,
) -> Option<PathBuf> {
    if let Some(dir) = non_empty_env(xdg_config_home) {
        return Some(PathBuf::from(dir).join(APP_DIR));
    }
    non_empty_env(home).map(|home| PathBuf::from(home).join(".config").join(APP_DIR))
}

#[cfg(test)]
fn home_dir_from_env(home: Option<OsString>) -> Result<PathBuf> {
    let Some(home) = non_empty_env(home) else {
        bail!("HOME is not set; set HOME or XDG_CONFIG_HOME to resolve cue-shell config paths");
    };
    Ok(PathBuf::from(home))
}

fn non_empty_env(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

pub(crate) fn read_config_source(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read config {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_config_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cue-client-config-source-test-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp config dir");
        dir
    }

    #[test]
    fn home_dir_requires_home_instead_of_falling_back_to_cwd() {
        let error = home_dir_from_env(None).expect_err("missing HOME should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }

    #[test]
    fn home_dir_rejects_empty_home() {
        let error = home_dir_from_env(Some(OsString::new())).expect_err("empty HOME should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }

    #[test]
    fn config_dir_uses_xdg_config_home_without_home() {
        assert_eq!(
            config_dir_from_env(Some(OsString::from("/xdg")), None).unwrap(),
            PathBuf::from("/xdg").join(APP_DIR)
        );
    }

    #[test]
    fn config_dir_requires_home_when_xdg_config_home_is_missing() {
        let error = config_dir_from_env(None, None).expect_err("missing HOME and XDG should fail");

        assert!(format!("{error:#}").contains("HOME is not set"));
    }

    #[test]
    fn optional_config_paths_are_absent_without_home_or_xdg() {
        assert_eq!(optional_config_dir_from_env(None, None), None);
    }

    #[test]
    fn optional_config_paths_resolve_client_file() {
        assert_eq!(
            optional_config_dir_from_env(None, Some(OsString::from("/home/me")))
                .map(ClientConfigPaths::from_dir),
            Some(ClientConfigPaths {
                client: PathBuf::from("/home/me")
                    .join(".config")
                    .join(APP_DIR)
                    .join(CLIENT_CONFIG_FILE),
            })
        );
    }

    #[test]
    fn read_client_config_sources_preserves_client_source() {
        let dir = temp_config_dir();
        let paths = ClientConfigPaths::from_dir(dir.clone());
        std::fs::write(paths.client(), "client = true\n").expect("write client config");

        let sources = read_client_config_sources(&paths).expect("read config sources");

        assert_eq!(
            sources.client().map(ClientConfigSource::path),
            Some(paths.client())
        );
        assert_eq!(
            sources.client().map(ClientConfigSource::text),
            Some("client = true\n")
        );
        assert_eq!(
            sources.primary().map(ClientConfigSource::path),
            Some(paths.client())
        );

        std::fs::remove_dir_all(dir).expect("remove temp config dir");
    }

    #[cfg(unix)]
    #[test]
    fn read_config_source_reports_symlink_loop_instead_of_treating_it_as_absent() {
        let dir = temp_config_dir();
        let path = dir.join("loop.toml");
        std::os::unix::fs::symlink(&path, &path).expect("create symlink loop");

        let error = read_config_source(&path).expect_err("symlink loop must be visible");

        assert!(format!("{error:#}").contains("read config"));
        std::fs::remove_file(path).expect("remove symlink loop");
        std::fs::remove_dir_all(dir).expect("remove temp config dir");
    }
}
