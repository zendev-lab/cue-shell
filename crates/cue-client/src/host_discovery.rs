use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::config_paths::read_config_source;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HostDiscoveryConfig {
    #[serde(default)]
    pub env_hosts: Vec<String>,
    #[serde(default)]
    pub env_endpoints: Vec<String>,
    #[serde(default)]
    pub env_hostfiles: Vec<String>,
    #[serde(default)]
    pub env_bracket_ranges: Vec<String>,
}

impl HostDiscoveryConfig {
    pub fn is_empty(&self) -> bool {
        self.env_hosts.is_empty()
            && self.env_endpoints.is_empty()
            && self.env_hostfiles.is_empty()
            && self.env_bracket_ranges.is_empty()
    }
}

pub fn detected_configured_hosts(config: &HostDiscoveryConfig) -> Result<BTreeSet<String>> {
    detected_configured_hosts_from_vars(std::env::vars(), config)
}

fn detected_configured_hosts_from_vars<I, K, V>(
    vars: I,
    config: &HostDiscoveryConfig,
) -> Result<BTreeSet<String>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut hosts = BTreeSet::new();
    if config.is_empty() {
        return Ok(hosts);
    }

    for (key, value) in vars {
        let key = key.as_ref();
        let value = value.as_ref().trim();
        if value.is_empty() {
            continue;
        }

        if contains_key(&config.env_hosts, key) || contains_key(&config.env_endpoints, key) {
            collect_hosts_from_list(value, &mut hosts);
        }

        if contains_key(&config.env_hostfiles, key) {
            collect_hosts_from_hostfile(value, &mut hosts)?;
        }

        if contains_key(&config.env_bracket_ranges, key) {
            for host in expand_bracket_ranges(value) {
                if is_detectable_host(&host) {
                    hosts.insert(host);
                }
            }
        }
    }

    Ok(hosts)
}

fn contains_key(keys: &[String], key: &str) -> bool {
    keys.iter().any(|candidate| candidate == key)
}

fn collect_hosts_from_list(value: &str, hosts: &mut BTreeSet<String>) {
    for token in value.split(is_host_list_separator) {
        collect_host_token(token, hosts);
    }
}

fn is_host_list_separator(ch: char) -> bool {
    let code = ch as u32;
    code == 44 || code == 59 || ch.is_whitespace()
}

fn collect_hosts_from_hostfile(path: &str, hosts: &mut BTreeSet<String>) -> Result<()> {
    let path = Path::new(path);
    let Some(text) = read_config_source(path)? else {
        bail!("configured hostfile {} does not exist", path.display());
    };

    for line in text.lines() {
        let line = strip_comment(line).trim();
        if line.is_empty() {
            continue;
        }
        if let Some(first_field) = line.split_whitespace().next() {
            collect_host_token(first_field, hosts);
        }
    }
    Ok(())
}

fn collect_host_token(token: &str, hosts: &mut BTreeSet<String>) {
    let Some(host) = endpoint_host(token) else {
        return;
    };
    if is_detectable_host(host) {
        hosts.insert(host.to_string());
    }
}

fn endpoint_host(token: &str) -> Option<&str> {
    let token = trim_endpoint_token(token.trim());
    if token.is_empty() {
        return None;
    }

    let token = token
        .strip_prefix("tcp://")
        .or_else(|| token.strip_prefix("udp://"))
        .or_else(|| token.strip_prefix("http://"))
        .or_else(|| token.strip_prefix("https://"))
        .unwrap_or(token);

    let token = token.split("/").next().unwrap_or(token);
    if token.is_empty() {
        return None;
    }

    if let Some(rest) = token.strip_prefix("[") {
        return rest.split("]").next().filter(|host| !host.is_empty());
    }

    match token.rsplit_once(":") {
        Some((host, port))
            if !host.contains(":") && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            Some(host)
        }
        _ => Some(token),
    }
}

fn trim_endpoint_token(mut token: &str) -> &str {
    loop {
        let Some(byte) = token.as_bytes().first().copied() else {
            return token;
        };
        if matches!(byte, 34 | 39 | 91 | 93) {
            token = &token[1..];
        } else {
            break;
        }
    }

    loop {
        let Some(byte) = token.as_bytes().last().copied() else {
            return token;
        };
        if matches!(byte, 34 | 39 | 91 | 93) {
            token = &token[..token.len() - 1];
        } else {
            break;
        }
    }

    token
}

fn is_detectable_host(host: &str) -> bool {
    let host = host.trim();
    is_explicit_host(host)
        && !host.eq_ignore_ascii_case("localhost")
        && host != "127.0.0.1"
        && host != "::1"
        && !host.bytes().all(|byte| byte.is_ascii_digit())
}

fn expand_bracket_ranges(value: &str) -> Vec<String> {
    let mut hosts = Vec::new();
    for part in split_top_level_commas(value) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        expand_bracket_range_part(part, &mut hosts);
    }
    hosts
}

fn split_top_level_commas(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;

    for (index, byte) in value.bytes().enumerate() {
        match byte {
            91 => depth += 1,
            93 => depth = depth.saturating_sub(1),
            44 if depth == 0 => {
                parts.push(&value[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&value[start..]);
    parts
}

fn expand_bracket_range_part(part: &str, hosts: &mut Vec<String>) {
    let Some(open) = part.find("[") else {
        hosts.push(part.to_string());
        return;
    };
    let Some(close_offset) = part[open + 1..].find("]") else {
        hosts.push(part.to_string());
        return;
    };

    let close = open + 1 + close_offset;
    let prefix = &part[..open];
    let body = &part[open + 1..close];
    let suffix = &part[close + 1..];

    for range in body.split(",") {
        if let Some((start, end)) = range.split_once("-") {
            let width = start.len().max(end.len());
            let Ok(start_num) = start.parse::<u64>() else {
                continue;
            };
            let Ok(end_num) = end.parse::<u64>() else {
                continue;
            };
            if start_num > end_num || end_num - start_num > 4096 {
                continue;
            }
            for number in start_num..=end_num {
                hosts.push(format!("{prefix}{number:0width$}{suffix}"));
            }
        } else if !range.is_empty() {
            hosts.push(format!("{prefix}{range}{suffix}"));
        }
    }
}

fn strip_comment(line: &str) -> &str {
    line.split("#").next().unwrap_or(line)
}

fn is_explicit_host(host: &str) -> bool {
    !host.is_empty()
        && host != "*"
        && !host.starts_with("!")
        && !host.contains("*")
        && !host.contains("?")
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, 46 | 45 | 95 | 58))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn env_host_discovery_is_opt_in() {
        let hosts = detected_configured_hosts_from_vars(
            [("CLUSTER_HOSTS", "node-a,node-b")],
            &HostDiscoveryConfig::default(),
        )
        .unwrap();

        assert!(hosts.is_empty());
    }

    #[test]
    fn detects_configured_env_hosts_and_endpoints() {
        let config = HostDiscoveryConfig {
            env_hosts: vec!["CLUSTER_HOSTS".into()],
            env_endpoints: vec!["CLUSTER_ENDPOINTS".into()],
            ..Default::default()
        };
        let hosts = detected_configured_hosts_from_vars(
            [
                ("CLUSTER_HOSTS", "node-a,node-b"),
                ("CLUSTER_ENDPOINTS", "10.0.0.1:40104,10.0.0.2:40105"),
                ("IGNORED_HOSTS", "node-c"),
            ],
            &config,
        )
        .unwrap();

        assert_eq!(
            hosts.into_iter().collect::<Vec<_>>(),
            vec![
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
                "node-a".to_string(),
                "node-b".to_string(),
            ]
        );
    }

    #[test]
    fn ignores_loopback_and_count_tokens() {
        let config = HostDiscoveryConfig {
            env_hosts: vec!["HOSTS".into()],
            env_endpoints: vec!["ENDPOINTS".into()],
            ..Default::default()
        };
        let hosts = detected_configured_hosts_from_vars(
            [
                ("HOSTS", "batch 4 worker-a 8 localhost 127.0.0.1"),
                ("ENDPOINTS", "10.0.0.1:40104;10.0.0.2:40105"),
            ],
            &config,
        )
        .unwrap();

        assert_eq!(
            hosts.into_iter().collect::<Vec<_>>(),
            vec![
                "10.0.0.1".to_string(),
                "10.0.0.2".to_string(),
                "batch".to_string(),
                "worker-a".to_string(),
            ]
        );
    }

    #[test]
    fn expands_configured_bracket_ranges() {
        let config = HostDiscoveryConfig {
            env_bracket_ranges: vec!["NODELIST".into()],
            ..Default::default()
        };
        let hosts =
            detected_configured_hosts_from_vars([("NODELIST", "gpu-[01-03,08],login")], &config)
                .unwrap();

        assert_eq!(
            hosts.into_iter().collect::<Vec<_>>(),
            vec![
                "gpu-01".to_string(),
                "gpu-02".to_string(),
                "gpu-03".to_string(),
                "gpu-08".to_string(),
                "login".to_string(),
            ]
        );
    }

    #[test]
    fn configured_hostfile_read_failure_is_reported() {
        let missing = std::env::temp_dir().join(format!(
            "cue-client-missing-hostfile-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let missing = missing.to_string_lossy().into_owned();
        let config = HostDiscoveryConfig {
            env_hostfiles: vec!["HOSTFILE".into()],
            ..Default::default()
        };

        let error = detected_configured_hosts_from_vars([("HOSTFILE", missing.as_str())], &config)
            .expect_err("missing configured hostfile should be visible");

        assert!(format!("{error:#}").contains("configured hostfile"));
    }
}
