use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Result, bail};

use crate::config_paths::read_config_source;

pub(crate) fn detected_ssh_hosts() -> Result<BTreeSet<String>> {
    let Some(home) = std::env::var_os("HOME").filter(|value| !value.is_empty()) else {
        bail!("HOME is not set; set HOME or disable transport.auto_detect_ssh");
    };
    let path = PathBuf::from(home).join(".ssh/config");
    let text = match read_config_source(&path) {
        Ok(Some(text)) => text,
        Ok(None) => return Ok(BTreeSet::new()),
        Err(error) => return Err(error),
    };

    Ok(parse_ssh_config_hosts(&text))
}

fn parse_ssh_config_hosts(text: &str) -> BTreeSet<String> {
    let mut hosts = BTreeSet::new();

    for line in text.lines() {
        let line = strip_comment(line).trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let Some(keyword) = parts.next() else {
            continue;
        };
        if !keyword.eq_ignore_ascii_case("host") {
            continue;
        }

        for host in parts {
            if is_explicit_host(host) {
                hosts.insert(host.to_string());
            }
        }
    }

    hosts
}

fn strip_comment(line: &str) -> &str {
    line.split('#').next().unwrap_or(line)
}

fn is_explicit_host(host: &str) -> bool {
    !host.is_empty()
        && host != "*"
        && !host.starts_with('!')
        && !host.contains('*')
        && !host.contains('?')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_explicit_hosts_and_ignores_patterns() {
        let hosts = parse_ssh_config_hosts(
            r#"
Host *
  ServerAliveInterval 30

Host devbox prod-box
  User me

Host !banned *.corp ???
  User ignored

Host nested # trailing comment
  HostName example.com
"#,
        );

        assert_eq!(
            hosts.into_iter().collect::<Vec<_>>(),
            vec![
                "devbox".to_string(),
                "nested".to_string(),
                "prod-box".to_string()
            ]
        );
    }
}
