use cue_core::scope::EnvSnapshot;

pub(crate) fn expand_command_line(
    command_line: &[String],
    snapshot: Option<&EnvSnapshot>,
) -> Vec<String> {
    command_line
        .iter()
        .map(|word| expand_word(word, snapshot))
        .collect()
}

fn expand_word(word: &str, snapshot: Option<&EnvSnapshot>) -> String {
    let with_tilde = expand_tilde(word, snapshot);
    expand_vars(&with_tilde, snapshot)
}

fn expand_tilde(word: &str, snapshot: Option<&EnvSnapshot>) -> String {
    let Some(home) = lookup_env(snapshot, "HOME") else {
        return word.to_string();
    };

    if word == "~" {
        return home.to_string();
    }

    let Some(rest) = word.strip_prefix("~/") else {
        return word.to_string();
    };

    std::path::PathBuf::from(home)
        .join(rest)
        .to_string_lossy()
        .into_owned()
}

fn expand_vars(word: &str, snapshot: Option<&EnvSnapshot>) -> String {
    let mut out = String::with_capacity(word.len());
    let mut chars = word.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' if chars.peek() == Some(&'$') => {
                chars.next();
                out.push('$');
            }
            '$' => match chars.peek().copied() {
                Some('{') => {
                    chars.next();
                    let mut name = String::new();
                    let mut closed = false;
                    for next in chars.by_ref() {
                        if next == '}' {
                            closed = true;
                            break;
                        }
                        name.push(next);
                    }

                    if closed && is_valid_var_name(&name) {
                        out.push_str(lookup_env(snapshot, &name).unwrap_or_default());
                    } else {
                        out.push_str("${");
                        out.push_str(&name);
                        if closed {
                            out.push('}');
                        }
                    }
                }
                Some(next) if is_var_start(next) => {
                    let mut name = String::new();
                    name.push(chars.next().expect("peeked variable start"));
                    while let Some(next) = chars.peek().copied() {
                        if !is_var_continue(next) {
                            break;
                        }
                        name.push(chars.next().expect("peeked variable continuation"));
                    }
                    out.push_str(lookup_env(snapshot, &name).unwrap_or_default());
                }
                _ => out.push('$'),
            },
            _ => out.push(ch),
        }
    }

    out
}

fn lookup_env<'a>(snapshot: Option<&'a EnvSnapshot>, key: &str) -> Option<&'a str> {
    snapshot
        .and_then(|snap| snap.env.get(key))
        .map(String::as_str)
}

fn is_var_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_var_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn is_valid_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if is_var_start(ch) => chars.all(is_var_continue),
        _ => false,
    }
}
