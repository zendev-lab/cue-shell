//! Tokenizer: raw input string → Vec<Spanned>.
//!
//! Context-sensitive `()` handling:
//! - `(` immediately after a `Command` token → `ModeParenOpen`
//! - `(` elsewhere → `GroupOpen`

use std::time::Duration;

use super::token::{IdKind, Span, Spanned, Token, Value};

/// Tokenizer state machine.
pub struct Tokenizer<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
    /// The last significant (non-whitespace) token kind, for `()` disambiguation.
    last_significant: Option<TokenClass>,
    /// Whether we are currently tokenizing `:cmd(...)` mode params.
    in_mode_params: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TokenClass {
    Command,
    Other,
}

/// Tokenizer error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("tokenizer error at byte {pos}: {message}")]
pub struct TokenizeError {
    pub pos: usize,
    pub message: String,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            last_significant: None,
            in_mode_params: false,
        }
    }

    /// Tokenize the entire input, returning all tokens including whitespace.
    pub fn tokenize(input: &str) -> Result<Vec<Spanned>, TokenizeError> {
        let mut t = Tokenizer::new(input);
        let mut tokens = Vec::new();
        loop {
            let spanned = t.next_token()?;
            let is_eof = spanned.token == Token::Eof;
            tokens.push(spanned);
            if is_eof {
                break;
            }
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let b = self.bytes.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    fn slice(&self, start: usize, end: usize) -> &'a str {
        &self.input[start..end]
    }

    fn next_token(&mut self) -> Result<Spanned, TokenizeError> {
        if self.pos >= self.bytes.len() {
            return Ok(Spanned {
                token: Token::Eof,
                span: Span::new(self.pos, self.pos),
            });
        }

        let start = self.pos;
        let b = self.bytes[self.pos];

        // Whitespace
        if b == b' ' || b == b'\t' {
            self.pos += 1;
            while self.pos < self.bytes.len()
                && (self.bytes[self.pos] == b' ' || self.bytes[self.pos] == b'\t')
            {
                self.pos += 1;
            }
            return Ok(Spanned {
                token: Token::Whitespace(self.slice(start, self.pos).into()),
                span: Span::new(start, self.pos),
            });
        }

        let tok = match b {
            b':' if start == 0 || self.last_significant.is_none() || self.is_colon_context() => {
                self.pos += 1;
                self.last_significant = Some(TokenClass::Other);

                // Try to read command name
                let cmd_start = self.pos;
                while self.pos < self.bytes.len() && is_ident_char(self.bytes[self.pos]) {
                    self.pos += 1;
                }
                if self.pos > cmd_start {
                    let name = self.slice(cmd_start, self.pos).to_string();
                    self.last_significant = Some(TokenClass::Command);
                    // Return Colon + Command as two tokens; but for simplicity
                    // in this pass, we return just Command (colon is implicit).
                    // The parser knows all commands start with `:`.
                    return Ok(Spanned {
                        token: Token::Command(name),
                        span: Span::new(start, self.pos),
                    });
                }
                Token::Colon
            }

            b'(' => {
                self.pos += 1;
                if self.last_significant == Some(TokenClass::Command) {
                    self.in_mode_params = true;
                    self.last_significant = Some(TokenClass::Other);
                    let tok = Token::ModeParenOpen;
                    // Read mode params until `)`
                    return self.tokenize_mode_params(start, tok);
                }
                self.last_significant = Some(TokenClass::Other);
                Token::GroupOpen
            }

            b')' => {
                self.pos += 1;
                if self.in_mode_params {
                    self.in_mode_params = false;
                }
                self.last_significant = Some(TokenClass::Other);
                Token::GroupClose
            }

            b'-' if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Token::SerialThen
            }

            b'~' if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Token::SerialAlways
            }

            b'|' => self.tokenize_pipe_or_parallel()?,

            _ => self.tokenize_word()?,
        };

        if !matches!(tok, Token::Whitespace(_)) {
            self.last_significant = Some(TokenClass::Other);
        }

        Ok(Spanned {
            token: tok,
            span: Span::new(start, self.pos),
        })
    }

    /// Check if `:` is in a position where it starts a command
    /// (start of input, or after whitespace/operator/group-open).
    fn is_colon_context(&self) -> bool {
        if self.pos == 0 {
            return true;
        }
        // Look at previous non-whitespace character
        let mut i = self.pos;
        while i > 0 {
            i -= 1;
            let c = self.bytes[i];
            if c == b' ' || c == b'\t' {
                continue;
            }
            return matches!(
                c,
                b'>' | b'(' | b'|' | b'?' // after operators or group open
            );
        }
        true // start of input
    }

    /// Tokenize after `(` when in mode-params context.
    /// Returns a sequence of param tokens, consuming up to and including `)`.
    fn tokenize_mode_params(
        &mut self,
        paren_start: usize,
        open_tok: Token,
    ) -> Result<Spanned, TokenizeError> {
        // We've already consumed `(`. We need to return ModeParenOpen first,
        // then subsequent calls will read key=value pairs.
        // But our tokenizer is single-token-at-a-time, so we store the open token
        // and let subsequent next_token calls handle the interior.

        // Actually, for simplicity in a single-pass tokenizer, let's collect
        // all mode params inline and return them as individual tokens.
        // We'll switch to a "mode params" sub-state.

        // For now, return just the ModeParenOpen. The parser will know to
        // expect params until ModeParenClose.
        Ok(Spanned {
            token: open_tok,
            span: Span::new(paren_start, self.pos),
        })
    }

    fn tokenize_pipe_or_parallel(&mut self) -> Result<Token, TokenizeError> {
        // Current char is `|`
        self.pos += 1;

        match self.peek() {
            Some(b'>') => {
                self.pos += 1;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeStdout)
            }
            Some(b'&') if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeAll)
            }
            Some(b'!') if self.peek_at(1) == Some(b'>') => {
                self.pos += 2;
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::PipeStderr)
            }
            Some(b'|') => {
                self.pos += 1;
                if self.peek() == Some(b'?') {
                    self.pos += 1;
                    self.last_significant = Some(TokenClass::Other);
                    Ok(Token::ParallelRace)
                } else {
                    self.last_significant = Some(TokenClass::Other);
                    Ok(Token::ParallelAll)
                }
            }
            _ => {
                // Bare `|` — treat as word for now (could be error)
                self.last_significant = Some(TokenClass::Other);
                Ok(Token::Word("|".into()))
            }
        }
    }

    fn tokenize_word(&mut self) -> Result<Token, TokenizeError> {
        let start = self.pos;

        // Check for ID ref: J1, C3, S0
        if let Some(kind) = self.try_id_kind() {
            let prefix_pos = self.pos;
            self.pos += 1; // skip prefix letter
            let num_start = self.pos;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            if self.pos > num_start {
                // Make sure next char is not alphanumeric (otherwise it's a regular word)
                if self.pos >= self.bytes.len() || !is_ident_char(self.bytes[self.pos]) {
                    let n: u32 = self.slice(num_start, self.pos).parse().unwrap_or(0);
                    self.last_significant = Some(TokenClass::Other);
                    return Ok(Token::IdRef(kind, n));
                }
            }
            // Not an ID ref, fall through to word
            self.pos = prefix_pos;
        }

        // Mode params interior tokens
        if self.in_mode_params && self.bytes[self.pos] == b'=' {
            self.pos += 1;
            self.last_significant = Some(TokenClass::Other);
            return Ok(Token::ParamEq);
        }
        if self.in_mode_params && self.bytes[self.pos] == b',' {
            self.pos += 1;
            self.last_significant = Some(TokenClass::Other);
            return Ok(Token::Comma);
        }

        // Quoted string (double quotes — escape sequences supported)
        if self.bytes[self.pos] == b'"' {
            return self.tokenize_quoted_string();
        }
        // Single-quoted string (literal — no escape sequences)
        if self.bytes[self.pos] == b'\'' {
            return self.tokenize_single_quoted_string();
        }

        // Regular word: gobble until delimiter or operator.
        while self.pos < self.bytes.len()
            && !is_delimiter(self.bytes[self.pos])
            && !(self.in_mode_params && self.bytes[self.pos] == b'=')
        {
            // Stop before any cue-shell operator (longest-match-first).
            if starts_with_operator(self.bytes, self.pos).is_some() {
                break;
            }
            self.pos += 1;
        }

        if self.pos == start {
            // Unknown character
            self.pos += 1;
            return Err(TokenizeError {
                pos: start,
                message: format!("unexpected character '{}'", self.slice(start, self.pos)),
            });
        }

        let text = self.slice(start, self.pos).to_string();
        self.last_significant = Some(TokenClass::Other);

        if self.in_mode_params
            && let Some(v) = try_parse_value(&text)
        {
            return Ok(Token::ParamValue(v));
        }

        Ok(Token::Word(text))
    }

    fn try_id_kind(&self) -> Option<IdKind> {
        match self.peek()? {
            b'J' if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) => Some(IdKind::Job),
            b'C' if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) => Some(IdKind::Cron),
            b'S' if self.peek_at(1).is_some_and(|b| b.is_ascii_digit()) => Some(IdKind::Scope),
            _ => None,
        }
    }

    fn tokenize_quoted_string(&mut self) -> Result<Token, TokenizeError> {
        let start = self.pos;
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(TokenizeError {
                        pos: start,
                        message: "unterminated string".into(),
                    });
                }
                Some(b'"') => break,
                Some(b'\\') => match self.advance() {
                    Some(b'"') => s.push('"'),
                    Some(b'\\') => s.push('\\'),
                    Some(b'n') => s.push('\n'),
                    Some(b't') => s.push('\t'),
                    Some(c) => {
                        s.push('\\');
                        s.push(c as char);
                    }
                    None => {
                        return Err(TokenizeError {
                            pos: self.pos,
                            message: "unterminated escape".into(),
                        });
                    }
                },
                Some(c) => s.push(c as char),
            }
        }
        self.last_significant = Some(TokenClass::Other);
        Ok(Token::Word(s))
    }

    /// Tokenize a single-quoted string literal.
    ///
    /// Single quotes capture everything literally until the closing `'`.
    /// Unlike double-quoted strings, there are no escape sequences.
    /// Unmatched quotes produce a `TokenizeError`.
    fn tokenize_single_quoted_string(&mut self) -> Result<Token, TokenizeError> {
        let start = self.pos;
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(TokenizeError {
                        pos: start,
                        message: "unterminated single-quoted string".into(),
                    });
                }
                Some(b'\'') => break,
                Some(c) => s.push(c as char),
            }
        }
        self.last_significant = Some(TokenClass::Other);
        Ok(Token::Word(s))
    }
}

/// Check whether `bytes[pos..]` starts with a cue-shell operator.
/// Operators are checked longest-match-first so `||?` is not split into `||` + `?`.
fn starts_with_operator(bytes: &[u8], pos: usize) -> Option<Token> {
    let tail = &bytes[pos..];
    if tail.len() < 2 {
        return None;
    }
    // longest match first
    if tail.starts_with(b"|&>") {
        return Some(Token::PipeAll);
    }
    if tail.starts_with(b"|!>") {
        return Some(Token::PipeStderr);
    }
    if tail.len() >= 3 && tail.starts_with(b"||?") {
        return Some(Token::ParallelRace);
    }
    if tail.starts_with(b"->") {
        return Some(Token::SerialThen);
    }
    if tail.starts_with(b"~>") {
        return Some(Token::SerialAlways);
    }
    if tail.starts_with(b"|>") {
        return Some(Token::PipeStdout);
    }
    if tail.starts_with(b"||") {
        return Some(Token::ParallelAll);
    }
    None
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn is_delimiter(b: u8) -> bool {
    matches!(
        b,
        b' ' | b'\t' | b'\n' | b'(' | b')' | b'|' | b':' | b',' | b'"'
    )
    // Note: `-` and `~` are NOT delimiters here.
    // The main tokenize loop handles `->` and `~>` as operators before
    // falling through to word tokenization, so `-` inside words (e.g. `--release`)
    // is correctly consumed as part of the word.
}

/// Try to parse a word as a typed value (for mode params).
fn try_parse_value(s: &str) -> Option<Value> {
    // Bool
    if s == "true" {
        return Some(Value::Bool(true));
    }
    if s == "false" {
        return Some(Value::Bool(false));
    }

    // Duration: 30s, 5m, 1h, 500ms
    if let Some(d) = try_parse_duration(s) {
        return Some(Value::Duration(d));
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Some(Value::Int(n));
    }

    None
}

fn try_parse_duration(s: &str) -> Option<Duration> {
    if s.ends_with("ms") {
        let n: u64 = s.strip_suffix("ms")?.parse().ok()?;
        return Some(Duration::from_millis(n));
    }
    if s.ends_with('s') {
        let n: u64 = s.strip_suffix('s')?.parse().ok()?;
        return Some(Duration::from_secs(n));
    }
    if s.ends_with('m') {
        let n: u64 = s.strip_suffix('m')?.parse().ok()?;
        return Some(Duration::from_secs(n * 60));
    }
    if s.ends_with('h') {
        let n: u64 = s.strip_suffix('h')?.parse().ok()?;
        return Some(Duration::from_secs(n * 3600));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(input: &str) -> Vec<Token> {
        Tokenizer::tokenize(input)
            .unwrap()
            .into_iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .map(|s| s.token)
            .collect()
    }

    #[test]
    fn simple_command() {
        let toks = tokens(":run cargo test");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn command_with_mode_params() {
        let toks = tokens(":run(retry=3) cargo test");
        assert_eq!(
            toks,
            vec![
                Token::Command("run".into()),
                Token::ModeParenOpen,
                Token::Word("retry".into()),
                Token::ParamEq,
                Token::ParamValue(Value::Int(3)),
                Token::GroupClose, // Will be ModeParenClose after parser context
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_operators() {
        let toks = tokens("a -> b ~> c || d ||? e");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::SerialThen,
                Token::Word("b".into()),
                Token::SerialAlways,
                Token::Word("c".into()),
                Token::ParallelAll,
                Token::Word("d".into()),
                Token::ParallelRace,
                Token::Word("e".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn env_assignment_is_single_word_outside_mode_params() {
        let toks = tokens(":env set FOO=bar");
        assert_eq!(
            toks,
            vec![
                Token::Command("env".into()),
                Token::Word("set".into()),
                Token::Word("FOO=bar".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn pipe_operators() {
        let toks = tokens("a |> b |&> c |!> d");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::PipeStdout,
                Token::Word("b".into()),
                Token::PipeAll,
                Token::Word("c".into()),
                Token::PipeStderr,
                Token::Word("d".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn id_refs() {
        let toks = tokens(":kill J1");
        assert_eq!(
            toks,
            vec![
                Token::Command("kill".into()),
                Token::IdRef(IdKind::Job, 1),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn grouping_parens() {
        let toks = tokens("(a -> b) || c");
        assert_eq!(
            toks,
            vec![
                Token::GroupOpen,
                Token::Word("a".into()),
                Token::SerialThen,
                Token::Word("b".into()),
                Token::GroupClose,
                Token::ParallelAll,
                Token::Word("c".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn duration_values() {
        assert_eq!(try_parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(try_parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(try_parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(
            try_parse_duration("500ms"),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn bare_input() {
        let toks = tokens("cargo test --release");
        assert_eq!(
            toks,
            vec![
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::Word("--release".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn bare_numeric_words_stay_words() {
        let toks = tokens("sleep 4");
        assert_eq!(
            toks,
            vec![
                Token::Word("sleep".into()),
                Token::Word("4".into()),
                Token::Eof,
            ]
        );

        let toks = tokens("sleep 4s");
        assert_eq!(
            toks,
            vec![
                Token::Word("sleep".into()),
                Token::Word("4s".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn complex_chain_with_pipes() {
        let toks = tokens("cargo build |> grep error -> cargo test || cargo clippy");
        assert_eq!(
            toks,
            vec![
                Token::Word("cargo".into()),
                Token::Word("build".into()),
                Token::PipeStdout,
                Token::Word("grep".into()),
                Token::Word("error".into()),
                Token::SerialThen,
                Token::Word("cargo".into()),
                Token::Word("test".into()),
                Token::ParallelAll,
                Token::Word("cargo".into()),
                Token::Word("clippy".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_with_dash_args() {
        // `-A` should be a word, not confused with `->`
        let toks = tokens("git add -A -> git commit -m \"fix\"");
        assert_eq!(
            toks,
            vec![
                Token::Word("git".into()),
                Token::Word("add".into()),
                Token::Word("-A".into()),
                Token::SerialThen,
                Token::Word("git".into()),
                Token::Word("commit".into()),
                Token::Word("-m".into()),
                Token::Word("fix".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_with_colon_in_quoted_arg() {
        // `:wrap` inside quotes should be a word, not a command
        let toks = tokens("echo \":wrap on\" -> echo done");
        assert_eq!(
            toks,
            vec![
                Token::Word("echo".into()),
                Token::Word(":wrap on".into()),
                Token::SerialThen,
                Token::Word("echo".into()),
                Token::Word("done".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_operator_no_space_left() {
        // `-A->` — no space before `->` should still work
        let toks = tokens("cmd -A-> cmd2");
        assert_eq!(
            toks,
            vec![
                Token::Word("cmd".into()),
                Token::Word("-A".into()),
                Token::SerialThen,
                Token::Word("cmd2".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn chain_operator_no_space_right() {
        // `->cmd` — no space after `->` should still work
        let toks = tokens("cmd1 ->cmd2");
        assert_eq!(
            toks,
            vec![
                Token::Word("cmd1".into()),
                Token::SerialThen,
                Token::Word("cmd2".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn pipe_operator_inside_word() {
        // `|>` immediately after a word should still be detected
        let toks = tokens("a|>b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::PipeStdout,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn parallel_operator_inside_word() {
        // `||` immediately after a word should still be detected
        let toks = tokens("a||b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::ParallelAll,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn pipe_stderr_operator_inside_word() {
        // `|!>` immediately after a word should still be detected
        let toks = tokens("a|!>b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::PipeStderr,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn serial_always_operator_inside_word() {
        let toks = tokens("a~>b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::SerialAlways,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn parallel_race_operator_inside_word() {
        let toks = tokens("a||?b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::ParallelRace,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn pipe_all_operator_inside_word() {
        let toks = tokens("a|&>b");
        assert_eq!(
            toks,
            vec![
                Token::Word("a".into()),
                Token::PipeAll,
                Token::Word("b".into()),
                Token::Eof,
            ]
        );
    }
}
