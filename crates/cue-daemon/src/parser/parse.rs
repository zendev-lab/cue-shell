//! Recursive descent parser: Vec<Token> → Ast.
//!
//! Grammar (EBNF):
//! ```ebnf
//! input       = command | bare_input
//! command     = ":" cmd_name mode_params? argument
//! bare_input  = chain
//!
//! chain       = parallel (serial_op parallel)*
//! parallel    = pipeline (parallel_op pipeline)*
//! pipeline    = atom (pipe_op atom)*
//! atom        = "(" chain ")" | word+
//!
//! serial_op   = "->" | "~>"
//! parallel_op = "||" | "||?"
//! pipe_op     = "|>" | "|&>" | "|!>"
//! ```

use cue_core::command_spec::{CommandArgKind, command_spec, command_suggestions};
use cue_core::pipeline::{ParallelOp, PipeOp, SerialOp};

use super::ast::{Argument, Ast, ChainNode, PipeSegment, Pipeline, ScriptItemAst};
use super::token::{Span, Spanned, Token, Value};
use super::tokenizer::Tokenizer;

/// Parser error.
#[derive(Debug, Clone, thiserror::Error)]
#[error("parse error at byte {span:?}: {message}")]
pub struct ParseError {
    pub span: Span,
    pub message: String,
    pub kind: ParseErrorKind,
    pub suggestions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseErrorKind {
    UnknownCommand,
    InvalidModeParam,
    UnexpectedToken,
    MissingArgument,
    InvalidIdRef,
    UnmatchedParen,
    InvalidOperator,
    InvalidCronSchedule,
}

/// Recursive descent parser.
pub struct Parser<'a> {
    tokens: Vec<Spanned>,
    /// Current position (index into tokens, skipping whitespace).
    pos: usize,
    /// Input length for EOF spans.
    input_len: usize,
    /// Original input string for raw-text extraction.
    input: &'a str,
}

struct ScriptChunk {
    tokens: Vec<Spanned>,
    source: String,
    span: Span,
}

impl<'a> Parser<'a> {
    /// Parse a raw input string into an AST.
    pub fn parse(input: &'a str) -> Result<Ast, ParseError> {
        let all_tokens = Tokenizer::tokenize(input).map_err(|e| ParseError {
            span: Span::new(e.pos, e.pos + 1),
            message: e.message,
            kind: ParseErrorKind::UnexpectedToken,
            suggestions: vec![],
        })?;

        // Filter horizontal whitespace for the parser (keep spans intact).
        let tokens: Vec<Spanned> = all_tokens
            .into_iter()
            .filter(|s| !matches!(s.token, Token::Whitespace(_)))
            .collect();

        Self::parse_tokens(tokens, input)
    }

    fn parse_tokens(tokens: Vec<Spanned>, input: &str) -> Result<Ast, ParseError> {
        let chunks = split_top_level_statements(&tokens, input);
        if chunks.is_empty() {
            return Ok(Ast::BareInput {
                argument: Argument::Empty,
                span: Span::new(0, 0),
            });
        }

        let mut parser = Parser {
            tokens: chunks[0].tokens.clone(),
            pos: 0,
            input_len: input.len(),
            input,
        };
        if chunks.len() == 1 {
            return parser.parse_single_statement();
        }

        let span = Span::new(
            chunks.first().map(|chunk| chunk.span.start).unwrap_or(0),
            chunks.last().map(|chunk| chunk.span.end).unwrap_or(0),
        );
        let mut items = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let mut parser = Parser {
                tokens: chunk.tokens,
                pos: 0,
                input_len: input.len(),
                input,
            };
            items.push(ScriptItemAst {
                source: chunk.source,
                span: chunk.span,
                statement: Box::new(parser.parse_single_statement()?),
            });
        }
        Ok(Ast::Script { items, span })
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .map(|s| &s.token)
            .unwrap_or(&Token::Eof)
    }

    fn peek_span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|s| s.span)
            .unwrap_or(Span::new(self.input_len, self.input_len))
    }

    fn advance(&mut self) -> &Spanned {
        let s = &self.tokens[self.pos];
        self.pos += 1;
        s
    }

    fn expect(&mut self, expected: &Token) -> Result<&Spanned, ParseError> {
        if self.peek() == expected {
            Ok(self.advance())
        } else {
            Err(ParseError {
                span: self.peek_span(),
                message: format!("expected {expected}, found {}", self.peek()),
                kind: ParseErrorKind::UnexpectedToken,
                suggestions: vec![],
            })
        }
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn parse_single_statement(&mut self) -> Result<Ast, ParseError> {
        let ast = self.parse_statement()?;
        if !self.at_end() {
            return Err(ParseError {
                span: self.peek_span(),
                message: format!("unexpected token {}", self.peek()),
                kind: ParseErrorKind::UnexpectedToken,
                suggestions: vec![],
            });
        }
        Ok(ast)
    }

    fn parse_statement(&mut self) -> Result<Ast, ParseError> {
        let span_start = self.peek_span().start;

        if let Token::Command(_) = self.peek() {
            return self.parse_command(span_start);
        }

        // Bare input
        if self.at_end() {
            return Ok(Ast::BareInput {
                argument: Argument::Empty,
                span: Span::new(span_start, span_start),
            });
        }

        let chain = self.parse_chain()?;
        let span_end = self.peek_span().end;
        Ok(Ast::BareInput {
            argument: Argument::Chain(chain),
            span: Span::new(span_start, span_end),
        })
    }

    fn parse_command(&mut self, span_start: usize) -> Result<Ast, ParseError> {
        let name = match self.advance().token.clone() {
            Token::Command(n) => n,
            _ => unreachable!(),
        };

        // Mode params?
        let mode_params = if matches!(self.peek(), Token::ModeParenOpen) {
            self.advance(); // consume ModeParenOpen
            self.parse_mode_params()?
        } else {
            vec![]
        };

        // Parse argument based on command classification
        let argument = self.parse_argument_for_command(&name)?;
        let span_end = self.peek_span().start;

        Ok(Ast::Command {
            name,
            mode_params,
            argument,
            span: Span::new(span_start, span_end),
        })
    }

    fn parse_mode_params(&mut self) -> Result<Vec<(String, Value)>, ParseError> {
        let mut params = vec![];
        loop {
            match self.peek() {
                Token::GroupClose => {
                    self.advance(); // consume )
                    break;
                }
                Token::Eof => {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: "unterminated mode params".into(),
                        kind: ParseErrorKind::UnmatchedParen,
                        suggestions: vec![],
                    });
                }
                Token::Comma => {
                    self.advance();
                    continue;
                }
                _ => {
                    // Expect key=value
                    let key = match self.peek().clone() {
                        Token::Word(k) => {
                            self.advance();
                            k
                        }
                        _ => {
                            return Err(ParseError {
                                span: self.peek_span(),
                                message: format!("expected parameter name, found {}", self.peek()),
                                kind: ParseErrorKind::InvalidModeParam,
                                suggestions: vec![],
                            });
                        }
                    };
                    self.expect(&Token::ParamEq)?;
                    let value = match self.peek().clone() {
                        Token::ParamValue(v) => {
                            self.advance();
                            v
                        }
                        Token::Word(s) => {
                            self.advance();
                            Value::Str(s)
                        }
                        _ => {
                            return Err(ParseError {
                                span: self.peek_span(),
                                message: format!("expected parameter value, found {}", self.peek()),
                                kind: ParseErrorKind::InvalidModeParam,
                                suggestions: vec![],
                            });
                        }
                    };
                    params.push((key, value));
                }
            }
        }
        Ok(params)
    }

    /// Determine argument type based on the shared command registry.
    fn parse_argument_for_command(&mut self, name: &str) -> Result<Argument, ParseError> {
        let Some(spec) = command_spec(name) else {
            return Err(ParseError {
                span: self.peek_span(),
                message: format!("unknown command `:{name}`"),
                kind: ParseErrorKind::UnknownCommand,
                suggestions: suggest_command(name),
            });
        };

        match spec.arg_kind {
            CommandArgKind::Chain => {
                if self.at_end() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: format!("`:{name}` requires a command"),
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![format!(":{name} cargo test")],
                    });
                }
                Ok(Argument::Chain(self.parse_chain()?))
            }

            CommandArgKind::Cron => {
                if self.at_end() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: format!("`:{name}` requires a schedule expression"),
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![":cron every 5m cargo test".into()],
                    });
                }
                Ok(Argument::Chain(self.parse_chain()?))
            }

            CommandArgKind::Tail => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    let bytes = match self.peek().clone() {
                        Token::Word(w) => {
                            if let Ok(b) = w.parse::<usize>() {
                                self.advance();
                                Some(b)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    Ok(Argument::TailRef(kind, n, bytes))
                } else {
                    Err(ParseError {
                        span: self.peek_span(),
                        message: format!(":{name} requires an ID (e.g. J1)"),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![format!(":{name} J1"), format!(":{name} J1 1024")],
                    })
                }
            }

            CommandArgKind::Id => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    Ok(Argument::IdRef(kind, n))
                } else {
                    Err(ParseError {
                        span: self.peek_span(),
                        message: format!(":{name} requires an ID (e.g. J1, C1)"),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![format!(":{name} J1")],
                    })
                }
            }

            CommandArgKind::Text => {
                let text = self.consume_remaining_raw_text();
                if text.is_empty() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: match name {
                            "send" => "`:send` requires a target and input".into(),
                            _ => format!(":{name} requires an argument"),
                        },
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![],
                    });
                }
                Ok(Argument::Text(text))
            }

            CommandArgKind::OptionalId => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    self.advance();
                    Ok(Argument::IdRef(kind, n))
                } else {
                    Ok(Argument::Empty)
                }
            }

            CommandArgKind::Empty => Ok(Argument::Empty),

            CommandArgKind::OptionalText => {
                let text = self.consume_remaining_text();
                if text.is_empty() {
                    Ok(Argument::Empty)
                } else {
                    Ok(Argument::Text(text))
                }
            }
        }
    }

    /// Consume all remaining tokens as a single text string.
    fn consume_remaining_text(&mut self) -> String {
        let mut parts = vec![];
        while !self.at_end() {
            parts.push(self.advance().token.to_string());
        }
        parts.join(" ")
    }

    /// Consume remaining input as raw text (preserving original characters).
    ///
    /// Uses token spans to extract from the original input, so operator
    /// characters like `->` inside raw-text arguments are preserved as-is.
    fn consume_remaining_raw_text(&mut self) -> String {
        if self.at_end() {
            return String::new();
        }
        let start = self.tokens[self.pos].span.start;
        // Skip all remaining tokens to advance position.
        let end_pos = self
            .tokens
            .last()
            .map(|s| s.span.end)
            .unwrap_or(self.input_len);
        while !self.at_end() {
            self.advance();
        }
        self.input[start..end_pos].to_string()
    }

    // ── Chain grammar (recursive descent) ──

    /// chain = parallel (serial_op parallel)*
    fn parse_chain(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_parallel()?;
        loop {
            let op = match self.peek() {
                Token::SerialThen => SerialOp::Then,
                Token::SerialAlways => SerialOp::Always,
                _ => break,
            };
            self.advance();
            let right = self.parse_parallel()?;
            left = ChainNode::Serial {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// parallel = pipeline (parallel_op pipeline)*
    fn parse_parallel(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_pipeline()?;
        loop {
            let op = match self.peek() {
                Token::ParallelAll => ParallelOp::All,
                Token::ParallelRace => ParallelOp::Race,
                _ => break,
            };
            self.advance();
            let right = self.parse_pipeline()?;
            left = ChainNode::Parallel {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// pipeline = atom (pipe_op atom)*
    fn parse_pipeline(&mut self) -> Result<ChainNode, ParseError> {
        let first = self.parse_atom_words()?;

        // Check for pipe operators
        let mut segments = vec![];
        let mut current_words = first;
        loop {
            let pipe_op = match self.peek() {
                Token::PipeStdout => PipeOp::Stdout,
                Token::PipeAll => PipeOp::StdoutStderr,
                Token::PipeStderr => PipeOp::StderrOnly,
                _ => {
                    // End of pipeline
                    if current_words.is_empty() {
                        break;
                    }
                    segments.push(PipeSegment {
                        command: current_words,
                        pipe_to_next: None,
                    });
                    break;
                }
            };
            segments.push(PipeSegment {
                command: current_words,
                pipe_to_next: Some(pipe_op),
            });
            self.advance(); // consume pipe op
            current_words = self.parse_atom_words()?;
        }

        if segments.is_empty() {
            return Err(ParseError {
                span: self.peek_span(),
                message: "expected command".into(),
                kind: ParseErrorKind::MissingArgument,
                suggestions: vec![],
            });
        }

        Ok(ChainNode::Leaf(Pipeline { segments }))
    }

    /// Parse words for one atom in a pipeline (or a grouped chain).
    fn parse_atom_words(&mut self) -> Result<Vec<String>, ParseError> {
        // Grouped chain at pipeline level is illegal:
        // `cmd |> (chain) |> cmd` — can't nest chain inside pipeline.
        if matches!(self.peek(), Token::GroupOpen) {
            let span = self.peek_span();
            self.advance(); // consume GroupOpen to avoid downstream confusion
            return Err(ParseError {
                span,
                message: "cannot nest a chain group `(...)` inside a pipeline. Use `|>` for process pipes, `->` / `||` for job chains.".into(),
                kind: ParseErrorKind::UnexpectedToken,
                suggestions: vec![
                    "use |> for piping between processes".into(),
                    "use -> for serial job chaining".into(),
                ],
            });
        }

        let mut words = vec![];
        loop {
            match self.peek() {
                Token::Word(w) => {
                    let w = w.clone();
                    self.advance();
                    words.push(w);
                }
                Token::IdRef(k, n) => {
                    let s = format!("{k}{n}");
                    self.advance();
                    words.push(s);
                }
                Token::Command(c) => {
                    // In chain context, a `:cmd` is treated as a word
                    let s = format!(":{c}");
                    self.advance();
                    words.push(s);
                }
                _ => break,
            }
        }
        Ok(words)
    }
}

fn split_top_level_statements(tokens: &[Spanned], input: &str) -> Vec<ScriptChunk> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0usize;

    for (idx, spanned) in tokens.iter().enumerate() {
        match spanned.token {
            Token::Eof => break,
            Token::ModeParenOpen | Token::GroupOpen => {
                depth += 1;
                current.push(spanned.clone());
            }
            Token::ModeParenClose | Token::GroupClose => {
                depth = depth.saturating_sub(1);
                current.push(spanned.clone());
            }
            Token::Newline => {
                if should_split_on_newline(tokens, idx, depth, &current) {
                    push_script_chunk(&mut chunks, &mut current, input);
                }
            }
            _ => current.push(spanned.clone()),
        }
    }

    push_script_chunk(&mut chunks, &mut current, input);
    chunks
}

fn should_split_on_newline(
    tokens: &[Spanned],
    newline_index: usize,
    depth: usize,
    current: &[Spanned],
) -> bool {
    if current.is_empty() {
        return true;
    }
    if depth > 0 {
        return false;
    }
    let prev = current.last().map(|spanned| &spanned.token);
    if prev.is_some_and(token_requires_continuation) {
        return false;
    }
    let next = tokens[newline_index + 1..]
        .iter()
        .find_map(|spanned| match &spanned.token {
            Token::Newline | Token::Eof => None,
            other => Some(other),
        });
    !next.is_some_and(token_continues_previous_line)
}

fn token_requires_continuation(token: &Token) -> bool {
    matches!(
        token,
        Token::SerialThen
            | Token::SerialAlways
            | Token::ParallelAll
            | Token::ParallelRace
            | Token::PipeStdout
            | Token::PipeAll
            | Token::PipeStderr
    )
}

fn token_continues_previous_line(token: &Token) -> bool {
    token_requires_continuation(token)
}

fn push_script_chunk(chunks: &mut Vec<ScriptChunk>, current: &mut Vec<Spanned>, input: &str) {
    if current.is_empty() {
        return;
    }
    let span = Span::new(
        current
            .first()
            .map(|spanned| spanned.span.start)
            .unwrap_or(0),
        current.last().map(|spanned| spanned.span.end).unwrap_or(0),
    );
    let source = input[span.start..span.end].trim().to_string();
    if source.is_empty() {
        current.clear();
        return;
    }
    chunks.push(ScriptChunk {
        tokens: std::mem::take(current),
        source,
        span,
    });
}

pub(super) fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    if s.ends_with("ms") {
        let n: u64 = s.strip_suffix("ms")?.parse().ok()?;
        return Some(std::time::Duration::from_millis(n));
    }
    for (suffix, multiplier) in [("s", 1u64), ("m", 60), ("h", 3600)] {
        if s.ends_with(suffix) {
            let n: u64 = s.strip_suffix(suffix)?.parse().ok()?;
            return Some(std::time::Duration::from_secs(n * multiplier));
        }
    }
    None
}

fn suggest_command(name: &str) -> Vec<String> {
    command_suggestions(name)
        .into_iter()
        .map(|command| format!(":{command}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::token::IdKind;
    use super::*;

    #[test]
    fn parse_simple_run() {
        let ast = Parser::parse(":run cargo test").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "run");
                match argument {
                    Argument::Chain(ChainNode::Leaf(p)) => {
                        assert_eq!(p.segments.len(), 1);
                        assert_eq!(p.segments[0].command, vec!["cargo", "test"]);
                    }
                    _ => panic!("expected Chain"),
                }
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_bare_input() {
        let ast = Parser::parse("cargo test --release").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments[0].command, vec!["cargo", "test", "--release"]);
                }
                _ => panic!("expected Chain"),
            },
            _ => panic!("expected BareInput"),
        }
    }

    #[test]
    fn parse_bare_input_with_numeric_arg() {
        let ast = Parser::parse("sleep 4").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments[0].command, vec!["sleep", "4"]);
                }
                _ => panic!("expected Chain"),
            },
            _ => panic!("expected BareInput"),
        }
    }

    #[test]
    fn parse_kill_id() {
        let ast = Parser::parse(":kill J1").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "kill");
                assert_eq!(argument, Argument::IdRef(IdKind::Job, 1));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_chain() {
        let ast = Parser::parse(":run a -> b || c").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { op, left, right }) => {
                    assert_eq!(op, SerialOp::Then);
                    assert!(matches!(*left, ChainNode::Leaf(_)));
                    assert!(matches!(*right, ChainNode::Parallel { .. }));
                }
                _ => panic!("expected Chain with Serial"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_pipeline() {
        let ast = Parser::parse(":run a |> b |&> c").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments.len(), 3);
                    assert_eq!(p.segments[0].pipe_to_next, Some(PipeOp::Stdout));
                    assert_eq!(p.segments[1].pipe_to_next, Some(PipeOp::StdoutStderr));
                    assert!(p.segments[2].pipe_to_next.is_none());
                }
                _ => panic!("expected pipeline"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_mode_params() {
        let ast = Parser::parse(":run(retry=3) cargo test").unwrap();
        match ast {
            Ast::Command {
                mode_params,
                argument,
                ..
            } => {
                assert_eq!(mode_params.len(), 1);
                assert_eq!(mode_params[0].0, "retry");
                assert_eq!(mode_params[0].1, Value::Int(3));
                assert!(matches!(argument, Argument::Chain(_)));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_send_text() {
        let ast = Parser::parse(":send J1 continue with the fix").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "send");
                assert_eq!(argument, Argument::Text("J1 continue with the fix".into()));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_send_raw_preserves_operators() {
        // `:send` should preserve `->` as literal text, not as chain operator.
        let ast = Parser::parse(":send J1 replace a->b with c->d").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "send");
                assert_eq!(argument, Argument::Text("J1 replace a->b with c->d".into()));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_cron() {
        let ast = Parser::parse(":cron every 5m cargo test").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "cron");
                assert!(matches!(argument, Argument::Chain(ChainNode::Leaf(_))));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_empty_command() {
        let ast = Parser::parse(":jobs").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "jobs");
                assert_eq!(argument, Argument::Empty);
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn unknown_command_error() {
        let err = Parser::parse(":foo").unwrap_err();
        assert_eq!(err.kind, ParseErrorKind::UnknownCommand);
        assert!(!err.suggestions.is_empty());
    }

    #[test]
    fn missing_run_argument() {
        let err = Parser::parse(":run").unwrap_err();
        assert_eq!(err.kind, ParseErrorKind::MissingArgument);
    }

    #[test]
    fn complex_chain_with_pipes() {
        // cargo build |> grep error -> cargo test || cargo clippy
        let ast =
            Parser::parse(":run cargo build |> grep error -> cargo test || cargo clippy").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { left, right, .. }) => {
                    // left = pipeline (cargo build |> grep error)
                    if let ChainNode::Leaf(p) = *left {
                        assert_eq!(p.segments.len(), 2);
                        assert_eq!(p.segments[0].command, vec!["cargo", "build"]);
                        assert_eq!(p.segments[0].pipe_to_next, Some(PipeOp::Stdout));
                        assert_eq!(p.segments[1].command, vec!["grep", "error"]);
                    } else {
                        panic!("expected Leaf pipeline");
                    }
                    // right = parallel (cargo test || cargo clippy)
                    assert!(matches!(*right, ChainNode::Parallel { .. }));
                }
                _ => panic!("expected Serial chain"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn single_quoted_args_with_colon() {
        // Regression: single quotes should be treated as literal string
        // delimiters, not as regular characters.  The `:` inside `'[:upper:]'`
        // must NOT be parsed as a command prefix.
        let ast = Parser::parse(":run tr '[:upper:]' '[:lower:]'").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments.len(), 1);
                    assert_eq!(p.segments[0].command, vec!["tr", "[:upper:]", "[:lower:]"]);
                }
                _ => panic!("expected single-segment pipeline"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn unquoted_args_with_colon() {
        let ast = Parser::parse(":run tr [:upper:] [:lower:]").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(p)) => {
                    assert_eq!(p.segments.len(), 1);
                    assert_eq!(p.segments[0].command, vec!["tr", "[:upper:]", "[:lower:]"]);
                }
                _ => panic!("expected single-segment pipeline"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_multiline_script() {
        let ast = Parser::parse("cargo test\n:run cargo clippy").unwrap();
        match ast {
            Ast::Script { items, .. } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].source, "cargo test");
                assert_eq!(items[1].source, ":run cargo clippy");
            }
            _ => panic!("expected Script"),
        }
    }

    #[test]
    fn parse_multiline_chain_continues_after_operator() {
        let ast = Parser::parse("cargo test ->\ncargo clippy").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => {
                assert!(matches!(
                    argument,
                    Argument::Chain(ChainNode::Serial { .. })
                ));
            }
            _ => panic!("expected BareInput"),
        }
    }

    #[test]
    fn parse_multiline_chain_continues_before_operator() {
        let ast = Parser::parse("cat a\n|| cat b").unwrap();
        match ast {
            Ast::BareInput { argument, .. } => {
                assert!(matches!(
                    argument,
                    Argument::Chain(ChainNode::Parallel {
                        op: ParallelOp::All,
                        ..
                    })
                ));
            }
            _ => panic!("expected BareInput"),
        }
    }
}
