//! Recursive descent parser: Vec<Token> → Ast.
//!
//! Grammar (EBNF):
//! ```ebnf
//! input       = command | bare_input
//! command     = ":" cmd_name mode_params? argument
//! bare_input  = chain
//!
//! chain       = parallel (serial_op parallel)*
//! parallel    = job_expr (parallel_op job_expr)*
//! job_expr    = pipeline (("&&" | "||") pipeline)*
//! pipeline    = atom (pipe_op atom)*
//! atom        = "(" chain ")" | word+
//!
//! serial_op   = "->" | "~>"
//! parallel_op = "|||" | "|?|"
//! pipe_op     = "|>" | "|&>" | "|!>"
//! ```

use cue_core::command_spec::{
    CommandArgKind, CommandIdKind, CommandSpec, ModeParamSpec, ModeParamValueKind, command_spec,
    command_suggestions, mode_param_spec, mode_param_spec_for_command,
};
use cue_core::pipeline::{ParallelOp, PipeOp, SerialOp};

use super::ast::{Argument, Ast, ChainNode, JobExpr, PipeSegment, Pipeline, ScriptItemAst};
use super::token::{IdKind, Span, Spanned, Token, Value};
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
    InvalidCronSchedule,
}

/// Recursive descent parser.
pub(super) struct Parser<'a> {
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
    pub(super) fn parse(input: &'a str) -> Result<Ast, ParseError> {
        let tokens = tokenize_for_parser(input)?;
        Self::parse_tokens(tokens, input)
    }

    /// Parse a `.cue` file-script body into a top-level script AST.
    pub(super) fn parse_file_script(input: &str) -> Result<Ast, ParseError> {
        let normalized = normalize_file_script(input);
        let tokens = tokenize_for_parser(&normalized)?;
        Self::parse_tokens_as_script(tokens, &normalized)
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

    fn parse_tokens_as_script(tokens: Vec<Spanned>, input: &str) -> Result<Ast, ParseError> {
        let chunks = split_top_level_statements(&tokens, input);
        if chunks.is_empty() {
            return Err(ParseError {
                span: Span::new(0, input.len()),
                message: "empty .cue script".into(),
                kind: ParseErrorKind::MissingArgument,
                suggestions: vec!["add at least one top-level item".into()],
            });
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
        let spec = command_spec_or_error(&name, self.peek_span())?;

        // Mode params?
        let mode_params = if matches!(self.peek(), Token::ModeParenOpen) {
            if !spec.accepts_mode_params() {
                return Err(ParseError {
                    span: self.peek_span(),
                    message: format!("`:{name}` does not accept mode params"),
                    kind: ParseErrorKind::InvalidModeParam,
                    suggestions: vec![spec.usage.to_string()],
                });
            }
            self.advance(); // consume ModeParenOpen
            self.parse_mode_params(&name)?
        } else {
            vec![]
        };

        // Parse argument based on command classification
        let argument = self.parse_argument_for_command(&name, spec)?;
        let span_end = self.peek_span().start;

        Ok(Ast::Command {
            name,
            mode_params,
            argument,
            span: Span::new(span_start, span_end),
        })
    }

    fn parse_mode_params(&mut self, command: &str) -> Result<Vec<(String, Value)>, ParseError> {
        let mut params = vec![];
        loop {
            match self.peek() {
                Token::ModeParenClose => {
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
                    let key_span = self.tokens[self.pos - 1].span;
                    let Some(global_spec) = mode_param_spec(&key) else {
                        return Err(ParseError {
                            span: key_span,
                            message: format!("unknown mode parameter `{key}`"),
                            kind: ParseErrorKind::InvalidModeParam,
                            suggestions: vec![],
                        });
                    };
                    let Some(spec) = mode_param_spec_for_command(command, &key) else {
                        return Err(ParseError {
                            span: key_span,
                            message: format!(
                                "mode parameter `{key}` is not supported by `:{command}`"
                            ),
                            kind: ParseErrorKind::InvalidModeParam,
                            suggestions: vec![],
                        });
                    };
                    debug_assert_eq!(spec.name, global_spec.name);
                    self.expect(&Token::ParamEq)?;
                    let value_span = self.peek_span();
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
                    validate_mode_param_value(spec, &value, value_span)?;
                    params.push((key, value));
                }
            }
        }
        Ok(params)
    }

    /// Determine argument type based on the shared command registry.
    fn parse_argument_for_command(
        &mut self,
        name: &str,
        spec: &CommandSpec,
    ) -> Result<Argument, ParseError> {
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

            CommandArgKind::Tail(allowed) => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    validate_id_kind(name, allowed, kind, self.peek_span())?;
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
                        message: format!(":{name} requires an ID ({})", allowed.display()),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![
                            format!(":{name} {}", allowed.first_example()),
                            format!(":{name} {} 1024", allowed.first_example()),
                        ],
                    })
                }
            }

            CommandArgKind::Id(allowed) => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    validate_id_kind(name, allowed, kind, self.peek_span())?;
                    self.advance();
                    Ok(Argument::IdRef(kind, n))
                } else {
                    Err(ParseError {
                        span: self.peek_span(),
                        message: format!(":{name} requires an ID ({})", allowed.display()),
                        kind: ParseErrorKind::InvalidIdRef,
                        suggestions: vec![format!(":{name} {}", allowed.first_example())],
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

            CommandArgKind::TargetText(allowed) => {
                let target = self.peek().clone();
                validate_text_target(name, allowed, &target, self.peek_span())?;
                let text = self.consume_remaining_raw_text();
                if text.is_empty() {
                    return Err(ParseError {
                        span: self.peek_span(),
                        message: format!("`:{name}` requires a target and input"),
                        kind: ParseErrorKind::MissingArgument,
                        suggestions: vec![format!(
                            ":{name} {} your input",
                            allowed.first_example()
                        )],
                    });
                }
                Ok(Argument::Text(text))
            }

            CommandArgKind::OptionalId(allowed) => {
                if let Token::IdRef(kind, n) = self.peek().clone() {
                    validate_id_kind(name, allowed, kind, self.peek_span())?;
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

    /// parallel = job_expr (parallel_op job_expr)*
    fn parse_parallel(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_job_expr()?;
        loop {
            let op = match self.peek() {
                Token::ParallelAll => ParallelOp::All,
                Token::ParallelRace => ParallelOp::Race,
                _ => break,
            };
            self.advance();
            let right = self.parse_job_expr()?;
            left = ChainNode::Parallel {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// job_expr = pipeline (("&&" | "||") pipeline)*
    fn parse_job_expr(&mut self) -> Result<ChainNode, ParseError> {
        let mut left = self.parse_pipeline()?;
        loop {
            let is_and = match self.peek() {
                Token::JobAnd => true,
                Token::JobOr => false,
                _ => break,
            };
            self.advance();
            let right = self.parse_pipeline()?;
            left = if is_and {
                JobExpr::And {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            } else {
                JobExpr::Or {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            };
        }
        Ok(ChainNode::Leaf(left))
    }

    /// pipeline = atom (pipe_op atom)*
    fn parse_pipeline(&mut self) -> Result<JobExpr, ParseError> {
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

        Ok(JobExpr::Pipeline(Pipeline { segments }))
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
                message: "cannot nest a chain group `(...)` inside a pipeline. Use `|>` for process pipes, `->` / `|||` for job chains.".into(),
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

fn validate_id_kind(
    command: &str,
    allowed: CommandIdKind,
    actual: IdKind,
    span: Span,
) -> Result<(), ParseError> {
    let accepted = match actual {
        IdKind::Job => allowed.accepts_job(),
        IdKind::Cron => allowed.accepts_cron(),
    };
    if accepted {
        return Ok(());
    }

    Err(ParseError {
        span,
        message: format!(":{command} expects {} ID, got {actual}", allowed.display()),
        kind: ParseErrorKind::InvalidIdRef,
        suggestions: vec![format!(":{command} {}", allowed.first_example())],
    })
}

fn validate_text_target(
    command: &str,
    allowed: CommandIdKind,
    target: &Token,
    span: Span,
) -> Result<(), ParseError> {
    match target {
        Token::IdRef(kind, _) => validate_id_kind(command, allowed, *kind, span),
        Token::Eof => Err(ParseError {
            span,
            message: format!("`:{command}` requires a target and input"),
            kind: ParseErrorKind::MissingArgument,
            suggestions: vec![format!(":{command} {} your input", allowed.first_example())],
        }),
        _ => Err(ParseError {
            span,
            message: format!(":{command} requires a target ID ({})", allowed.display()),
            kind: ParseErrorKind::InvalidIdRef,
            suggestions: vec![format!(":{command} {} your input", allowed.first_example())],
        }),
    }
}

fn tokenize_for_parser(input: &str) -> Result<Vec<Spanned>, ParseError> {
    let all_tokens = Tokenizer::tokenize(input).map_err(|e| ParseError {
        span: Span::new(e.pos, e.pos + 1),
        message: e.message,
        kind: ParseErrorKind::UnexpectedToken,
        suggestions: vec![],
    })?;

    // Filter horizontal whitespace for the parser (keep spans intact).
    Ok(all_tokens
        .into_iter()
        .filter(|s| !matches!(s.token, Token::Whitespace(_)))
        .collect())
}

fn normalize_file_script(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    for (index, line) in input.split_inclusive('\n').enumerate() {
        let (body, newline) = line
            .strip_suffix('\n')
            .map_or((line, ""), |body| (body, "\n"));
        let body = body.strip_suffix('\r').unwrap_or(body);

        if index == 0 && body.starts_with("#!") {
            output.push_str(newline);
            continue;
        }

        output.push_str(strip_file_script_comment(body));
        output.push_str(newline);
    }
    output
}

fn strip_file_script_comment(line: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;
    let mut comment_boundary = true;

    for (index, ch) in line.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if in_single {
            if ch == '\'' {
                in_single = false;
            }
            continue;
        }

        match ch {
            '#' if comment_boundary => return line[..index].trim_end(),
            '"' => {
                in_double = true;
                comment_boundary = false;
            }
            '\'' => {
                in_single = true;
                comment_boundary = false;
            }
            ' ' | '\t' => {
                comment_boundary = true;
            }
            _ => {
                comment_boundary = false;
            }
        }
    }

    line
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
            | Token::JobAnd
            | Token::JobOr
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

fn command_spec_or_error(name: &str, span: Span) -> Result<&'static CommandSpec, ParseError> {
    command_spec(name).ok_or_else(|| ParseError {
        span,
        message: format!("unknown command `:{name}`"),
        kind: ParseErrorKind::UnknownCommand,
        suggestions: suggest_command(name),
    })
}

fn validate_mode_param_value(
    spec: &ModeParamSpec,
    value: &Value,
    span: Span,
) -> Result<(), ParseError> {
    let valid = matches!(
        (spec.value_kind, value),
        (ModeParamValueKind::String, Value::Str(_)) | (ModeParamValueKind::Bool, Value::Bool(_))
    );
    if valid {
        return Ok(());
    }

    Err(ParseError {
        span,
        message: format!(
            "mode parameter `{}` expects {} (e.g. {})",
            spec.name,
            mode_param_value_kind_name(spec.value_kind),
            spec.value_hint
        ),
        kind: ParseErrorKind::InvalidModeParam,
        suggestions: vec![format!("{}={}", spec.name, spec.value_hint)],
    })
}

fn mode_param_value_kind_name(kind: ModeParamValueKind) -> &'static str {
    match kind {
        ModeParamValueKind::String => "a string",
        ModeParamValueKind::Bool => "a boolean",
    }
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

    fn leaf_pipeline(chain: &ChainNode) -> &Pipeline {
        match chain {
            ChainNode::Leaf(JobExpr::Pipeline(pipeline)) => pipeline,
            other => panic!("expected leaf pipeline, got {other:?}"),
        }
    }

    fn simple_command(expr: &JobExpr) -> Vec<&str> {
        match expr {
            JobExpr::Pipeline(pipeline) => pipeline.segments[0]
                .command
                .iter()
                .map(String::as_str)
                .collect(),
            other => panic!("expected pipeline, got {other:?}"),
        }
    }

    #[test]
    fn parse_simple_run() {
        let ast = Parser::parse(":run cargo test").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "run");
                match argument {
                    Argument::Chain(chain) => {
                        let p = leaf_pipeline(&chain);
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
                Argument::Chain(chain) => {
                    let p = leaf_pipeline(&chain);
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
                Argument::Chain(chain) => {
                    let p = leaf_pipeline(&chain);
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
    fn id_arguments_enforce_command_entity_kind() {
        let kill_cron = Parser::parse(":kill C1").unwrap();
        match kill_cron {
            Ast::Command { argument, .. } => {
                assert_eq!(argument, Argument::IdRef(IdKind::Cron, 1));
            }
            _ => panic!("expected Command"),
        }

        let log_cron = Parser::parse(":log C1").unwrap();
        match log_cron {
            Ast::Command { argument, .. } => {
                assert_eq!(argument, Argument::IdRef(IdKind::Cron, 1));
            }
            _ => panic!("expected Command"),
        }

        let pause_job = Parser::parse(":pause J1").expect_err("pause requires cron id");
        assert_eq!(pause_job.kind, ParseErrorKind::InvalidIdRef);
        assert!(pause_job.message.contains("C<n>"));

        let fg_cron = Parser::parse(":fg C1").expect_err("fg requires job id");
        assert_eq!(fg_cron.kind, ParseErrorKind::InvalidIdRef);
        assert!(fg_cron.message.contains("J<n>"));

        let tail_cron = Parser::parse(":tail C1").expect_err("tail requires job id");
        assert_eq!(tail_cron.kind, ParseErrorKind::InvalidIdRef);
        assert!(tail_cron.message.contains("J<n>"));
    }

    #[test]
    fn parse_chain() {
        let ast = Parser::parse(":run a -> b ||| c").unwrap();
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
    fn parse_job_logical_expression_as_single_leaf() {
        let ast = Parser::parse(":run false && echo no || echo yes").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Leaf(JobExpr::Or { left, right })) => {
                    match *left {
                        JobExpr::And { left, right } => {
                            assert_eq!(simple_command(&left), vec!["false"]);
                            assert_eq!(simple_command(&right), vec!["echo", "no"]);
                        }
                        other => panic!("expected left-assoc AND, got {other:?}"),
                    }
                    assert_eq!(simple_command(&right), vec!["echo", "yes"]);
                }
                other => panic!("expected single job logical leaf, got {other:?}"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_job_logical_binds_tighter_than_chain_operators() {
        let ast = Parser::parse(":run a || b -> c ||| d").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { left, right, .. }) => {
                    assert!(matches!(*left, ChainNode::Leaf(JobExpr::Or { .. })));
                    assert!(matches!(*right, ChainNode::Parallel { .. }));
                }
                other => panic!("expected serial chain, got {other:?}"),
            },
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_pipeline() {
        let ast = Parser::parse(":run a |> b |&> c").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(chain) => {
                    let p = leaf_pipeline(&chain);
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
    fn parse_rejects_bare_shell_pipe() {
        let err = Parser::parse(":run echo hi | wc -c")
            .expect_err("bare shell pipe should not be accepted as a word");

        assert!(
            err.message
                .contains("bare `|` is not a cue-shell pipe operator")
        );
        assert!(err.message.contains("use `|>`"));
    }

    #[test]
    fn parse_mode_params() {
        let ast = Parser::parse(":run(pty=false) cargo test").unwrap();
        match ast {
            Ast::Command {
                mode_params,
                argument,
                ..
            } => {
                assert_eq!(mode_params.len(), 1);
                assert_eq!(mode_params[0].0, "pty");
                assert_eq!(mode_params[0].1, Value::Bool(false));
                assert!(matches!(argument, Argument::Chain(_)));
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn mode_params_reject_commands_without_mode_param_support() {
        let err = Parser::parse(":kill(timeout=1s) J1").expect_err("kill params should fail");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(err.message.contains("does not accept mode params"));
    }

    #[test]
    fn mode_params_reject_unknown_names() {
        let err = Parser::parse(":run(typo=1) cargo test").expect_err("unknown param should fail");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(err.message.contains("unknown mode parameter `typo`"));
    }

    #[test]
    fn mode_params_reject_params_not_supported_by_command() {
        let err = Parser::parse(":cron(pty=false) every 5m cargo test")
            .expect_err("cron must not accept run-only pty param");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(
            err.message
                .contains("mode parameter `pty` is not supported by `:cron`")
        );
    }

    #[test]
    fn mode_params_reject_unimplemented_timeout() {
        let err = Parser::parse(":run(timeout=1s) cargo test")
            .expect_err("timeout is not an implemented mode parameter");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(err.message.contains("unknown mode parameter `timeout`"));
    }

    #[test]
    fn mode_params_reject_values_with_wrong_type() {
        let err = Parser::parse(":run(pty=soon) cargo test").expect_err("pty needs bool");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(err.message.contains("pty"));
        assert!(err.message.contains("a boolean"));

        let err = Parser::parse(":run(retry=-1) cargo test")
            .expect_err("retry is not an implemented mode parameter");
        assert_eq!(err.kind, ParseErrorKind::InvalidModeParam);
        assert!(err.message.contains("unknown mode parameter `retry`"));
    }

    #[test]
    fn oversized_id_ref_is_rejected_for_id_commands() {
        let err = Parser::parse(":kill J4294967296").expect_err("oversized ID should fail");
        assert_eq!(err.kind, ParseErrorKind::InvalidIdRef);
        assert!(err.message.contains("requires an ID"));
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
    fn parse_send_raw_preserves_quoted_operators() {
        // Chain operators without whitespace must be quoted when passed as text.
        let ast = Parser::parse(":send J1 replace 'a->b' with 'c->d'").unwrap();
        match ast {
            Ast::Command { name, argument, .. } => {
                assert_eq!(name, "send");
                assert_eq!(
                    argument,
                    Argument::Text("J1 replace 'a->b' with 'c->d'".into())
                );
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn parse_send_raw_rejects_unquoted_chain_operator_without_boundary() {
        let err = Parser::parse(":send J1 replace a->b").unwrap_err();
        assert!(err.message.contains("must be surrounded by whitespace"));
    }

    #[test]
    fn parse_send_requires_job_target() {
        let cron_target = Parser::parse(":send C1 input").expect_err("send requires job id");
        assert_eq!(cron_target.kind, ParseErrorKind::InvalidIdRef);
        assert!(cron_target.message.contains("J<n>"));

        let text_target = Parser::parse(":send process input").expect_err("send requires id");
        assert_eq!(text_target.kind, ParseErrorKind::InvalidIdRef);
        assert!(text_target.message.contains("target ID"));
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
        // cargo build |> grep error -> cargo test ||| cargo clippy
        let ast =
            Parser::parse(":run cargo build |> grep error -> cargo test ||| cargo clippy").unwrap();
        match ast {
            Ast::Command { argument, .. } => match argument {
                Argument::Chain(ChainNode::Serial { left, right, .. }) => {
                    // left = pipeline (cargo build |> grep error)
                    if let ChainNode::Leaf(_) = *left {
                        let p = leaf_pipeline(&left);
                        assert_eq!(p.segments.len(), 2);
                        assert_eq!(p.segments[0].command, vec!["cargo", "build"]);
                        assert_eq!(p.segments[0].pipe_to_next, Some(PipeOp::Stdout));
                        assert_eq!(p.segments[1].command, vec!["grep", "error"]);
                    } else {
                        panic!("expected Leaf pipeline");
                    }
                    // right = parallel (cargo test ||| cargo clippy)
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
                Argument::Chain(chain) => {
                    let p = leaf_pipeline(&chain);
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
                Argument::Chain(chain) => {
                    let p = leaf_pipeline(&chain);
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
        let ast = Parser::parse("cat a\n||| cat b").unwrap();
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

    #[test]
    fn parse_file_script_wraps_single_item_as_script() {
        let ast = Parser::parse_file_script(":run cargo test").unwrap();
        match ast {
            Ast::Script { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].source, ":run cargo test");
            }
            _ => panic!("expected Script"),
        }
    }

    #[test]
    fn parse_file_script_ignores_leading_shebang() {
        let ast = Parser::parse_file_script("#!/usr/bin/env cue\n:run cargo test").unwrap();
        match ast {
            Ast::Script { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].source, ":run cargo test");
            }
            _ => panic!("expected Script"),
        }
    }

    #[test]
    fn parse_file_script_rejects_comment_only_file() {
        let err = Parser::parse_file_script("#!/usr/bin/env cue\n# only comments\n  # more\n")
            .expect_err("comment-only file should be invalid");
        assert_eq!(err.kind, ParseErrorKind::MissingArgument);
        assert_eq!(err.message, "empty .cue script");
    }

    #[test]
    fn parse_file_script_ignores_comment_lines_between_items() {
        let ast = Parser::parse_file_script(":run cargo fmt\n# skip me\n:run cargo test").unwrap();
        match ast {
            Ast::Script { items, .. } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0].source, ":run cargo fmt");
                assert_eq!(items[1].source, ":run cargo test");
            }
            _ => panic!("expected Script"),
        }
    }

    #[test]
    fn parse_file_script_strips_trailing_comments() {
        let ast = Parser::parse_file_script(":run echo ok # note").unwrap();
        match ast {
            Ast::Script { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].source, ":run echo ok");
                match &*items[0].statement {
                    Ast::Command { argument, .. } => match argument {
                        Argument::Chain(chain) => {
                            let p = leaf_pipeline(chain);
                            assert_eq!(p.segments[0].command, vec!["echo", "ok"]);
                        }
                        _ => panic!("expected Chain"),
                    },
                    _ => panic!("expected Command"),
                }
            }
            _ => panic!("expected Script"),
        }
    }

    #[test]
    fn parse_file_script_preserves_hash_inside_quotes() {
        let ast = Parser::parse_file_script(":run echo '#literal' \"#also-literal\"").unwrap();
        match ast {
            Ast::Script { items, .. } => match &*items[0].statement {
                Ast::Command { argument, .. } => match argument {
                    Argument::Chain(chain) => {
                        let p = leaf_pipeline(chain);
                        assert_eq!(
                            p.segments[0].command,
                            vec!["echo", "#literal", "#also-literal"]
                        );
                    }
                    _ => panic!("expected Chain"),
                },
                _ => panic!("expected Command"),
            },
            _ => panic!("expected Script"),
        }
    }
}
