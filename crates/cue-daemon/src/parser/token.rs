use std::fmt;
use std::time::Duration;

/// Fine-grained token types produced by the Tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Command prefix
    /// `:` prefix for builtin commands.
    Colon,
    /// Command name immediately after `:`, e.g. `run`, `kill`, `jobs`.
    Command(String),

    // Mode params (context-sensitive: immediately after Command)
    /// `(` in mode-params context.
    ModeParenOpen,
    /// `)` in mode-params context.
    ModeParenClose,
    /// Parameter key in mode-params, e.g. `cwd`, `retry`.
    ParamKey(String),
    /// `=` in mode-params.
    ParamEq,
    /// Parameter value in mode-params.
    ParamValue(Value),
    /// `,` separator in mode-params.
    Comma,

    // Chain operators (job-level)
    /// `->` serial-then.
    SerialThen,
    /// `~>` serial-always.
    SerialAlways,
    /// `|||` parallel-all.
    ParallelAll,
    /// `|?|` parallel-race.
    ParallelRace,
    /// `&&` job-internal AND.
    JobAnd,
    /// `||` job-internal OR.
    JobOr,

    // Pipe operators (process-level, within a job)
    /// `|>` stdout pipe.
    PipeStdout,
    /// `|&>` stdout+stderr pipe.
    PipeAll,
    /// `|!>` stderr-only pipe.
    PipeStderr,

    // Grouping (chain-level)
    /// `(` for chain grouping.
    GroupOpen,
    /// `)` for chain grouping.
    GroupClose,

    // Content
    /// A word (command argument, filename, flag, etc.)
    Word(String),
    /// An entity ID reference like J1, C3, S0.
    IdRef(IdKind, u32),

    // Whitespace (preserved for highlighting, skipped during parsing)
    Whitespace(String),
    Newline,

    // Sentinel
    Eof,
}

/// Entity ID prefix kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    Job,
    Cron,
    Scope,
}

/// Typed value in mode-params.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Duration(Duration),
    Str(String),
    Bool(bool),
}

/// A token with its byte-offset span in the original input.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned {
    pub token: Token,
    pub span: Span,
}

/// Byte-offset range in the original input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

impl fmt::Display for IdKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Job => "J",
            Self::Cron => "C",
            Self::Scope => "S",
        })
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Colon => f.write_str(":"),
            Self::Command(s) => write!(f, "{s}"),
            Self::ModeParenOpen | Self::GroupOpen => f.write_str("("),
            Self::ModeParenClose | Self::GroupClose => f.write_str(")"),
            Self::ParamKey(s) => write!(f, "{s}"),
            Self::ParamEq => f.write_str("="),
            Self::ParamValue(v) => write!(f, "{v:?}"),
            Self::Comma => f.write_str(","),
            Self::SerialThen => f.write_str("->"),
            Self::SerialAlways => f.write_str("~>"),
            Self::ParallelAll => f.write_str("|||"),
            Self::ParallelRace => f.write_str("|?|"),
            Self::JobAnd => f.write_str("&&"),
            Self::JobOr => f.write_str("||"),
            Self::PipeStdout => f.write_str("|>"),
            Self::PipeAll => f.write_str("|&>"),
            Self::PipeStderr => f.write_str("|!>"),
            Self::Word(s) => write!(f, "{s}"),
            Self::IdRef(k, n) => write!(f, "{k}{n}"),
            Self::Whitespace(s) => write!(f, "{s}"),
            Self::Newline => f.write_str("\\n"),
            Self::Eof => f.write_str("<EOF>"),
        }
    }
}

impl Token {
    pub fn operator_text(&self) -> &'static str {
        match self {
            Self::SerialThen => "->",
            Self::SerialAlways => "~>",
            Self::ParallelAll => "|||",
            Self::ParallelRace => "|?|",
            Self::JobAnd => "&&",
            Self::JobOr => "||",
            Self::PipeStdout => "|>",
            Self::PipeAll => "|&>",
            Self::PipeStderr => "|!>",
            _ => "",
        }
    }
}
