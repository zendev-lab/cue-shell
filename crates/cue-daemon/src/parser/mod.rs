//! Command parser for cue-shell.
//!
//! Three-layer pipeline (all running inside cued):
//! ```text
//! Raw input (String)
//!   → Tokenizer  → Vec<Token>
//!   → Parser     → Ast (unresolved)
//!   → Resolver   → validated, ready for execution
//! ```

mod ast;
mod duration;
mod parse;
mod resolver;
mod token;
mod tokenizer;

use cue_core::mode::Mode;

pub(crate) use parse::ParseError;
pub(crate) use resolver::{ResolvedCommand, ResolvedScriptItem};
pub(crate) use token::Token;
pub(crate) use tokenizer::Tokenizer;

pub(crate) fn parse_command(input: &str, mode: Mode) -> Result<ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse(input)?;
    resolver::Resolver::resolve(ast, mode)
}

pub(crate) fn parse_file_script_command(input: &str) -> Result<ResolvedCommand, ParseError> {
    let ast = parse::Parser::parse_file_script(input)?;
    resolver::Resolver::resolve(ast, Mode::Job)
}
