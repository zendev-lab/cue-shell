//! Command parser for cue-shell.
//!
//! Three-layer pipeline (all running inside cued):
//! ```text
//! Raw input (String)
//!   → Tokenizer  → Vec<Token>
//!   → Parser     → Ast (unresolved)
//!   → Resolver   → validated, ready for execution
//! ```

pub mod ast;
pub mod parse;
pub mod resolver;
pub mod token;
pub mod tokenizer;

pub use ast::Ast;
pub use parse::Parser;
pub use resolver::Resolver;
pub use token::Token;
pub use tokenizer::Tokenizer;
