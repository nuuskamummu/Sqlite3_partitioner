mod error;

pub mod shadow_tables;
pub mod types;
pub mod utils;
pub mod vtab_interface;
pub use shadow_tables::{Lookup, LookupTable, Root, RootTable, Template, TemplateTable};
pub use types::*;
pub use vtab_interface::operations;
pub use vtab_interface::*;
//pub use partition::*;

// Additional FFI helper functions as needed
//
