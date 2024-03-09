use sqlite3_ext::Connection;
use sqlite3_ext::Result;

/// Defines behavior for managing template tables, including creation, copying, and schema definition.
///
/// This trait encapsulates methods required to create template tables, generate creation queries,
/// copy existing templates, and retrieve table schema information.
pub trait Interface {
    /// Generates a SQL query string to create a new table.
    ///
    /// # Returns
    /// - `String`: A SQL query string for creating a new table.
    fn create_table_query(&self) -> String;

    /// Creates the template table in the specified database connection.
    ///
    /// # Parameters
    /// - `db`: A reference to the database connection.
    ///
    /// # Returns
    /// - `Result<bool>`: Returns `Ok(true)` if the table creation was successful.
    fn create_table(&self, db: &Connection) -> Result<bool>;

    /// Retrieves the base name of the template table, without the postfix.
    ///
    /// # Returns
    /// - `Option<&str>`: The base name of the template table if it can be derived from the current name.
    fn get_base_name(&self) -> Option<&str>;

    /// Generates a string representation of the column declarations for the template table.
    ///
    /// # Returns
    /// - `String`: A comma-separated string of column declarations.
    fn get_column_declarations(&self) -> String;

    fn drop_table_query(&self) -> String;
    fn drop_table(&self, db: &Connection);
}
