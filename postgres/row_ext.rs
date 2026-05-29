use std::fmt::Display;

use tokio_postgres::{Row, row::RowIndex, types::FromSql};

/// Extension methods for a single [`Row`].
pub trait RowExt {
    /// Gets a column value, returning `default` if the column is absent or the
    /// type conversion fails.
    fn get_or<'a, I, T>(&'a self, index: I, default: T) -> T
    where
        T: FromSql<'a> + Clone,
        I: RowIndex + Display;
}

impl RowExt for Row {
    fn get_or<'a, I, T>(&'a self, index: I, default: T) -> T
    where
        T: FromSql<'a> + Clone,
        I: RowIndex + Display,
    {
        self.try_get::<I, T>(index).unwrap_or(default)
    }
}

/// Extension methods for a slice of [`Row`]s.
pub trait RowVecExt {
    /// Gets a value at `(row_index, column)`, returning `default` if either
    /// the row index is out of range or the column conversion fails.
    fn get_or<'a, I, T>(&'a self, row_index: usize, column: I, default: T) -> T
    where
        T: FromSql<'a> + Clone,
        I: RowIndex + Display;
}

impl<S: AsRef<[Row]>> RowVecExt for S {
    fn get_or<'a, I, T>(&'a self, row_index: usize, column: I, default: T) -> T
    where
        T: FromSql<'a> + Clone,
        I: RowIndex + Display,
    {
        match self.as_ref().get(row_index) {
            Some(row) => row.get_or(column, default),
            None => default,
        }
    }
}
