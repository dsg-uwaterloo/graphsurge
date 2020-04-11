use crate::error::{io_error, ErrorType, GraphSurgeError};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

pub fn get_buf_reader(file_path: &str) -> Result<BufReader<File>, GraphSurgeError> {
    Ok(BufReader::new(File::open(file_path).map_err(|e| {
        GraphSurgeError::new(
            ErrorType::IOError,
            format!("Could not open file '{}' for reading: {}", file_path, e),
        )
    })?))
}

pub fn get_file_lines(file_path: &str) -> Result<impl Iterator<Item = String>, GraphSurgeError> {
    Ok(get_buf_reader(file_path)?.lines().filter_map(Result::ok))
}

pub struct GSWriter {
    buf_writer: BufWriter<File>,
    file_path: String,
}

impl GSWriter {
    pub fn new(file_path: String) -> Result<Self, GraphSurgeError> {
        let buf_writer = BufWriter::new(File::create(&file_path).map_err(|e| {
            GraphSurgeError::new(
                ErrorType::IOError,
                format!("Could not create file '{}' for writing: {}", file_path, e),
            )
        })?);
        Ok(Self { buf_writer, file_path })
    }

    #[inline]
    pub fn write_file_lines(
        &mut self,
        lines: impl Iterator<Item = String>,
    ) -> Result<(), GraphSurgeError> {
        for line in lines {
            self.write_file_line(&line)?;
        }
        Ok(())
    }

    #[inline]
    pub fn write_file_line(&mut self, line: &str) -> Result<(), GraphSurgeError> {
        self.buf_writer
            .write([line, "\n"].concat().as_bytes())
            .map_err(|e| io_error(format!("Could not write to '{}': {}", self.file_path, e)))?;
        Ok(())
    }

    pub fn into_buf_writer(self) -> BufWriter<File> {
        self.buf_writer
    }
}
