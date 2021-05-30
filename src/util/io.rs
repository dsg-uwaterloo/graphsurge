use crate::error::GSError;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

pub fn get_buf_reader(file_path: &str) -> Result<BufReader<File>, GSError> {
    Ok(BufReader::new(
        File::open(file_path)
            .map_err(|e| GSError::ReadFile(file_path.to_owned(), e.to_string()))?,
    ))
}

pub fn get_file_lines(file_path: &str) -> Result<impl Iterator<Item = String>, GSError> {
    Ok(get_buf_reader(file_path)?.lines().filter_map(Result::ok))
}

pub struct GsWriter {
    buf_writer: BufWriter<File>,
    file_path: String,
}

impl GsWriter {
    pub fn new(file_path: String) -> Result<Self, GSError> {
        let buf_writer = BufWriter::new(
            File::create(&file_path)
                .map_err(|e| GSError::CreateFile(file_path.clone(), e.to_string()))?,
        );
        Ok(Self { buf_writer, file_path })
    }

    #[inline]
    pub fn write_file_lines(&mut self, lines: impl Iterator<Item = String>) -> Result<(), GSError> {
        for line in lines {
            self.write_file_line(&line)?;
        }
        Ok(())
    }

    #[inline]
    pub fn write_file_line(&mut self, line: &str) -> Result<(), GSError> {
        self.buf_writer
            .write([line, "\n"].concat().as_bytes())
            .map_err(|e| GSError::WriteFile(self.file_path.clone(), e.to_string()))?;
        Ok(())
    }

    pub fn into_buf_writer(self) -> BufWriter<File> {
        self.buf_writer
    }
}
