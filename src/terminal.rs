use std::io;

use anyhow::{Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

const MAX_LINE_BYTES: usize = 16 * 1024;

/// A terminal adapter that accepts the line endings commonly used by radio and
/// terminal clients, while always writing CRLF.
pub struct Terminal<S> {
    reader: BufReader<S>,
    skip_optional_lf: bool,
}

impl<S> Terminal<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            reader: BufReader::new(stream),
            skip_optional_lf: false,
        }
    }

    pub async fn read_line(&mut self) -> Result<Option<String>> {
        self.consume_optional_lf().await?;

        let mut line = Vec::new();
        loop {
            let (bytes_to_take, terminator) = {
                let available = self.reader.fill_buf().await?;
                if available.is_empty() {
                    if line.is_empty() {
                        return Ok(None);
                    }
                    return decode_line(line).map(Some);
                }

                if let Some(position) = available
                    .iter()
                    .position(|byte| matches!(byte, b'\r' | b'\n'))
                {
                    append_with_limit(&mut line, &available[..position])?;
                    (position + 1, Some(available[position]))
                } else {
                    append_with_limit(&mut line, available)?;
                    (available.len(), None)
                }
            };

            self.reader.consume(bytes_to_take);
            if let Some(terminator) = terminator {
                self.skip_optional_lf = terminator == b'\r';
                return decode_line(line).map(Some);
            }
        }
    }

    pub async fn write(&mut self, text: &str) -> io::Result<()> {
        self.reader.get_mut().write_all(text.as_bytes()).await?;
        self.reader.get_mut().flush().await
    }

    pub async fn write_line(&mut self, text: &str) -> io::Result<()> {
        self.write(text).await?;
        self.write("\r\n").await
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.reader.get_mut().shutdown().await
    }

    async fn consume_optional_lf(&mut self) -> Result<()> {
        if !self.skip_optional_lf {
            return Ok(());
        }

        let consume_lf = {
            let available = self.reader.fill_buf().await?;
            if available.is_empty() {
                self.skip_optional_lf = false;
                return Ok(());
            }
            available.first() == Some(&b'\n')
        };
        if consume_lf {
            self.reader.consume(1);
        }
        self.skip_optional_lf = false;
        Ok(())
    }
}

fn append_with_limit(line: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    if line.len() + bytes.len() > MAX_LINE_BYTES {
        bail!("input line is longer than {MAX_LINE_BYTES} bytes");
    }
    line.extend_from_slice(bytes);
    Ok(())
}

fn decode_line(line: Vec<u8>) -> Result<String> {
    String::from_utf8(line).map_err(|_| anyhow::anyhow!("input is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncWriteExt, duplex};

    use super::Terminal;

    #[tokio::test]
    async fn accepts_cr_crlf_and_lf() {
        let (mut client, server) = duplex(128);
        client.write_all(b"one\rtwo\r\nthree\n").await.unwrap();
        client.shutdown().await.unwrap();

        let mut terminal = Terminal::new(server);
        assert_eq!(terminal.read_line().await.unwrap().as_deref(), Some("one"));
        assert_eq!(terminal.read_line().await.unwrap().as_deref(), Some("two"));
        assert_eq!(
            terminal.read_line().await.unwrap().as_deref(),
            Some("three")
        );
        assert_eq!(terminal.read_line().await.unwrap(), None);
    }
}
