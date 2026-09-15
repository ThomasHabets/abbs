use std::io;

use anyhow::{Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const MAX_LINE_BYTES: usize = 16 * 1024;
const ZMODEM_START: &[u8] = b"**\x18B00";

pub enum TerminalInput {
    Line(String),
    Zmodem(Vec<u8>),
}

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

    pub async fn read_input(&mut self) -> Result<Option<TerminalInput>> {
        self.consume_optional_lf().await?;
        let mut input = Vec::new();
        loop {
            let byte = {
                let available = self.reader.fill_buf().await?;
                if available.is_empty() {
                    return if input.is_empty() {
                        Ok(None)
                    } else {
                        decode_line(input).map(|line| Some(TerminalInput::Line(line)))
                    };
                }
                available[0]
            };
            self.reader.consume(1);
            if matches!(byte, b'\r' | b'\n') {
                self.skip_optional_lf = byte == b'\r';
                return decode_line(input).map(|line| Some(TerminalInput::Line(line)));
            }
            append_with_limit(&mut input, &[byte])?;
            if input == ZMODEM_START {
                return Ok(Some(TerminalInput::Zmodem(input)));
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

    pub async fn read_bytes(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            let read = self.reader.read(buffer).await?;
            if read == 0 {
                return Ok(0);
            }
            if self.skip_optional_lf {
                self.skip_optional_lf = false;
                if buffer[0] == b'\n' {
                    if read == 1 {
                        continue;
                    }
                    buffer.copy_within(1..read, 0);
                    return Ok(read - 1);
                }
            }
            return Ok(read);
        }
    }

    /// Consume the `OO` acknowledgement that a ZMODEM sender emits after the
    /// receiver's final ZFIN.  Any bytes after it remain buffered for normal
    /// terminal input, so a following command cannot become `OO<command>`.
    pub async fn consume_zmodem_final_ack(&mut self) -> Result<bool> {
        let available = self.reader.fill_buf().await?;
        if available.is_empty() || available[0] != b'O' {
            return Ok(false);
        }
        self.reader.consume(1);

        let available = self.reader.fill_buf().await?;
        if available.first() != Some(&b'O') {
            bail!("invalid ZMODEM final acknowledgement");
        }
        self.reader.consume(1);
        Ok(true)
    }

    pub async fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.reader.get_mut().write_all(bytes).await?;
        self.reader.get_mut().flush().await
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

    #[tokio::test]
    async fn raw_reads_discard_the_lf_after_a_command_crlf() {
        let (mut client, server) = duplex(128);
        client.write_all(b"DOWNLOAD file\r\nZMODEM").await.unwrap();
        client.shutdown().await.unwrap();

        let mut terminal = Terminal::new(server);
        assert_eq!(
            terminal.read_line().await.unwrap().as_deref(),
            Some("DOWNLOAD file")
        );
        let mut buffer = [0; 16];
        let read = terminal.read_bytes(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..read], b"ZMODEM");
    }

    #[tokio::test]
    async fn detects_a_zmodem_header_before_line_decoding() {
        let (mut client, server) = duplex(128);
        client.write_all(b"**\x18B00rest").await.unwrap();

        let mut terminal = Terminal::new(server);
        let Some(super::TerminalInput::Zmodem(header)) = terminal.read_input().await.unwrap()
        else {
            panic!("ZMODEM header was not detected");
        };
        assert_eq!(header, b"**\x18B00");
    }

    #[tokio::test]
    async fn zmodem_final_ack_does_not_prefix_the_next_command() {
        let (mut client, server) = duplex(128);
        client.write_all(b"OOFILES\r").await.unwrap();

        let mut terminal = Terminal::new(server);
        assert!(terminal.consume_zmodem_final_ack().await.unwrap());
        assert_eq!(
            terminal.read_line().await.unwrap().as_deref(),
            Some("FILES")
        );
    }
}
