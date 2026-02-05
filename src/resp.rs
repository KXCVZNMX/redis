use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
}

impl Value {
    pub fn serialise(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{s}\r\n"),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            _ => panic!("Unsupported token"),
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buf: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> RespHandler {
        RespHandler {
            stream,
            buf: BytesMut::with_capacity(1024),
        }
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<Value>> {
        let bytes_len = self.stream.read_buf(&mut self.buf).await?;

        if bytes_len == 0 {
            return Ok(None)
        }

        let (v, _) = parse_message(self.buf.split())?;

        Ok(Some(v))
    }

    pub async fn write(&mut self, value: Value) -> anyhow::Result<()> {
        self.stream.write(value.serialise().as_bytes()).await?;

        Ok(())
    }
}

fn parse_message(buf: BytesMut) -> anyhow::Result<(Value, usize)> {
    match buf[0] as char {
        '+' => parse_simple_string(buf),
        '$' => parse_bulk_string(buf),
        '*' => parse_array(buf),
        _ => Err(anyhow::anyhow!("Invalid message: {:?}", buf)),
    }
}

fn parse_simple_string(buf: BytesMut) -> anyhow::Result<(Value, usize)> {
    if let Some((line, len)) = read_until_crlf(&buf) {
        let string = String::from_utf8(line.to_vec())?;

        return Ok((Value::SimpleString(string), len))
    }

    Err(anyhow::anyhow!("Invalid string: {:?}", buf))
}

fn parse_bulk_string(buf: BytesMut) -> anyhow::Result<(Value, usize)> {
    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buf[1..]) {
        let bulk_str_len = parse_int(line)?;

        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid bulk string format {:?}", buf));
    };

    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((Value::BulkString(String::from_utf8(buf[bytes_consumed..end_of_bulk_str].to_vec())?), total_parsed))
}

fn parse_array(buf: BytesMut) -> anyhow::Result<(Value, usize)> {
    let (array_length, mut bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buf[1..]) {
        let array_length = parse_int(line)?;

        (array_length, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buf));
    };

    let mut items = vec![];
    for _ in 0..array_length {
        let (array_item, len) = parse_message(BytesMut::from(&buf[bytes_consumed..]))?;

        items.push(array_item);
        bytes_consumed += len;
    }

    Ok((Value::Array(items), bytes_consumed))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

fn parse_int(buffer: &[u8]) -> anyhow::Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}