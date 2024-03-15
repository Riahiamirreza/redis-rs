use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::{
    collections,
    sync::{Arc, Mutex},
    thread, time,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let data_storage = Arc::new(Mutex::new(collections::HashMap::<
        String,
        (Option<time::Instant>, Vec<u8>),
    >::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let cloned_storage = data_storage.clone();
                thread::spawn(move || handle(s, cloned_storage));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle(
    mut stream: TcpStream,
    storage: Arc<Mutex<collections::HashMap<String, (Option<time::Instant>, Vec<u8>)>>>,
) {
    let mut buf = [0u8; 64];
    loop {
        let read_count = stream.read(&mut buf).expect("Could not read from client");
        if read_count == 0 {
            return;
        }
        let mut new_buf = Vec::new();
        for i in 0..read_count {
            new_buf.push(buf[i]);
        }
        match Command::from_buffer(&new_buf.as_slice()) {
            Ok(Command::Ping) => {
                stream.write_all(b"+PONG\r\n");
            }
            Ok(Command::Echo(s)) => {
                let out = serialize_to_bulk_string(s.as_bytes());
                stream.write_all(out.as_slice());
            }
            Ok(Command::Set(key, value, expiry)) => {
                let mut storage = storage.lock().unwrap();
                let expiry =
                    expiry.map(|t| time::Instant::now() + time::Duration::from_millis(t as u64));
                storage.insert(key, (expiry, value));
                let out = serialize_to_simple_string("OK".as_bytes());
                stream.write_all(out.as_slice());
            }
            Ok(Command::Get(key)) => {
                let mut storage = storage.lock().unwrap();
                match storage.get(&key) {
                    Some((expiry, v)) => {
                        if let Some(expiry) = expiry {
                            if time::Instant::now() >= *expiry {
                                stream.write_all(b"$-1\r\n");
                            } else {
                                let out = serialize_to_bulk_string(v);
                                stream.write_all(out.as_slice());
                            }
                        } else {
                            let out = serialize_to_bulk_string(v);
                            stream.write_all(out.as_slice());
                        }
                    }
                    None => {
                        stream.write_all(b"$-1\r\n");
                    }
                }
            }
            Err(_) => {
                stream.write_all(b"-Error\r\n");
            }
        }
    }
}

fn serialize_to_simple_string(s: &[u8]) -> Vec<u8> {
    [b"+", s, b"\r\n"].concat()
}

fn serialize_to_bulk_string(s: &[u8]) -> Vec<u8> {
    [b"$", format!("{}", s.len()).as_bytes(), b"\r\n", s, b"\r\n"].concat()
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, Vec<u8>, Option<u64>),
    Get(String),
}

enum DataType {
    SimpleString,
    SimpleErr,
    Integer,
    BulkString,
    Array,
}

impl DataType {
    fn from_byte(b: u8) -> Self {
        match b {
            b'+' => Self::SimpleString,
            b'-' => Self::SimpleErr,
            b':' => Self::Integer,
            b'$' => Self::BulkString,
            b'*' => Self::Array,
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
enum RedisObject {
    SimpleString(String),
    SimpleErr(String),
    Integer(i32),
    BulkString(usize, String),
    Array(Vec<RedisObject>),
}

struct Parser<'a> {
    stream: &'a [u8],
}

impl<'a> Parser<'a> {
    fn new(stream: &'a [u8]) -> Self {
        Self { stream }
    }

    fn parse(&mut self) -> Result<RedisObject, ()> {
        match Self::parse_object(self.stream) {
            Ok((Some(object), _)) => Ok(object),
            Ok(_) => Err(()),
            Err(_) => Err(()),
        }
    }

    fn parse_object(stream: &[u8]) -> Result<(Option<RedisObject>, usize), ()> {
        if stream[0..2] == *b"\r\n" {
            return Ok((None, 2));
        }
        match DataType::from_byte(stream[0]) {
            DataType::Array => Parser::parse_array(&stream[1..]),
            DataType::SimpleString => {
                let parts = split_by_line(&stream[1..]);
                Ok((
                    Some(RedisObject::SimpleString(
                        String::from_utf8(parts[0].clone()).unwrap(),
                    )),
                    parts[0].len(),
                ))
            }
            DataType::Integer => {
                let parts = split_by_line(&stream[1..]);
                Ok((
                    Some(RedisObject::Integer(
                        String::from_utf8(parts[0].clone())
                            .unwrap()
                            .parse::<i32>()
                            .unwrap(),
                    )),
                    parts[0].len(),
                ))
            }
            DataType::BulkString => {
                let parts = split_by_line(&stream[1..]);
                let Ok(size) = String::from_utf8(parts[0].clone())
                    .unwrap()
                    .parse::<usize>()
                else {
                    panic!("invalid string");
                };
                let string = String::from_utf8(parts[1].clone()).unwrap();
                assert!(string.len() == size as usize);
                Ok((
                    Some(RedisObject::BulkString(size, string)),
                    parts[0].len() + parts[1].len() + 3,
                ))
            }
            _ => unimplemented!("type not implemented"),
        }
    }

    fn parse_array(stream: &[u8]) -> Result<(Option<RedisObject>, usize), ()> {
        let parts = split_by_line(stream);
        let _size = String::from_utf8(parts[0].clone())
            .expect("invalid string")
            .parse::<usize>()
            .expect("invalid string");
        let mut objects = vec![];
        let mut pos: usize = parts[0].len() + 2;
        loop {
            match Parser::parse_object(&stream[pos..]) {
                Ok((Some(object), consumed)) => {
                    objects.push(object);
                    pos += consumed;
                    if pos > stream.len() {
                        break;
                    }
                }
                Ok((None, consumed)) => {
                    pos += consumed;
                    if pos >= stream.len() {
                        break;
                    }
                }
                Err(_) => {
                    return Err(());
                }
            }
        }
        println!("{objects:?}");
        Ok((Some(RedisObject::Array(objects)), pos))
    }
}

fn split_by_line(stream: &[u8]) -> Vec<Vec<u8>> {
    let line_positions = stream
        .windows(2)
        .enumerate()
        .filter(|(_, w)| w == b"\r\n")
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    let mut lines = vec![stream[..line_positions[0]].to_vec()];
    lines.extend(
        line_positions
            .windows(2)
            .map(|i| stream[i[0] + 2..i[1]].to_vec())
            .collect::<Vec<_>>(),
    );
    lines.push(stream[*line_positions.last().unwrap() + 2..].to_vec());
    lines
        .into_iter()
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
}

impl Command {
    fn from_buffer(buf: &[u8]) -> Result<Self, ()> {
        let mut p = Parser::new(buf);
        match p.parse() {
            Ok(object) => match object {
                RedisObject::Array(arr) => match arr.as_slice() {
                    [RedisObject::BulkString(4, s)] => {
                        if s.to_uppercase() == "PING".to_string() {
                            Ok(Command::Ping)
                        } else {
                            Err(())
                        }
                    }
                    [RedisObject::BulkString(4, s), RedisObject::BulkString(_, o)] => {
                        if s.to_uppercase() == "ECHO".to_string() {
                            Ok(Command::Echo(o.to_string()))
                        } else {
                            Err(())
                        }
                    }
                    [RedisObject::BulkString(3, s), RedisObject::BulkString(_, key)] => {
                        if s.to_uppercase() == "GET".to_string() {
                            Ok(Command::Get(key.to_string()))
                        } else {
                            Err(())
                        }
                    }
                    [RedisObject::BulkString(3, s), RedisObject::BulkString(_, key), RedisObject::BulkString(_, value), RedisObject::BulkString(2, ex), RedisObject::BulkString(_, duration)] => {
                        if s.to_uppercase() == "SET".to_string()
                            && ex.to_uppercase() == "PX"
                            && duration.parse::<u64>().is_ok()
                        {
                            Ok(Command::Set(
                                key.to_string(),
                                value.as_bytes().to_vec(),
                                Some(duration.parse::<u64>().unwrap()),
                            ))
                        } else {
                            Err(())
                        }
                    }
                    [RedisObject::BulkString(3, s), RedisObject::BulkString(_, key), RedisObject::BulkString(_, value)] => {
                        if s.to_uppercase() == "SET".to_string() {
                            Ok(Command::Set(
                                key.to_string(),
                                value.as_bytes().to_vec(),
                                None,
                            ))
                        } else {
                            Err(())
                        }
                    }
                    _ => Err(()),
                },
                _ => Err(()),
            },
            Err(_) => Err(()),
        }
    }
}
