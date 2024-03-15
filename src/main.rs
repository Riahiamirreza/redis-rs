use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(move || handle(s));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle(mut stream: TcpStream) {
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
                let out = [b"$", format!("{}", s.len()).as_bytes() , b"\r\n" , s.as_bytes() , b"\r\n"].concat();
                stream.write_all(out.as_slice());
            }
            Err(_) => {
                stream.write_all(b"+PONG\r\n");
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
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
            x => unimplemented!("hello {}", x),
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
        let size = String::from_utf8(parts[0].clone())
            .expect("invalid string")
            .parse::<usize>()
            .expect("invalid string");
        let mut objects = vec![];
        let mut pos: usize = parts[0].len() + 2;
        loop {
            match Parser::parse_object(&stream[pos..]) {
                Ok((Some(object), consumed)) => {
                    println!("object ok: {:?}", object);
                    objects.push(object);
                    pos += consumed;
                    if pos > stream.len() {
                        println!("here?, {}, {}, {}", pos, stream.len(), consumed);
                        break;
                    }
                }
                Ok((None, consumed)) => {
                    pos += consumed;
                    if pos >= stream.len() {
                        println!("here?, {}, {}, {}", pos, stream.len(), consumed);
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
        let mut q = Parser::new(buf);
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
                    _ => Err(()),
                },
                _ => Err(()),
            },
            Err(_) => Err(()),
        }
    }
}
