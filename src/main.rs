use std::{
    collections::{self, HashMap},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread, time,
    fs,
};

use clap::Parser;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    dir: Option<String>,
    #[arg(long("dbfilename"))]
    db_filename: Option<String>,
}

fn init_config(conf: &mut Config) {
    let args = Args::parse();
    conf.dir = args.dir;
    conf.db_filename = args.db_filename;
}

struct State {
    config: Mutex<Config>,
    storage: Mutex<HashMap<String, (Option<time::Instant>, Vec<u8>)>>,
}

struct RDBObject {
    version: String,
    path: String,
    metadata: HashMap<String, String>,
    storage: HashMap<String, (Option<time::Instant>, Vec<u8>)>,
}

enum RDBFileObject {
    Str(Vec<u8>),
    Integer(i64)
}

/*
It implements this: https://rdb.fnordig.de/file_format.html#length-encoding
*/
fn decode_object(data:&[u8]) -> Option<(RDBFileObject, usize)> {
    let first_byte = data[0];

    match first_byte >> 6 {
        0 => {
            let size = (first_byte & 63) as usize;
            Some((RDBFileObject::Str(data[1..size+1].to_vec()), size + 1))
        }
        1=> {
            let second_byte = data[1];
            let size = ((first_byte & 63) + (second_byte << 6)) as usize;
            Some((RDBFileObject::Str(data[2..size+2].to_vec()), size + 2))
        }
        2 => {
            unimplemented!()
        }
        3 => {
            let remaining = first_byte & 63;
            match remaining {
                0 => {
                    Some((RDBFileObject::Integer(data[1] as i64), 2 as usize))
                }
                1 => {
                    let mut r = data[1] as i64;
                    r = (r << 8) + data[2] as i64;
                    Some((RDBFileObject::Integer(r), 3 as usize))
                }
                2 => {
                    let mut r = data[1] as i64;
                    r = (r << 8) + data[2] as i64;
                    r = (r << 8) + data[3] as i64;
                    r = (r << 8) + data[4] as i64;
                    Some((RDBFileObject::Integer(r), 5 as usize))
                }
                3 => {
                    unimplemented!()
                }
                _ => {
                    panic!();
                }
            }
        }
        _ => None
    }


}

fn decode_length(data: &[u8]) -> Result<(u64, usize), ()> {

    let first_byte = data[0];

    match first_byte >> 6 {
        0 => {
            let size = (first_byte & 63) as u64;
            Ok((size, 1 as usize))
        }
        1=> {
            let second_byte = data[1];
            let size = ((first_byte & 63) + (second_byte << 6)) as u64;
            Ok((size, 2))
        }
        2 => {
            unimplemented!()
        }
        3 => {
            let remaining = first_byte & 63;
            match remaining {
                0 => {
                    Ok(((data[1] as u64), 2 as usize))
                }
                1 => {
                    let mut r = data[1] as u64;
                    r = (r << 8) + data[2] as u64;
                    Ok((r, 3 as usize))
                }
                2 => {
                    let mut r = data[1] as u64;
                    r = (r << 8) + data[2] as u64;
                    r = (r << 8) + data[3] as u64;
                    r = (r << 8) + data[4] as u64;
                    Ok((r, 5 as usize))
                }
                3 => {
                    unimplemented!()
                }
                what => {
                    println!("{}", what);
                    panic!();
                }
            }
        }
        _ => panic!()
    }
}

impl RDBObject {
    fn from_file(path: &str) -> Result<Self, ()> {
        let mut i: usize = 0;
        let Ok(data) = fs::read(path) else {
            return Err(());
        };

        // Checking header:
        if data.len() < 5 || data[i..i+5] != [82, 69, 68, 73, 83]{
            println!("Invalid file header");
            return Err(());
        }

        i = i + 5;
        //skep version:
        i = i + 4;

        // start metadata section:
        if data[i] != 250 {
            println!("Invalid metadata section");
            return Err(());
        } else {
            i = i + 1;
        }

        // Reading metadata:
        let mut metadata: HashMap<String, String> = HashMap::new();
        loop {
            let mut key: String;
            let mut value: String;
            if let Some((obj, consumed)) = decode_object(&data[i..]) {
                if let RDBFileObject::Str(s) = obj {
                    i = i + consumed;
                    key = String::from_utf8(s).expect("Invalid metadata");

                } else {
                    println!("Invalid metadata 11");
                    return Err(());
                }
            } else {
                println!("Invalid string 1");
                return Err(());
            }
            if let Some((obj, consumed)) = decode_object(&data[i..]) {
                match obj {
                    RDBFileObject::Str(s) => {
                        i = i + consumed;
                        value = String::from_utf8(s).expect("Invalid metadata");
                    }
                    RDBFileObject::Integer(integer) => {
                        i = i + consumed;
                        value = format!("{}", integer);
                    }
                }
            } else {
                println!("Invalid string 2");
                return Err(());
            }
            metadata.insert(key, value);
            if data[i] == 250 {
                i += 1;
            } else {
                break;
            }
        }
        println!("{:?}", metadata);

        i += 1;

        let db_index = data[i];
        i += 1;
        if data[i] != 0xfb {
            return Err(());
        }
        i += 1;
        let hash_table_size: u64;
        let expire_table_size: u64;
        if let Ok((size, consumed)) = decode_length(&data[i..]) {
            i += consumed;
            hash_table_size = size;
        } else {
            println!("Error");
            return Err(());
        }
        if let Ok((size, consumed)) = decode_length(&data[i..]) {
            i += consumed;
            expire_table_size = size;
        } else {
            println!("Error");
            return Err(());
        }

        let mut store: HashMap<String, (Option<time::Instant>, Vec<u8>)> = HashMap::new();
        for _ in 0..hash_table_size {
            let type_flag = data[i];
            i += 1;
            match type_flag {
                0 => {
                    let (key, value): (String, Vec<u8>);
                    if let Ok((size, consumed)) = decode_length(&data[i..]) {
                        i = i + consumed;
                        key = String::from_utf8(data[i..i+(size as usize)].to_vec()).expect("Invalid key");
                        i = i + (size as usize);
                        println!("{}", key);
        
                    } else {
                        println!("Invalid key");
                        return Err(());
                    }
                    if let Ok((size, consumed)) = decode_length(&data[i..]) {
                        i = i + consumed;
                        value = data[i..i+(size as usize)].to_vec();
                        i = i + (size as usize);
                        println!("{:?}", value);
        
                    } else {
                        println!("Invalid key");
                        return Err(());
                    }
                    if data[i] == 0xFC {
                        i += 1;
                        let expiry = u64::from_be_bytes(data[i..i+8].try_into().expect(""));
                        let expiry = time::Instant::now() - time::Duration::from_millis(expiry);
                        i += 8;
                        store.insert(key, (Some(expiry), value));
                    } else if data[i] == 0xFD {
                        let expiry = u32::from_be_bytes(data[i..i+4].try_into().expect(""));
                        let expiry = time::Instant::now() - time::Duration::from_secs(expiry as u64);
                        i += 4;
                        store.insert(key, (Some(expiry), value));
                    } else {
                        store.insert(key, (None, value));
                    }
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        Ok(Self {
            path: path.to_string(),
            version: "".to_string(),
            storage: store,
            metadata: metadata
        })
    }
}


fn main() {

    let mut config = Config::new();
    init_config(&mut config);

    let mut rdb_object: Option<RDBObject> = None;
    match (&config.dir, &config.db_filename){
        (Some(dir), Some(db_filename)) => {
            let mut cloned_dir = dir.clone();
            cloned_dir.push_str(db_filename);
            if let Ok(o) = RDBObject::from_file(&cloned_dir) {
                rdb_object = Some(o);
            }
        }
        _ => {}
    }

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();


    let data_storage = match rdb_object {
        Option::Some(s) => s.storage,
        Option::None => collections::HashMap::<String, (Option<time::Instant>, Vec<u8>)>::new()
    };

    let state = Arc::new(State {
        config: Mutex::new(config),
        storage: Mutex::new(data_storage),
    });

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let cloned_state = state.clone();
                thread::spawn(move || handle(s, cloned_state));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle(mut stream: TcpStream, state: Arc<State>) {
    let mut buf = [0u8; 1024];
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
                let mut storage = state.storage.lock().unwrap();
                let expiry =
                    expiry.map(|t| time::Instant::now() + time::Duration::from_millis(t as u64));
                storage.insert(key, (expiry, value));
                let out = serialize_to_simple_string("OK".as_bytes());
                stream.write_all(out.as_slice());
            }
            Ok(Command::Get(key)) => {
                let mut storage = state.storage.lock().unwrap();
                match storage.get(&key) {
                    Some((expiry, v)) => {
                        if let Some(expiry) = expiry {
                            if time::Instant::now() >= *expiry {
                                stream.write_all(b"$-1\r\n");
                                storage.remove(&key);
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
            Ok(Command::Keys(pattern)) => {
                let storage = state.storage.lock().unwrap();
                let mut keys: Vec<String> = Vec::new();

                if &pattern == "*" {
                    for key in storage.keys() {
                        keys.push(key.clone());
                    }

                } else {
                }
                let out = serialize_to_array(&keys.iter().map(|i| i.as_bytes()).collect::<Vec<&[u8]>>());
                stream.write_all(out.as_slice());

            }
            Ok(Command::ConfigGet(key)) => {
                if !["dir", "dbfilename"].contains(&key.as_str()) {
                    stream.write_all(b"-Error\r\n");
                } else {
                    let config = state.config.lock().unwrap();
                    match key.as_str() {
                        "dir" => match config.dir.clone() {
                            Some(dir) => {
                                let out = serialize_to_array(&["dir".as_bytes(), dir.as_bytes()]);
                                stream.write_all(out.as_slice());
                            }
                            None => {
                                stream.write_all(b"-Error\r\n");
                            }
                        },
                        "dbfilename" => match config.db_filename.clone() {
                            Some(db_filename) => {
                                let out = serialize_to_array(&[
                                    "dbfilename".as_bytes(),
                                    db_filename.as_bytes(),
                                ]);
                                stream.write_all(out.as_slice());
                            }
                            None => {}
                        },
                        _ => {
                            stream.write_all(b"-Error\r\n");
                        }
                    }
                }
            }
            Err(_) => {
                stream.write_all(b"-Error\r\n");
            }
        }
    }
}

fn serialize_to_array(strings: &[&[u8]]) -> Vec<u8> {
    [
        b"*",
        format!("{}", strings.len()).as_bytes(),
        b"\r\n",
        strings
            .iter()
            .map(|s| serialize_to_bulk_string(s))
            .collect::<Vec<_>>()
            .concat()
            .as_slice(),
    ]
    .concat()
}
fn serialize_to_simple_string(s: &[u8]) -> Vec<u8> {
    [b"+", s, b"\r\n"].concat()
}

fn serialize_to_bulk_string(s: &[u8]) -> Vec<u8> {
    [b"$", format!("{}", s.len()).as_bytes(), b"\r\n", s, b"\r\n"].concat()
}


struct Config {
    dir: Option<String>,
    db_filename: Option<String>,
}

impl Config {
    fn new() -> Self {
        Self {
            dir: None,
            db_filename: None,
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Set(String, Vec<u8>, Option<u64>),
    Get(String),
    Keys(String),
    ConfigGet(String),
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

struct RESPParser<'a> {
    stream: &'a [u8],
}

impl<'a> RESPParser<'a> {
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
            DataType::Array => RESPParser::parse_array(&stream[1..]),
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
            match RESPParser::parse_object(&stream[pos..]) {
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
        // println!("{objects:?}");
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
        let mut p = RESPParser::new(buf);
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
                        } else if s.to_uppercase() == "KEYS".to_string() {
                            Ok(Command::Keys(o.to_string()))
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
                    [RedisObject::BulkString(6, config), RedisObject::BulkString(3, s), RedisObject::BulkString(_, key)] => {
                        if s.to_uppercase() == "GET".to_string()
                            || config.to_uppercase() == "CONFIG"
                        {
                            Ok(Command::ConfigGet(key.to_string()))
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
