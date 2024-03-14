use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream};

fn main() {
    println!("Logs from your program will appear here!");

    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                handle(s);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


fn handle(mut s: TcpStream) {
    let mut buf = [0u8; 1024];
    loop {
        let read_count = s.read(&mut buf).expect("Could not read from client");
        if read_count == 0 {
            return;
        }
        s.write_all(b"+PONG\r\n");
    }
}
