use crate::metrics::{Gather, MetricsProcess};
use http::{response::Builder, Request, Response};
use httparse;
use lunatic::{
    net::{TcpListener, TcpStream},
    process::{ProcessRef, Request as R},
    Mailbox, Process,
};
use std::io::{Read, Result as IoResult, Write};

pub struct HttpTransaction {
    pub stream: TcpStream,
    pub request: Request<Vec<u8>>,
    pub response: Builder,
}

impl HttpTransaction {
    pub fn start_server() {
        Process::spawn_link((), |(), _: Mailbox<()>| {
            let listener = TcpListener::bind("0.0.0.0:8080").unwrap();
            while let Ok((stream, _)) = listener.accept() {
                Process::spawn_link(stream, |stream: TcpStream, _: Mailbox<()>| {
                    let metrics = ProcessRef::<MetricsProcess>::lookup("metrics").unwrap();
                    let transaction = Self::parse_request(stream);
                    let body = metrics.request(Gather);
                    let res = transaction
                        .response
                        .version(transaction.request.version())
                        .header("Content-Length", body.len())
                        .status(200)
                        .body::<Vec<u8>>(body)
                        .unwrap();
                    match HttpTransaction::write_response(transaction.stream, res) {
                        Ok(_) => println!("[http reader] SENT Response 200"),
                        Err(e) => eprintln!("[http reader] FAILED to send response {:?}", e),
                    }
                });
            }
        });
    }

    fn write_response(mut stream: TcpStream, response: Response<Vec<u8>>) -> IoResult<()> {
        // writing status line
        write!(
            &mut stream,
            "HTTP/{:?} {} {}\r\n",
            response.version(),
            response.status().as_u16(),
            response.status().canonical_reason().unwrap()
        )?;
        // writing headers
        for (key, value) in response.headers().iter() {
            stream.write_all(key.as_ref())?;
            write!(&mut stream, ": ")?;
            stream.write_all(value.as_ref())?;
            write!(&mut stream, "\r\n")?;
        }
        // separator between header and data
        write!(&mut stream, "\r\n")?;
        stream.write_all(response.body())?;
        Ok(())
    }

    pub fn parse_request(mut stream: TcpStream) -> HttpTransaction {
        let mut headers = [httparse::EMPTY_HEADER; 16];
        let mut buf = [0; 200];
        if let Err(e) = stream.read(&mut buf) {
            panic!("[http reader] Failed to read from tcp stream {:?}", e);
        }
        let mut req = httparse::Request::new(&mut headers);
        let offset = req.parse(&buf).unwrap();
        if let (true, None) = (offset.is_partial(), req.path) {
            panic!("[http reader] Failed to read request");
        }

        let mut request_builder = Request::builder()
            .method(req.method.unwrap())
            .uri(req.path.unwrap());
        println!("[http reader] GOT THESE HEADERS {:?}", req);
        for h in req.headers {
            if h.name.is_empty() {
                break;
            }
            request_builder = request_builder.header(h.name, h.value);
        }
        // get body
        let mut body: Vec<u8> = vec![];
        if let httparse::Status::Complete(idx) = offset {
            body = buf[idx..].to_owned();
        }

        let request = request_builder.body(body).unwrap();

        HttpTransaction {
            stream: stream.clone(),
            response: Response::builder().version(request.version()),
            request,
        }
    }
}
