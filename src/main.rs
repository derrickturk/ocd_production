use std::{
    collections::HashMap,
    env,
    error::Error,
    fs::File,
    io::BufReader,
    str,
};

use zip::ZipArchive;

use encoding_rs_io::DecodeReaderBytes;

use quick_xml::{
    events::Event,
    Reader,
};

const BUF_SIZE: usize = 4096; // 4kb at once

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct WellAPI {
    pub state: u8,
    pub county: u16,
    pub well: u32,
}

#[derive(Copy, Clone, Debug)]
enum Phase {
    Oil,
    Gas,
    Water,
}

// TODO: dates
#[derive(Clone, Debug)]
struct WellProduction {
    pub oil: Vec<f64>,
    pub gas: Vec<f64>,
    pub water: Vec<f64>,
}

#[derive(Copy, Clone, Debug)]
enum ParserState {
    Between,
    Production,
    ReadAPIState,
    ReadAPICounty,
    ReadAPIWell,
    ReadPhase,
    ReadVolume,
}

struct WellProductionParser {
    state: ParserState,
    phase: Phase,
    production: HashMap<WellAPI, WellProduction>,
    current_api: WellAPI,
    current_record: WellProduction,
}

impl WellProductionParser {
    pub fn new() -> Self {
        WellProductionParser {
            state: ParserState::Between,
            phase: Phase::Oil,
            production: HashMap::new(),
            current_api: WellAPI { state: 0, county: 0, well: 0 },
            current_record: WellProduction {
                oil: Vec::new(),
                gas: Vec::new(),
                water: Vec::new(),
            },
        }
    }

    pub fn finish(mut self) -> HashMap<WellAPI, WellProduction> {
        self.production.insert(self.current_api, self.current_record);
        self.production
    }

    pub fn process(&mut self, ev: Event) {
        self.state = match self.state {
            ParserState::Between => {
                match ev {
                    Event::Start(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Production,
                    _ => ParserState::Between,
                }
            },
            
            st => st,
        };
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("no filename provided")?;
    let zipfile = File::open(path)?;
    let mut zip = ZipArchive::new(zipfile)?;

    if zip.len() != 1 {
        Err("expected one file in zip archive")?;
    }

    let xmlfile = zip.by_index(0)?;
    println!("file is {}, size {} bytes", xmlfile.name(), xmlfile.size());
    let xmlfile = BufReader::new(DecodeReaderBytes::new(xmlfile));
    let mut xmlfile = Reader::from_reader(xmlfile);

    let mut prodparser = WellProductionParser::new();
    let mut buf = Vec::with_capacity(BUF_SIZE);
    loop {
        match xmlfile.read_event(&mut buf)? {
            Event::Eof => break,
            ev => prodparser.process(ev),
        };
        buf.clear();
    }

    let prod = prodparser.finish();
    dbg!(prod);

    Ok(())
}
