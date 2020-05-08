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

    pub fn process(&mut self, ev: Event) -> Result<(), Box<dyn Error>> {
        self.state = match self.state {
            ParserState::Between => {
                match ev {
                    Event::Start(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Production,
                    _ => ParserState::Between,
                }
            },

            ParserState::Production => {
                match ev {
                    Event::Start(e) => match e.local_name() {
                        b"api_st_cde" => ParserState::ReadAPIState,
                        b"api_cnty_cde" => ParserState::ReadAPICounty,
                        b"api_well_idn" => ParserState::ReadAPIWell,
                        b"prd_knd_cde" => ParserState::ReadPhase,
                        b"prod_amt" => ParserState::ReadVolume,
                        _ => ParserState::Production,
                    },

                    Event::End(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Between,

                    _ => ParserState::Production,
                }
            },

            ParserState::ReadAPIState => {
                match ev {
                    Event::Text(e) => {
                        self.current_api.state = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadAPIState
                    },

                    Event::End(e) if e.local_name() == b"api_st_cde" =>
                        ParserState::Production,

                    _ => ParserState::ReadAPIState,
                }
            },

            ParserState::ReadAPICounty => {
                match ev {
                    Event::Text(e) => {
                        self.current_api.county = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadAPICounty
                    },

                    Event::End(e) if e.local_name() == b"api_cnty_cde" =>
                        ParserState::Production,

                    _ => ParserState::ReadAPICounty,
                }
            },

            ParserState::ReadAPIWell => {
                match ev {
                    Event::Text(e) => {
                        self.current_api.well = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadAPIWell
                    },

                    Event::End(e) if e.local_name() == b"api_well_idn" =>
                        ParserState::Production,

                    _ => ParserState::ReadAPIWell,
                }
            },

            ParserState::ReadPhase => {
                match ev {
                    Event::Text(e) => {
                        self.phase = match e.escaped() {
                            b"O" => Phase::Oil,
                            b"G" => Phase::Gas,
                            b"W" => Phase::Water,
                            _ => Err("invalid phase")?,
                        };
                        ParserState::ReadPhase
                    },

                    Event::End(e) if e.local_name() == b"prd_knd_cde" =>
                        ParserState::Production,

                    _ => ParserState::ReadPhase,
                }
            },

            ParserState::ReadVolume => {
                match ev {
                    Event::Text(e) => {
                        let vol = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;

                        match self.phase {
                            Phase::Oil => self.current_record.oil.push(vol),
                            Phase::Gas => self.current_record.gas.push(vol),
                            Phase::Water => self.current_record.water.push(vol),
                        };

                        ParserState::ReadVolume
                    },

                    Event::End(e) if e.local_name() == b"prod_amt" =>
                        ParserState::Production,

                    _ => ParserState::ReadVolume,
                }
            },
        };

        Ok(())
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
            ev => prodparser.process(ev)?,
        };
        buf.clear();
    }

    let prod = prodparser.finish();
    dbg!(prod);

    Ok(())
}
