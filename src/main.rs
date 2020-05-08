use std::{
    collections::HashMap,
    env,
    error::Error,
    fs::File,
    io::BufReader,
    mem,
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

impl WellAPI {
    pub fn new() -> Self {
        WellAPI {
            state: 0,
            county: 0,
            well: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state == 0 && self.county == 0 && self.well == 0
    }
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

impl WellProduction {
    pub fn new() -> Self {
        WellProduction {
            oil: Vec::new(),
            gas: Vec::new(),
            water: Vec::new(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum ParserState {
    Between,
    ProductionNeedAPI,
    ReadAPIState,
    ReadAPICounty,
    ReadAPIWell,
    ProductionHaveAPI,
    ReadPhase,
    ReadVolume,
}

struct WellProductionParser {
    state: ParserState,
    phase: Phase,
    production: HashMap<WellAPI, WellProduction>,
    last_api: WellAPI,
    current_api: WellAPI,
    current_record: WellProduction,
}

impl WellProductionParser {
    pub fn new() -> Self {
        WellProductionParser {
            state: ParserState::Between,
            phase: Phase::Oil,
            production: HashMap::new(),
            last_api: WellAPI::new(),
            current_api: WellAPI::new(),
            current_record: WellProduction::new(),
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
                        ParserState::ProductionNeedAPI,
                    _ => ParserState::Between,
                }
            },

            ParserState::ProductionNeedAPI => {
                match ev {
                    Event::Start(e) => match e.local_name() {
                        b"api_st_cde" => ParserState::ReadAPIState,
                        b"api_cnty_cde" => ParserState::ReadAPICounty,
                        b"api_well_idn" => ParserState::ReadAPIWell,
                        _ => ParserState::ProductionNeedAPI,
                    },

                    _ => ParserState::ProductionNeedAPI,
                }
            },

            ParserState::ProductionHaveAPI => {
                match ev {
                    Event::Start(e) => match e.local_name() {
                        b"prd_knd_cde" => ParserState::ReadPhase,
                        b"prod_amt" => ParserState::ReadVolume,
                        _ => ParserState::ProductionHaveAPI,
                    },

                    Event::End(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Between,

                    _ => ParserState::ProductionHaveAPI,
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
                        ParserState::ProductionNeedAPI,

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
                        ParserState::ProductionNeedAPI,

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

                    Event::End(e) if e.local_name() == b"api_well_idn" => {
                        self.see_api();
                        ParserState::ProductionHaveAPI
                    },

                    _ => ParserState::ReadAPIWell,
                }
            },

            ParserState::ReadPhase => {
                match ev {
                    Event::Text(e) => {
                        self.phase = match e.escaped().first() {
                            Some(b'O') => Phase::Oil,
                            Some(b'G') => Phase::Gas,
                            Some(b'W') => Phase::Water,
                            _ => Err("invalid phase")?,
                        };
                        ParserState::ReadPhase
                    },

                    Event::End(e) if e.local_name() == b"prd_knd_cde" =>
                        ParserState::ProductionHaveAPI,

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
                        ParserState::ProductionHaveAPI,

                    _ => ParserState::ReadVolume,
                }
            },
        };

        Ok(())
    }

    fn see_api(&mut self) {
        if !self.last_api.is_empty() && self.current_api != self.last_api {
            let mut record = WellProduction::new();
            mem::swap(&mut record, &mut self.current_record);
            self.production.insert(self.last_api, record);
        }

        self.last_api = self.current_api;
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
    let xmlfile = BufReader::new(DecodeReaderBytes::new(xmlfile));
    let mut xmlfile = Reader::from_reader(xmlfile);

    let mut prodparser = WellProductionParser::new();
    let mut buf = Vec::with_capacity(BUF_SIZE);
    let mut i = 0;
    loop {
        match xmlfile.read_event(&mut buf)? {
            Event::Eof => break,
            ev => prodparser.process(ev)?,
        };
        buf.clear();

        i += 1;
        if i > 25000 {
            break;
        }
    }

    let prod = prodparser.finish();
    dbg!(prod);

    Ok(())
}
