use std::{
    collections::HashMap,
    env,
    error::Error,
    fmt::{self, Display},
    fs::File,
    io::{self, BufReader, Write},
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

impl Display for WellAPI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}{:03}{:05}", self.state, self.county, self.well)
    }
}

impl WellAPI {
    pub fn new() -> Self {
        WellAPI { state: 0, county: 0, well: 0, }
    }
}

#[derive(Copy, Clone, Debug)]
enum Phase {
    Oil,
    Gas,
    Water,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct Date {
    year: u16,
    month: u8,
}

impl Date {
    pub fn new() -> Self {
        Date { year: 0, month: 0 }
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

#[derive(Clone, Debug)]
struct WellProduction {
    pub oil: Option<f64>,
    pub gas: Option<f64>,
    pub water: Option<f64>,
}

impl WellProduction {
    pub fn new() -> Self {
        WellProduction { oil: None, gas: None, water: None, }
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
    ProductionSkip,
    ReadMonth,
    ReadYear,
    ReadPhase,
    ReadVolume,
}

struct WellProductionParser<'a> {
    state: ParserState,
    phase: Phase,
    production: HashMap<WellAPI, HashMap<Date, WellProduction>>,
    current_api: WellAPI,
    current_date: Date,
    api_predicate: Option<&'a dyn Fn(WellAPI) -> bool>,
}

impl<'a> WellProductionParser<'a> {
    pub fn new() -> Self {
        WellProductionParser {
            state: ParserState::Between,
            phase: Phase::Oil,
            production: HashMap::new(),
            current_api: WellAPI::new(),
            current_date: Date::new(),
            api_predicate: None,
        }
    }

    pub fn with_predicate(p: &'a dyn Fn(WellAPI) -> bool) -> Self {
        let mut parser = WellProductionParser::new();
        parser.api_predicate = Some(p);
        parser
    }

    pub fn finish(self) -> HashMap<WellAPI, HashMap<Date, WellProduction>> {
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
                        b"prodn_mth" => ParserState::ReadMonth,
                        b"prodn_yr" => ParserState::ReadYear,
                        b"prd_knd_cde" => ParserState::ReadPhase,
                        b"prod_amt" => ParserState::ReadVolume,
                        _ => ParserState::ProductionHaveAPI,
                    },

                    Event::End(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Between,

                    _ => ParserState::ProductionHaveAPI,
                }
            },

            ParserState::ProductionSkip => {
                match ev {
                    Event::End(e) if e.local_name() == b"wcproduction" =>
                        ParserState::Between,

                    _ => ParserState::ProductionSkip,
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
                        match self.api_predicate {
                            Some(p) if !p(self.current_api) =>
                                ParserState::ProductionSkip,
                            _ => ParserState::ProductionHaveAPI,
                        }
                    }

                    _ => ParserState::ReadAPIWell,
                }
            },

            ParserState::ReadMonth => {
                match ev {
                    Event::Text(e) => {
                        self.current_date.month = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadMonth
                    },

                    Event::End(e) if e.local_name() == b"prodn_mth" =>
                        ParserState::ProductionHaveAPI,

                    _ => ParserState::ReadMonth,
                }
            },

            ParserState::ReadYear => {
                match ev {
                    Event::Text(e) => {
                        self.current_date.year = str::parse(
                            str::from_utf8(&e.unescaped()?)?
                        )?;
                        ParserState::ReadYear
                    },

                    Event::End(e) if e.local_name() == b"prodn_yr" =>
                        ParserState::ProductionHaveAPI,

                    _ => ParserState::ReadYear,
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

                        let mut rec = self.production.entry(self.current_api)
                            .or_insert_with(HashMap::new)
                            .entry(self.current_date)
                            .or_insert_with(WellProduction::new);

                        match self.phase {
                            Phase::Oil => rec.oil = Some(vol),
                            Phase::Gas => rec.gas = Some(vol),
                            Phase::Water => rec.water = Some(vol),
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
}

fn write_table(w: &mut impl Write,
  production: &HashMap<WellAPI, HashMap<Date, WellProduction>>
  ) -> io::Result<()> {
    write!(w, "api\tyear\tmonth\toil\tgas\twater\n")?;
    for (api, by_date) in production {
        for (date, vols) in by_date {
            write!(w, "{}\t{}\t{}", api, date.year, date.month)?;

            if let Some(oil) = vols.oil {
                write!(w, "\t{}", oil)?;
            } else {
                write!(w, "\t")?;
            }

            if let Some(gas) = vols.gas {
                write!(w, "\t{}", gas)?;
            } else {
                write!(w, "\t")?;
            }

            if let Some(water) = vols.water {
                write!(w, "\t{}\n", water)?;
            } else {
                write!(w, "\t\n")?;
            }
        }
    }
    Ok(())
}

const EDDY_COUNTY: u16 = 15;

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

    let mut prodparser = WellProductionParser::with_predicate(
        &|api: WellAPI| api.county == EDDY_COUNTY);
    let mut buf = Vec::with_capacity(BUF_SIZE);
    loop {
        match xmlfile.read_event(&mut buf)? {
            Event::Eof => break,
            ev => prodparser.process(ev)?,
        };
        buf.clear();
    }

    let prod = prodparser.finish();
    write_table(&mut io::stdout(), &prod)?;

    Ok(())
}
