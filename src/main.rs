use std::{
    env,
    error::Error,
    fs::File,
    io::Read,
    str,
};

use zip::ZipArchive;

const BUF_SIZE: usize = 4096; // 4kb at once

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("no filename provided")?;
    let zipfile = File::open(path)?;
    let mut zip = ZipArchive::new(zipfile)?;

    if zip.len() != 1 {
        Err("expected one file in zip archive")?;
    }

    let mut xmlfile = zip.by_index(0)?;
    println!("file is {}, size {} bytes", xmlfile.name(), xmlfile.size());

    let mut buf = [0u8; BUF_SIZE];
    loop {
        if xmlfile.read(&mut buf[..])? == 0 {
            break;
        }

        println!("read chunk: {:?}", &buf[..]);
        break;
    }

    Ok(())
}
