use std::{
    fs::{self, File},
    io,
    io::{ErrorKind, Read, Write},
    path::Path,
};

use crate::components::configs::Configs;

pub struct FileUtils<'a> {
    conf: &'a Configs,
}

impl<'a> FileUtils<'a> {
    pub fn new(conf: &'a Configs) -> Result<FileUtils<'a>, io::Error> {
        match fs::create_dir_all(Path::new(&conf.args.dir_data)) {
            Ok(_) => Ok(FileUtils { conf }),
            Err(err) => Err(err),
        }
    }

    pub fn save_file(&self, filename: &String, bytes: &Vec<u8>) -> Result<(), io::Error> {
        let path = Path::new(&self.conf.args.dir_data).join(filename);

        if path.exists() {
            log::warn!("Path existed. Write new file: {}", filename);
        }

        let mut file = match File::create(path) {
            Ok(file) => file,
            Err(err) => {
                log::error!("Cannot create new file: {}: Err: {}", filename, err);
                return Err(err);
            }
        };

        file.write_all(&bytes.as_slice())
    }

    pub fn read_file(&self, filename: &String) -> Result<Vec<u8>, ErrorKind> {
        let path = Path::new(&self.conf.args.dir_data).join(filename);

        if !path.exists() {
            return Err(ErrorKind::NotFound);
        };

        let mut buff = Vec::<u8>::new();
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(err) => {
                log::error!("Cannot create new file: {}: Err: {}", filename, err);
                return Err(ErrorKind::InvalidData);
            }
        };
        if let Err(err) = file.read_to_end(&mut buff) {
            log::error!("Cannot create new file: {}: Err: {}", filename, err);
            return Err(ErrorKind::InvalidData);
        };

        Ok(buff)
    }
}
