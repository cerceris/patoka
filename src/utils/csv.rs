use csv;
use std::{error::Error, fs::File};

pub fn load_from_file<T: serde::de::DeserializeOwned>(
    path: &str
) -> Result<Vec<T>, Box<dyn Error>> {
    let file = File::open(path).expect(
        &format!("Failed to open file {}", &path)
    );

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file);

    let mut items = Vec::new();

    for line in reader.deserialize() {
        let item: T = line?;
        items.push(item);
    }

    Ok(items)
}
