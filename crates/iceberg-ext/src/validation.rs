use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub fn validate_unique_vec<T>(items: &Vec<T>) -> Result<(), iceberg::Error>
where
    T: Eq + Hash + Debug,
{
    let set: HashSet<_> = items.iter().collect();
    if set.len() != items.len() {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "Duplicate Values".to_string(),
        ));
    }

    Ok(())
}
