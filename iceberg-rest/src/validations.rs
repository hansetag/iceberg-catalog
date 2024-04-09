use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub fn validate_unique_vec<T>(items: &Vec<T>) -> Result<(), validator::ValidationError>
where
    T: Eq + Hash + Debug,
{
    let set: HashSet<_> = items.iter().collect();
    if set.len() != items.len() {
        return Err(validator::ValidationError::new("duplicate values"));
    }

    Ok(())
}
