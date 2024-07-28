use std::collections::HashMap;
use lazy_static::lazy_static;

#[derive(Debug)]
pub struct Collation {
    pub coll_name: String,
    pub charset_name: String,
    pub id: u32,
    pub is_default: bool,
}

lazy_static! {
    static ref COLLMAP: HashMap<u32, Collation> = {
        let mut map = HashMap::new();
        map.insert(
            32,
            Collation {
                coll_name: "armscii8_general_ci".into(),
                charset_name: "armscii8".into(),
                id: 32,
                is_default: true,
            },
        );
        map
    };
}

pub fn get_collation(id: u32) -> &'static Collation {
    COLLMAP.get(&id).unwrap()
}

#[cfg(test)]
mod meta_consts_tests {

    use std::env::set_var;
    use log::info;
    use crate::util;

    use super::*;

    fn setup() {
        set_var("RUST_LOG", "info");
        util::init();
    }

    #[test]
    fn test_get_collection() {
        setup();
        let coll = get_collation(32);
        assert!(coll.id > 0);
        info!("{:?}", coll);
    }
}
