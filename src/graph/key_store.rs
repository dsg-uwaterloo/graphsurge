use abomonation_derive::Abomonation;
use hashbrown::HashMap;
use std::convert::TryFrom;

pub const ID_STRING: &str = "id";
pub const STAR_STRING: &str = "*"; // For count(*).

pub const ID_KEY_ID: KeyId = KeyId(0);
pub const STAR_KEY_ID: KeyId = KeyId(1);

/// Stores a mapping from `String`s to `KeyId`s with integer type `Id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyStore {
    key_strings: Vec<String>,
    key_string_to_id: HashMap<String, KeyId>,
}

impl Default for KeyStore {
    fn default() -> Self {
        let mut new_keystore =
            KeyStore { key_strings: Vec::new(), key_string_to_id: HashMap::new() };
        new_keystore.reset(); // Reuse `reset()` logic.
        new_keystore
    }
}

impl KeyStore {
    pub fn reset(&mut self) {
        self.key_strings.clear();
        self.key_string_to_id.clear();
        // The order is important and must be the same as the defined `const`s.
        let key_id = self.get_key_id_or_insert(ID_STRING);
        assert_eq!(key_id, ID_KEY_ID);
        let key_id = self.get_key_id_or_insert(STAR_STRING);
        assert_eq!(key_id, STAR_KEY_ID);
    }

    pub fn is_defined_constant(key_id: KeyId) -> bool {
        [ID_KEY_ID, STAR_KEY_ID].contains(&key_id)
    }

    pub fn get_key_id_or_insert(&mut self, type_string: &str) -> KeyId {
        if let Some(&id) = self.key_string_to_id.get(type_string) {
            id
        } else {
            self.key_strings.push(type_string.to_owned());
            let id = KeyIdType::try_from(self.key_strings.len() - 1).expect("Ran out of key ids");
            let key_id = KeyId(id);
            self.key_string_to_id.insert(type_string.to_owned(), key_id);
            key_id
        }
    }

    pub fn get_key_id(&self, type_string: &str) -> Option<KeyId> {
        self.key_string_to_id.get(type_string).copied()
    }

    pub fn key_string(&self, key: KeyId) -> &String {
        // Guaranteed to exist because no one outside the module can construct `KeyId`s.
        &self.key_strings[usize::from(key.0)]
    }
}

#[derive(
    Default,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Abomonation,
    Serialize,
    Deserialize,
)]
pub struct KeyId(KeyIdType);
pub type KeyIdType = u16;

impl KeyId {
    pub fn to_usize(self) -> usize {
        usize::from(self.0)
    }
}

impl std::fmt::Display for KeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}
