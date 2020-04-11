use crate::graph::key_store::{KeyId, ID_KEY_ID, STAR_KEY_ID};
use crate::graph::properties::property_value::PropertyValue;
use abomonation_derive::Abomonation;

pub mod operations;
pub mod property_value;
pub mod property_value_type;

pub type StoreIndex = usize;
pub type PropertyKeyId = KeyId;

#[derive(Debug, Clone, Abomonation, Serialize, Deserialize)]
pub struct Properties {
    entries: Vec<(PropertyKeyId, PropertyValue)>,
}

impl Default for Properties {
    fn default() -> Self {
        Self { entries: Vec::new() }
    }
}

impl Properties {
    pub fn add_new_property(
        &mut self,
        property_key_id: PropertyKeyId,
        property_value: PropertyValue,
    ) {
        self.entries.push((property_key_id, property_value));
    }

    pub fn get_property<'a>(
        &'a self,
        id: &'a PropertyValue,
        property_key_id: PropertyKeyId,
    ) -> Option<&'a PropertyValue> {
        if property_key_id == ID_KEY_ID {
            return Some(id);
        }
        if property_key_id == STAR_KEY_ID {
            unreachable!("Vertex or Edge stream should not have star property operand");
        }
        for (key_id, property) in &self.entries {
            if *key_id == property_key_id {
                return Some(property);
            }
        }
        None
    }
}
