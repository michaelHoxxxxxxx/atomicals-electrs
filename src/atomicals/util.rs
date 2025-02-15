use bitcoin::Script;
use bitcoin::hashes::{sha256, Hash};

/// Check if a script is an Atomicals protocol script
pub fn is_atomicals_script(_script: &Script) -> bool {
    // TODO: Implement Atomicals script detection
    false
}

/// Extract Atomicals data from a script
pub fn extract_atomicals_data(_script: &Script) -> Option<Vec<u8>> {
    // TODO: Implement data extraction
    None
}

/// Convert a location to an Atomicals ID
pub fn location_to_atomicals_id(txid: &[u8], vout: u32) -> Vec<u8> {
    let mut data = Vec::with_capacity(36);
    data.extend_from_slice(txid);
    data.extend_from_slice(&vout.to_le_bytes());
    data
}

/// Hash data using SHA256
pub fn hash_data(data: &[u8]) -> [u8; 32] {
    sha256::Hash::hash(data).to_byte_array()
}
