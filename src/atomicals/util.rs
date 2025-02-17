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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::hex::{FromHex, ToHex};
    use bitcoin::script::Builder;

    #[test]
    fn test_is_atomicals_script() {
        // 创建一个普通的脚本
        let script = Builder::new()
            .push_slice(&[1, 2, 3])
            .into_script();
        
        // 目前应该返回 false，因为功能尚未实现
        assert!(!is_atomicals_script(&script));

        // TODO: 一旦实现了 is_atomicals_script，添加更多测试用例
        // 1. 测试有效的 Atomicals 脚本
        // 2. 测试无效的 Atomicals 脚本
        // 3. 测试边界情况
    }

    #[test]
    fn test_extract_atomicals_data() {
        // 创建一个普通的脚本
        let script = Builder::new()
            .push_slice(&[1, 2, 3])
            .into_script();
        
        // 目前应该返回 None，因为功能尚未实现
        assert!(extract_atomicals_data(&script).is_none());

        // TODO: 一旦实现了 extract_atomicals_data，添加更多测试用例
        // 1. 测试有效的 Atomicals 数据提取
        // 2. 测试无效的 Atomicals 数据
        // 3. 测试空数据
        // 4. 测试大数据
    }

    #[test]
    fn test_location_to_atomicals_id() {
        // 测试用例 1：全零的 txid 和 vout
        let txid = vec![0; 32];
        let vout = 0;
        let id = location_to_atomicals_id(&txid, vout);
        assert_eq!(id.len(), 36); // 32 bytes txid + 4 bytes vout
        assert_eq!(&id[..32], &txid[..]);
        assert_eq!(&id[32..], &vout.to_le_bytes());

        // 测试用例 2：特定的 txid 和 vout
        let txid = Vec::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef").unwrap();
        let vout = 1;
        let id = location_to_atomicals_id(&txid, vout);
        assert_eq!(id.len(), 36);
        assert_eq!(&id[..32], &txid[..]);
        assert_eq!(&id[32..], &vout.to_le_bytes());

        // 测试用例 3：最大 vout 值
        let vout = u32::MAX;
        let id = location_to_atomicals_id(&txid, vout);
        assert_eq!(id.len(), 36);
        assert_eq!(&id[..32], &txid[..]);
        assert_eq!(&id[32..], &vout.to_le_bytes());

        // 测试用例 4：验证输出格式的一致性
        let id1 = location_to_atomicals_id(&txid, 1);
        let id2 = location_to_atomicals_id(&txid, 1);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_hash_data() {
        // 测试用例 1：空数据
        let empty_data = b"";
        let empty_hash = hash_data(empty_data);
        assert_eq!(empty_hash.len(), 32);
        let expected_empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(empty_hash.to_hex(), expected_empty_hash);

        // 测试用例 2：简单字符串
        let data = b"Hello, Atomicals!";
        let hash = hash_data(data);
        assert_eq!(hash.len(), 32);
        // 验证哈希值的一致性
        let hash2 = hash_data(data);
        assert_eq!(hash, hash2);

        // 测试用例 3：二进制数据
        let binary_data = vec![0, 1, 2, 3, 4, 5];
        let binary_hash = hash_data(&binary_data);
        assert_eq!(binary_hash.len(), 32);

        // 测试用例 4：大数据
        let large_data = vec![0xFF; 1000];
        let large_hash = hash_data(&large_data);
        assert_eq!(large_hash.len(), 32);

        // 测试用例 5：验证不同数据产生不同的哈希
        let data1 = b"Hello";
        let data2 = b"World";
        let hash1 = hash_data(data1);
        let hash2 = hash_data(data2);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_data_properties() {
        // 测试哈希函数的基本属性

        // 1. 确定性：相同输入应产生相同输出
        let data = b"Test data";
        let hash1 = hash_data(data);
        let hash2 = hash_data(data);
        assert_eq!(hash1, hash2);

        // 2. 雪崩效应：小的输入改变应导致完全不同的哈希
        let data1 = b"Test data 1";
        let data2 = b"Test data 2";
        let hash1 = hash_data(data1);
        let hash2 = hash_data(data2);
        assert_ne!(hash1, hash2);
        
        // 计算有多少位不同
        let diff_bits = hash1.iter()
            .zip(hash2.iter())
            .map(|(a, b)| (a ^ b).count_ones())
            .sum::<u32>();
        // 对于好的哈希函数，平均应有一半位数不同
        assert!(diff_bits > 50); // 256位中至少50位不同

        // 3. 固定长度输出：无论输入长度如何，输出始终为32字节
        let inputs = vec![
            vec![],
            vec![1],
            vec![1, 2, 3],
            vec![1; 100],
            vec![1; 1000],
        ];

        for input in inputs {
            let hash = hash_data(&input);
            assert_eq!(hash.len(), 32);
        }
    }
}
