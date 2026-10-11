//! Published networking fixtures call production admission and custody helpers.

use super::{config_override, read_bounded, Case, Outcome};
use reth_embedded_beacon_network::custody::{CustodyError, CustodyParameters};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "networking" &&
        matches!(case.fork.as_str(), "fulu" | "gloas") &&
        matches!(
            case.handler.as_str(),
            "get_custody_groups" | "compute_columns_for_custody_group"
        )
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    custody_case(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn custody_case(case: &Case, max_bytes: usize) -> Result<(), String> {
    let parameters = CustodyParameters {
        number_of_groups: config_override(case, "NUMBER_OF_CUSTODY_GROUPS", 128, max_bytes)?,
        number_of_columns: config_override(case, "NUMBER_OF_COLUMNS", 128, max_bytes)?,
    };
    let (metadata, node_id) = metadata(case, max_bytes)?;
    let actual = match case.handler.as_str() {
        "get_custody_groups" => parameters.get_custody_groups(
            node_id.ok_or("custody fixture has no node identity")?,
            unsigned(&metadata["custody_group_count"])?,
        ),
        "compute_columns_for_custody_group" => {
            parameters.compute_columns_for_custody_group(unsigned(&metadata["custody_group"])?)
        }
        _ => return Err("unknown custody fixture handler".into()),
    };
    if metadata["result"].is_null() {
        return match actual {
            Err(CustodyError::Group) => Ok(()),
            result => Err(format!("expected custody bounds rejection, received {result:?}")),
        };
    }
    let expected = metadata["result"]
        .as_sequence()
        .ok_or("custody result is not a sequence")?
        .iter()
        .map(unsigned)
        .collect::<Result<Vec<_>, _>>()?;
    let actual = actual.map_err(|error| error.to_string())?;
    if actual != expected {
        return Err(format!(
            "custody selection differs: expected {expected:?}, received {actual:?}"
        ));
    }
    Ok(())
}

fn unsigned(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| "invalid unsigned custody metadata".into())
}

fn metadata(case: &Case, max_bytes: usize) -> Result<(Value, Option<[u8; 32]>), String> {
    let bytes = read_bounded(&case.path.join("meta.yaml"), max_bytes)?;
    let text = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
    let mut lines = text.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut identity = None;
    if let Some(index) = lines.iter().position(|line| line.trim_start().starts_with("node_id:")) {
        let scalar = lines[index]
            .trim()
            .strip_prefix("node_id:")
            .ok_or("node identity field")?
            .trim()
            .to_owned();
        let (scalar, continuation) = if scalar.is_empty() {
            (lines.get(index + 1).ok_or("missing node identity")?.trim().to_owned(), true)
        } else {
            (scalar, false)
        };
        identity = Some(parse_uint256(scalar.trim_matches(['\'', '"']))?);
        // Preserve a large YAML integer lexically instead of passing it through floating point.
        lines[index] = format!("node_id: '{}'", scalar.trim_matches(['\'', '"']));
        if continuation {
            lines.remove(index + 1);
        }
    }
    Ok((serde_yaml::from_str(&lines.join("\n")).map_err(|error| error.to_string())?, identity))
}

fn parse_uint256(text: &str) -> Result<[u8; 32], String> {
    let mut bytes = [0u8; 32];
    if let Some(hex) = text.strip_prefix("0x") {
        if hex.is_empty() || hex.len() > 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("invalid hexadecimal node identity".into());
        }
        for (index, digit) in hex.bytes().rev().enumerate() {
            let digit = match digit {
                b'0'..=b'9' => digit - b'0',
                b'a'..=b'f' => digit - b'a' + 10,
                _ => digit - b'A' + 10,
            };
            bytes[index / 2] |= digit << (index % 2 * 4);
        }
        return Ok(bytes);
    }
    if text.is_empty() || text.len() > 78 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid decimal node identity".into());
    }
    for digit in text.bytes() {
        let mut carry = u16::from(digit - b'0');
        for byte in &mut bytes {
            let value = u16::from(*byte) * 10 + carry;
            *byte = value as u8;
            carry = value >> 8;
        }
        if carry != 0 {
            return Err("node identity exceeds uint256".into());
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_identity_preserves_all_256_bits_and_rejects_overflow() {
        let maximum =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        assert_eq!(parse_uint256(maximum).unwrap(), [255; 32]);
        assert_eq!(
            parse_uint256(
                "115792089237316195423570985008687907853269984665640564039457584007913129639936"
            ),
            Err("node identity exceeds uint256".into())
        );
        assert_eq!(parse_uint256("0x100").unwrap()[..2], [0, 1]);
        assert_eq!(parse_uint256("-1"), Err("invalid decimal node identity".into()));
    }
}
