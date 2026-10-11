//! Sampling is identity-bound, complete and independent of stored validity hints.

use super::*;

#[test]
fn unbound_selection_has_no_arbitrary_column_fallback() {
    assert_eq!(SamplingSelection::default().columns(), Err(RetrievalError::Missing));
}

#[test]
fn eight_distinct_samples_cover_native_custody_vectors() {
    let mut one = [0; 32];
    one[31] = 1;
    let mut prefix = [0; 32];
    prefix[..2].copy_from_slice(&[0xe3, 0x43]);
    for (identity, custody) in [
        ([0; 32], [1, 17, 87, 102]),
        (one, [1, 17, 75, 87]),
        ([255; 32], [1, 47, 87, 102]),
        (prefix, [21, 57, 68, 83]),
    ] {
        let selection = SamplingSelection::default();
        selection.bind(identity).unwrap();
        let columns = selection.columns().unwrap();
        assert_eq!(columns.len(), 8);
        assert!(columns.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(columns.iter().all(|column| *column < 128));
        assert!(custody.iter().all(|column| columns.contains(column)));
        assert_eq!(selection.columns().unwrap(), columns);
    }
}

#[test]
fn complete_node_identity_is_reversed_for_native_das_selection() {
    let mut identity = [0; 32];
    identity[31] = 1;
    let selection = SamplingSelection::default();
    selection.bind(identity).unwrap();
    let columns = selection.columns().unwrap();
    let mut native_integer = [0; 32];
    native_integer[0] = 1;
    assert_eq!(
        columns,
        CustodyParameters::default().get_custody_groups(native_integer, 8).unwrap()
    );
    assert_ne!(columns, CustodyParameters::default().get_custody_groups(identity, 8).unwrap());
}

#[test]
fn foreign_rebinding_cannot_replace_cached_sampling_columns() {
    let selection = SamplingSelection::default();
    selection.bind([0; 32]).unwrap();
    let columns = selection.columns().unwrap();
    selection.bind([0; 32]).unwrap();
    assert_eq!(selection.bind([1; 32]), Err(RetrievalError::Association));
    assert_eq!(selection.columns().unwrap(), columns);
}
