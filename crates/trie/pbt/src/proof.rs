use crate::{
    hash::{branch_hash, leaf_hash},
    tree::{bit, matches_prefix, validate_key, Node},
    BinaryTree, TreeHasher, MAX_KEY_LENGTH,
};
use alloy_primitives::B256;

/// A single-key membership or non-membership proof.
///
/// Steps are ordered root to terminal. Prefixes are relative to the previous branch, exactly
/// as in node hashing. This is an internal proof representation, not an RPC wire format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proof {
    /// Branches traversed before reaching the terminal node.
    pub steps: Vec<ProofStep>,
    /// The leaf, divergent branch, or empty tree ending the search.
    pub terminal: ProofTerminal,
}

impl Proof {
    /// Verifies this proof against a trusted root and returns the proven value or absence.
    pub fn verify<H: TreeHasher>(
        &self,
        root: B256,
        key: &[u8],
    ) -> Result<Option<B256>, ProofError> {
        validate_key(key).map_err(|_| ProofError::InvalidPath)?;
        let mut depth = 0;
        for step in &self.steps {
            if !valid_prefix(depth, &step.prefix) || !matches_prefix(key, depth, &step.prefix) {
                return Err(ProofError::InvalidPath);
            }
            depth += step.prefix.len();
            if bit(key, depth) != Some(step.right) {
                return Err(ProofError::InvalidPath);
            }
            depth += 1;
        }
        let (mut hash, value) = match &self.terminal {
            ProofTerminal::Empty => {
                if !self.steps.is_empty() {
                    return Err(ProofError::InvalidPath);
                }
                (B256::ZERO, None)
            }
            ProofTerminal::Leaf { key: stored, value } => {
                validate_key(stored).map_err(|_| ProofError::InvalidPath)?;
                // A different terminal leaf must still inhabit the path the proof traversed.
                if stored.len() * 8 < depth || (0..depth).any(|i| bit(key, i) != bit(stored, i)) {
                    return Err(ProofError::InvalidPath);
                }
                (leaf_hash::<H>(stored, *value), (key == stored).then_some(*value))
            }
            ProofTerminal::Branch { prefix, left, right } => {
                if !valid_prefix(depth, prefix) ||
                    (matches_prefix(key, depth, prefix) &&
                        bit(key, depth + prefix.len()).is_some())
                {
                    return Err(ProofError::InvalidPath);
                }
                (branch_hash::<H>(prefix, *left, *right), None)
            }
        };
        for step in self.steps.iter().rev() {
            let (left, right) =
                if step.right { (step.sibling, hash) } else { (hash, step.sibling) };
            hash = branch_hash::<H>(&step.prefix, left, right);
        }
        if hash != root {
            return Err(ProofError::RootMismatch);
        }
        Ok(value)
    }
}

/// One authenticated branch on a proof path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProofStep {
    /// Compressed path before the branch selector bit.
    pub prefix: Vec<bool>,
    /// Whether the search selects the right child.
    pub right: bool,
    /// Commitment to the child not selected.
    pub sibling: B256,
}

/// A node sufficient to end a membership or non-membership search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProofTerminal {
    /// The entire tree is empty.
    Empty,
    /// A matching leaf proves presence; a different leaf proves absence.
    Leaf {
        /// The leaf's complete key.
        key: Vec<u8>,
        /// The leaf's value, which may be zero in the generic tree.
        value: B256,
    },
    /// The query diverges from, or ends within, this branch's compressed path.
    Branch {
        /// Relative compressed prefix.
        prefix: Vec<bool>,
        /// Left child commitment.
        left: B256,
        /// Right child commitment.
        right: B256,
    },
}

/// A malformed proof or a proof for a different root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProofError {
    /// The search path, prefix length, or terminal is invalid.
    #[error("invalid binary proof path")]
    InvalidPath,
    /// The proof does not authenticate to the supplied root.
    #[error("binary proof root mismatch")]
    RootMismatch,
}

impl<H: TreeHasher> BinaryTree<H> {
    /// Produces a proof for the exact key, including a proof of absence if it is not stored.
    pub fn proof(&self, key: &[u8]) -> Proof {
        let mut steps = Vec::new();
        let Some(mut node) = self.root.as_deref() else {
            return Proof { steps, terminal: ProofTerminal::Empty };
        };
        let mut depth = 0;
        let terminal = loop {
            match node {
                Node::Leaf { key, value, .. } => {
                    break ProofTerminal::Leaf { key: key.to_vec(), value: *value }
                }
                Node::Branch { prefix, left, right, .. } => {
                    let direction = bit(key, depth + prefix.len());
                    if !matches_prefix(key, depth, prefix) || direction.is_none() {
                        break ProofTerminal::Branch {
                            prefix: prefix.clone(),
                            left: left.hash(),
                            right: right.hash(),
                        };
                    }
                    let direction = direction.unwrap();
                    steps.push(ProofStep {
                        prefix: prefix.clone(),
                        right: direction,
                        sibling: if direction { left.hash() } else { right.hash() },
                    });
                    node = if direction { right } else { left };
                    depth += prefix.len() + 1;
                }
            }
        };
        Proof { steps, terminal }
    }
}

const fn valid_prefix(depth: usize, prefix: &[bool]) -> bool {
    prefix.len() <= u16::MAX as usize && depth + prefix.len() < MAX_KEY_LENGTH * 8
}
