//! Bounded field-fragment trees. Exact offsets are reconstructed from the typed SSZ layout.

use super::{CompactStateError, StateRecord, StoredPreset};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_shared_data::{
    CommittedObject, CommittedPinnedObject, Encoding, ObjectId, ObjectKind, Owner, SharedDataStore,
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

pub(super) const LEAF_BYTES: usize = 65_536;
const FANOUT: usize = 32;
const MAX_HEIGHT: u8 = 4;
const MAX_FIELDS: usize = 64;
const BRANCH_MAGIC: &[u8; 8] = b"EBSBRN01";
const ROOT_MAGIC: &[u8; 8] = b"EBSROOT1";
const MAX_BRANCH_BYTES: usize = 18 + FANOUT * 41;
const MAX_ROOT_BYTES: usize = 92 + MAX_FIELDS * 41;

#[derive(Debug, Clone, Copy)]
struct Link {
    id: ObjectId,
    size: usize,
}

#[derive(Debug)]
struct Staged<'a> {
    object: CommittedObject<'a>,
    owners: Vec<Owner>,
}

#[derive(Debug)]
pub(super) struct Publication<'a> {
    pub(super) root: ObjectId,
    objects: BTreeMap<ObjectId, Staged<'a>>,
}

impl<'a> Publication<'a> {
    pub(super) fn build(
        fields: &[&'a [u8]],
        record: &StateRecord,
    ) -> Result<Self, CompactStateError> {
        if fields.is_empty() || fields.len() > MAX_FIELDS {
            return Err(CompactStateError::InvalidDescriptor)
        }
        let mut result = Self { root: record.manifest, objects: BTreeMap::new() };
        let mut links = Vec::with_capacity(fields.len());
        for field in fields {
            let mut level = if field.is_empty() {
                vec![result.insert(ObjectKind::BeaconStateComponent, Cow::Borrowed(field))]
            } else {
                field
                    .chunks(LEAF_BYTES)
                    .map(|chunk| {
                        result.insert(ObjectKind::BeaconStateComponent, Cow::Borrowed(chunk))
                    })
                    .collect()
            };
            let mut height = 0u8;
            while level.len() > 1 {
                height = height.checked_add(1).ok_or(CompactStateError::InvalidDescriptor)?;
                if height > MAX_HEIGHT {
                    return Err(CompactStateError::InvalidDescriptor)
                }
                let mut next = Vec::with_capacity(level.len().div_ceil(FANOUT));
                for children in level.chunks(FANOUT) {
                    let total = children.iter().try_fold(0usize, |n, child| {
                        n.checked_add(child.size).ok_or(CompactStateError::InvalidDescriptor)
                    })?;
                    let mut bytes = BRANCH_MAGIC.to_vec();
                    bytes.push(height);
                    bytes.push(children.len() as u8);
                    bytes.extend_from_slice(&(total as u64).to_le_bytes());
                    for child in children {
                        encode_link(&mut bytes, *child);
                    }
                    let parent = result.insert(ObjectKind::BeaconStateBranch, Cow::Owned(bytes));
                    result.add_edges(parent.id, children)?;
                    next.push(Link { id: parent.id, size: total });
                }
                level = next;
            }
            links.push(*level.first().ok_or(CompactStateError::InvalidDescriptor)?);
        }
        let mut bytes = ROOT_MAGIC.to_vec();
        bytes.push(record.fork as u8);
        bytes.push(record.preset.tag());
        bytes.extend_from_slice(&(links.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&record.slot.to_le_bytes());
        bytes.extend_from_slice(&record.genesis_root);
        bytes.extend_from_slice(&record.state_root);
        bytes.extend_from_slice(&record.ssz_bytes.to_le_bytes());
        for child in &links {
            encode_link(&mut bytes, *child);
        }
        let root = result.insert(ObjectKind::BeaconStateManifest, Cow::Owned(bytes));
        result.add_edges(root.id, &links)?;
        result
            .objects
            .get_mut(&root.id)
            .ok_or(CompactStateError::InvalidDescriptor)?
            .owners
            .push(Owner::CompactState(record.state_root));
        result.root = root.id;
        Ok(result)
    }

    fn insert(&mut self, kind: ObjectKind, bytes: Cow<'a, [u8]>) -> Link {
        let size = bytes.len();
        let object = CommittedObject::new(kind, Encoding::Raw, bytes);
        let id = object.id();
        self.objects.entry(id).or_insert_with(|| Staged { object, owners: Vec::new() });
        Link { id, size }
    }

    fn add_edges(&mut self, parent: ObjectId, children: &[Link]) -> Result<(), CompactStateError> {
        for child in children {
            let owners = &mut self
                .objects
                .get_mut(&child.id)
                .ok_or(CompactStateError::InvalidDescriptor)?
                .owners;
            let owner = Owner::CompactNode(parent);
            if !owners.contains(&owner) {
                owners.push(owner);
            }
        }
        Ok(())
    }

    pub(super) fn borrowed(&self) -> Vec<CommittedPinnedObject<'_>> {
        self.objects.values().map(|staged| staged.object.pinned(&staged.owners)).collect()
    }
}

fn encode_link(bytes: &mut Vec<u8>, link: Link) {
    bytes.push(link.id.kind as u8);
    bytes.extend_from_slice(&(link.size as u64).to_le_bytes());
    bytes.extend_from_slice(&link.id.digest);
}

pub(super) fn reconstruct(
    shared: &SharedDataStore,
    record: &StateRecord,
    layout: &[Option<usize>],
    max_state_bytes: usize,
) -> Result<Vec<u8>, CompactStateError> {
    let links = descriptor(shared, record, layout)?;
    let size =
        usize::try_from(record.ssz_bytes).map_err(|_| CompactStateError::InvalidDescriptor)?;
    if size > max_state_bytes {
        return Err(CompactStateError::Capacity)
    }
    let fixed = layout.iter().try_fold(0usize, |n, width| {
        n.checked_add(width.unwrap_or(4)).ok_or(CompactStateError::InvalidDescriptor)
    })?;
    if fixed > size {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let mut output = Vec::with_capacity(size);
    output.resize(fixed, 0);
    let mut cursor = 0usize;
    for (width, link) in layout.iter().zip(links) {
        let range = if let Some(width) = width {
            let end = cursor.checked_add(*width).ok_or(CompactStateError::InvalidDescriptor)?;
            let range = cursor..end;
            cursor = end;
            range
        } else {
            let start = output.len();
            let end = start.checked_add(link.size).ok_or(CompactStateError::InvalidDescriptor)?;
            if end > size {
                return Err(CompactStateError::InvalidDescriptor)
            }
            output[cursor..cursor + 4].copy_from_slice(
                &u32::try_from(start)
                    .map_err(|_| CompactStateError::InvalidDescriptor)?
                    .to_le_bytes(),
            );
            cursor += 4;
            output.resize(end, 0);
            start..end
        };
        read_tree(shared, link, &mut output[range], None, MAX_HEIGHT)?;
    }
    if cursor != fixed || output.len() != size {
        return Err(CompactStateError::InvalidDescriptor)
    }
    Ok(output)
}

fn descriptor(
    shared: &SharedDataStore,
    record: &StateRecord,
    layout: &[Option<usize>],
) -> Result<Vec<Link>, CompactStateError> {
    if record.manifest.kind != ObjectKind::BeaconStateManifest ||
        record.manifest.encoding != Encoding::Raw ||
        layout.is_empty() ||
        layout.len() > MAX_FIELDS
    {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let bytes = shared
        .get_bounded(record.manifest, MAX_ROOT_BYTES)?
        .ok_or(CompactStateError::MissingComponent(record.manifest))?;
    let mut reader = Reader::new(&bytes);
    if reader.take(8)? != ROOT_MAGIC ||
        decode_fork(reader.byte()?)? != record.fork ||
        StoredPreset::from_tag(reader.byte()?)? != record.preset ||
        usize::from(reader.short()?) != layout.len() ||
        reader.long()? != record.slot ||
        reader.root()? != record.genesis_root ||
        reader.root()? != record.state_root ||
        reader.long()? != record.ssz_bytes
    {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let mut total = 0usize;
    let mut links = Vec::with_capacity(layout.len());
    for width in layout {
        let link = reader.link()?;
        if width.is_some_and(|width| width != link.size) {
            return Err(CompactStateError::InvalidDescriptor)
        }
        total = total
            .checked_add(link.size)
            .and_then(|n| n.checked_add(if width.is_none() { 4 } else { 0 }))
            .ok_or(CompactStateError::InvalidDescriptor)?;
        links.push(link);
    }
    reader.finish()?;
    if total as u64 != record.ssz_bytes {
        return Err(CompactStateError::InvalidDescriptor)
    }
    require_edges(shared, record.manifest, &links)?;
    Ok(links)
}

fn branch(
    shared: &SharedDataStore,
    link: Link,
    expected_height: Option<u8>,
) -> Result<(u8, Vec<Link>), CompactStateError> {
    let bytes = shared
        .get_bounded(link.id, MAX_BRANCH_BYTES)?
        .ok_or(CompactStateError::MissingComponent(link.id))?;
    let mut reader = Reader::new(&bytes);
    if reader.take(8)? != BRANCH_MAGIC {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let height = reader.byte()?;
    let count = usize::from(reader.byte()?);
    if height == 0 ||
        height > MAX_HEIGHT ||
        expected_height.is_some_and(|expected| expected != height) ||
        count == 0 ||
        count > FANOUT ||
        reader.long()? != link.size as u64
    {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let mut children = Vec::with_capacity(count);
    let mut total = 0usize;
    for _ in 0..count {
        let child = reader.link()?;
        let expected = if height == 1 {
            ObjectKind::BeaconStateComponent
        } else {
            ObjectKind::BeaconStateBranch
        };
        if child.id.kind != expected || child.size == 0 {
            return Err(CompactStateError::InvalidDescriptor)
        }
        total = total.checked_add(child.size).ok_or(CompactStateError::InvalidDescriptor)?;
        children.push(child);
    }
    reader.finish()?;
    if total != link.size {
        return Err(CompactStateError::InvalidDescriptor)
    }
    require_edges(shared, link.id, &children)?;
    Ok((height, children))
}

fn read_tree(
    shared: &SharedDataStore,
    link: Link,
    output: &mut [u8],
    expected_height: Option<u8>,
    remaining: u8,
) -> Result<(), CompactStateError> {
    if output.len() != link.size || link.id.encoding != Encoding::Raw {
        return Err(CompactStateError::InvalidDescriptor)
    }
    if link.id.kind == ObjectKind::BeaconStateComponent {
        if expected_height.is_some_and(|height| height != 0) || link.size > LEAF_BYTES {
            return Err(CompactStateError::InvalidDescriptor)
        }
        let bytes = shared
            .get_bounded(link.id, LEAF_BYTES)?
            .ok_or(CompactStateError::MissingComponent(link.id))?;
        if bytes.len() != output.len() {
            return Err(CompactStateError::InvalidDescriptor)
        }
        output.copy_from_slice(&bytes);
        return Ok(())
    }
    if link.id.kind != ObjectKind::BeaconStateBranch || remaining == 0 {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let (height, children) = branch(shared, link, expected_height)?;
    let mut cursor = 0usize;
    for child in children {
        let end = cursor.checked_add(child.size).ok_or(CompactStateError::InvalidDescriptor)?;
        read_tree(shared, child, &mut output[cursor..end], Some(height - 1), remaining - 1)?;
        cursor = end;
    }
    Ok(())
}

fn require_edges(
    shared: &SharedDataStore,
    parent: ObjectId,
    children: &[Link],
) -> Result<(), CompactStateError> {
    let expected = children.iter().map(|child| child.id).collect::<BTreeSet<_>>();
    let actual =
        shared.owner_objects(Owner::CompactNode(parent)).into_iter().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(CompactStateError::InvalidDescriptor)
    }
    Ok(())
}

pub(super) fn live_nodes(
    shared: &SharedDataStore,
    records: &BTreeMap<Root, StateRecord>,
) -> Result<BTreeSet<ObjectId>, CompactStateError> {
    let mut live = BTreeSet::new();
    let mut validated = BTreeMap::new();
    for record in records.values() {
        let roots = shared.owner_objects(Owner::CompactState(record.state_root));
        if roots != vec![record.manifest] {
            return Err(CompactStateError::InvalidDescriptor)
        }
        live.insert(record.manifest);
        let layout =
            reth_embedded_beacon_state_transition::ssz::BeaconState::container_field_layout(
                record.fork,
                record.preset.to_preset(),
            );
        for link in descriptor(shared, record, &layout)? {
            walk_tree(shared, link, &mut live, &mut validated, None, MAX_HEIGHT)?;
        }
    }
    Ok(live)
}

fn walk_tree(
    shared: &SharedDataStore,
    link: Link,
    live: &mut BTreeSet<ObjectId>,
    validated: &mut BTreeMap<ObjectId, (usize, u8)>,
    expected_height: Option<u8>,
    remaining: u8,
) -> Result<(), CompactStateError> {
    if let Some((size, height)) = validated.get(&link.id) {
        if *size != link.size || expected_height.is_some_and(|expected| expected != *height) {
            return Err(CompactStateError::InvalidDescriptor)
        }
        return Ok(())
    }
    live.insert(link.id);
    if link.id.kind == ObjectKind::BeaconStateComponent {
        if link.size > LEAF_BYTES || expected_height.is_some_and(|height| height != 0) {
            return Err(CompactStateError::InvalidDescriptor)
        }
        let bytes = shared
            .get_bounded(link.id, LEAF_BYTES)?
            .ok_or(CompactStateError::MissingComponent(link.id))?;
        if bytes.len() != link.size {
            return Err(CompactStateError::InvalidDescriptor)
        }
        validated.insert(link.id, (link.size, 0));
        return Ok(())
    }
    if link.id.kind != ObjectKind::BeaconStateBranch || remaining == 0 {
        return Err(CompactStateError::InvalidDescriptor)
    }
    let (height, children) = branch(shared, link, expected_height)?;
    for child in children {
        walk_tree(shared, child, live, validated, Some(height - 1), remaining - 1)?;
    }
    validated.insert(link.id, (link.size, height));
    Ok(())
}

const fn decode_fork(tag: u8) -> Result<Fork, CompactStateError> {
    match tag {
        0 => Ok(Fork::Phase0),
        1 => Ok(Fork::Altair),
        2 => Ok(Fork::Bellatrix),
        3 => Ok(Fork::Capella),
        4 => Ok(Fork::Deneb),
        5 => Ok(Fork::Electra),
        6 => Ok(Fork::Fulu),
        7 => Ok(Fork::Gloas),
        _ => Err(CompactStateError::InvalidDescriptor),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}
impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }
    fn take(&mut self, size: usize) -> Result<&'a [u8], CompactStateError> {
        let end = self.cursor.checked_add(size).ok_or(CompactStateError::InvalidDescriptor)?;
        let result =
            self.bytes.get(self.cursor..end).ok_or(CompactStateError::InvalidDescriptor)?;
        self.cursor = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, CompactStateError> {
        Ok(self.take(1)?[0])
    }
    fn short(&mut self) -> Result<u16, CompactStateError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().map_err(|_| CompactStateError::InvalidDescriptor)?,
        ))
    }
    fn long(&mut self) -> Result<u64, CompactStateError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| CompactStateError::InvalidDescriptor)?,
        ))
    }
    fn root(&mut self) -> Result<Root, CompactStateError> {
        self.take(32)?.try_into().map_err(|_| CompactStateError::InvalidDescriptor)
    }
    fn link(&mut self) -> Result<Link, CompactStateError> {
        let kind = match self.byte()? {
            8 => ObjectKind::BeaconStateComponent,
            9 => ObjectKind::BeaconStateBranch,
            _ => return Err(CompactStateError::InvalidDescriptor),
        };
        let size =
            usize::try_from(self.long()?).map_err(|_| CompactStateError::InvalidDescriptor)?;
        Ok(Link { id: ObjectId { kind, encoding: Encoding::Raw, digest: self.root()? }, size })
    }
    const fn finish(self) -> Result<(), CompactStateError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(CompactStateError::InvalidDescriptor)
        }
    }
}
