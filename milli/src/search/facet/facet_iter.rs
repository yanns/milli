use std::fmt::Debug;
use std::ops::Bound::{Included, Unbounded};

use either::Either::{self, Left, Right};
use heed::types::{DecodeIgnore, ByteSlice};
use heed::{Database, BytesEncode};
use log::debug;
use num_traits::Bounded;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::{Index, FieldId};

use super::{FacetRange, FacetRevRange};

pub struct FacetIter<'t, T: 't, KC> {
    rtxn: &'t heed::RoTxn<'t>,
    db: Database<KC, CboRoaringBitmapCodec>,
    field_id: FieldId,
    level_iters: Vec<(RoaringBitmap, Either<FacetRange<'t, T, KC>, FacetRevRange<'t, T, KC>>)>,
    must_reduce: bool,
}

impl<'t, T, KC> FacetIter<'t, T, KC>
where
    KC: heed::BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    /// Create a `FacetIter` that will iterate on the different facet entries
    /// (facet value + documents ids) and that will reduce the given documents ids
    /// while iterating on the different facet levels.
    pub fn new_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Left(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: true })
    }

    /// Create a `FacetIter` that will iterate on the different facet entries in reverse
    /// (facet value + documents ids) and that will reduce the given documents ids
    /// while iterating on the different facet levels.
    pub fn new_reverse_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRevRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Right(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: true })
    }

    /// Create a `FacetIter` that will iterate on the different facet entries
    /// (facet value + documents ids) and that will not reduce the given documents ids
    /// while iterating on the different facet levels, possibly returning multiple times
    /// a document id associated with multiple facet values.
    pub fn new_non_reducing(
        rtxn: &'t heed::RoTxn,
        index: &'t Index,
        field_id: FieldId,
        documents_ids: RoaringBitmap,
    ) -> heed::Result<FacetIter<'t, T, KC>>
    {
        let db = index.facet_field_id_value_docids.remap_key_type::<KC>();
        let highest_level = Self::highest_level(rtxn, db, field_id)?.unwrap_or(0);
        let highest_iter = FacetRange::new(rtxn, db, field_id, highest_level, Unbounded, Unbounded)?;
        let level_iters = vec![(documents_ids, Left(highest_iter))];
        Ok(FacetIter { rtxn, db, field_id, level_iters, must_reduce: false })
    }

    fn highest_level<X>(rtxn: &'t heed::RoTxn, db: Database<KC, X>, fid: FieldId) -> heed::Result<Option<u8>> {
        let level = db.remap_types::<ByteSlice, DecodeIgnore>()
            .prefix_iter(rtxn, &[fid][..])?
            .remap_key_type::<KC>()
            .last().transpose()?
            .map(|((_, level, _, _), _)| level);
        Ok(level)
    }
}

impl<'t, T: 't, KC> Iterator for FacetIter<'t, T, KC>
where
    KC: heed::BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    KC: for<'x> heed::BytesEncode<'x, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded + Debug,
{
    type Item = heed::Result<(T, RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let (documents_ids, last) = self.level_iters.last_mut()?;
            let is_ascending = last.is_left();
            for result in last {
                // If the last iterator must find an empty set of documents it means
                // that we found all the documents in the sub level iterations already,
                // we can pop this level iterator.
                if documents_ids.is_empty() {
                    break;
                }

                match result {
                    Ok(((_fid, level, left, right), mut docids)) => {

                        docids.intersect_with(&documents_ids);
                        if !docids.is_empty() {
                            if self.must_reduce {
                                documents_ids.difference_with(&docids);
                            }

                            if level == 0 {
                                debug!("found {:?} at {:?}",  docids, left);
                                return Some(Ok((left, docids)));
                            }

                            let rtxn = self.rtxn;
                            let db = self.db;
                            let fid = self.field_id;
                            let left = Included(left);
                            let right = Included(right);

                            debug!("calling with {:?} to {:?} (level {}) to find {:?}",
                                left, right, level - 1, docids,
                            );

                            let result = if is_ascending {
                                FacetRange::new(rtxn, db, fid, level - 1, left, right).map(Left)
                            } else {
                                FacetRevRange::new(rtxn, db, fid, level - 1, left, right).map(Right)
                            };

                            match result {
                                Ok(iter) => {
                                    self.level_iters.push((docids, iter));
                                    continue 'outer;
                                },
                                Err(e) => return Some(Err(e)),
                            }
                        }
                    },
                    Err(e) => return Some(Err(e)),
                }
            }
            self.level_iters.pop();
        }
    }
}
