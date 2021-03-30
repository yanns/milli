use std::ops::Bound::{self, Included, Excluded, Unbounded};

use heed::{BytesEncode, BytesDecode};
use heed::{Database, RoRange, RoRevRange, LazyDecode};
use num_traits::Bounded;
use roaring::RoaringBitmap;

use crate::heed_codec::CboRoaringBitmapCodec;
use crate::FieldId;

pub struct FacetRange<'t, T: 't, KC> {
    iter: RoRange<'t, KC, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<T>,
}

impl<'t, T: 't, KC> FacetRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    pub fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<KC, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<T>,
        right: Bound<T>,
    ) -> heed::Result<FacetRange<'t, T, KC>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, T::min_value())),
            Excluded(left) => Excluded((field_id, level, left, T::min_value())),
            Unbounded => Included((field_id, level, T::min_value(), T::min_value())),
        };
        let right_bound = Included((field_id, level, T::max_value(), T::max_value()));
        let iter = db.lazily_decode_data().range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRange { iter, end: right })
    }
}

impl<'t, T, KC> Iterator for FacetRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    KC: BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy,
{
    type Item = heed::Result<((FieldId, u8, T, T), RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(((fid, level, left, right), docids))) => {
                let must_be_returned = match self.end {
                    Included(end) => right <= end,
                    Excluded(end) => right < end,
                    Unbounded => true,
                };
                if must_be_returned {
                    match docids.decode() {
                        Ok(docids) => Some(Ok(((fid, level, left, right), docids))),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    None
                }
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct FacetRevRange<'t, T: 't, KC> {
    iter: RoRevRange<'t, KC, LazyDecode<CboRoaringBitmapCodec>>,
    end: Bound<T>,
}

impl<'t, T: 't, KC> FacetRevRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy + Bounded,
{
    pub fn new(
        rtxn: &'t heed::RoTxn,
        db: Database<KC, CboRoaringBitmapCodec>,
        field_id: FieldId,
        level: u8,
        left: Bound<T>,
        right: Bound<T>,
    ) -> heed::Result<FacetRevRange<'t, T, KC>>
    {
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, T::min_value())),
            Excluded(left) => Excluded((field_id, level, left, T::min_value())),
            Unbounded => Included((field_id, level, T::min_value(), T::min_value())),
        };
        let right_bound = Included((field_id, level, T::max_value(), T::max_value()));
        let iter = db.lazily_decode_data().rev_range(rtxn, &(left_bound, right_bound))?;
        Ok(FacetRevRange { iter, end: right })
    }
}

impl<'t, T, KC> Iterator for FacetRevRange<'t, T, KC>
where
    KC: for<'a> BytesEncode<'a, EItem = (FieldId, u8, T, T)>,
    KC: BytesDecode<'t, DItem = (FieldId, u8, T, T)>,
    T: PartialOrd + Copy,
{
    type Item = heed::Result<((FieldId, u8, T, T), RoaringBitmap)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(((fid, level, left, right), docids))) => {
                    let must_be_returned = match self.end {
                        Included(end) => right <= end,
                        Excluded(end) => right < end,
                        Unbounded => true,
                    };
                    if must_be_returned {
                        match docids.decode() {
                            Ok(docids) => return Some(Ok(((fid, level, left, right), docids))),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    continue;
                },
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}
