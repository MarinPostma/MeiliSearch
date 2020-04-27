use std::borrow::Cow;
use std::hash::Hash;
use std::collections::HashMap;

use heed::{RwTxn, RoTxn};
use zerocopy::{AsBytes, FromBytes};
use crate::database::MainT;
use heed::Result as ZResult;
use meilisearch_types::DocumentId;
use meilisearch_schema::FieldId;
use heed::types::CowSlice;
use crate::error::Error;
use sdset::Set;

#[derive(Debug, Eq, PartialEq, Hash, AsBytes, FromBytes)]
#[repr(transparent)]
pub struct FacetKey(u64);

impl FacetKey {
    pub fn new(_field_id: FieldId, _value:String) -> Self {
        todo!()
    }
}

impl<'a> heed::BytesEncode<'a> for FacetKey {
    type EItem = FacetKey;

    fn bytes_encode(_item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        todo!()
    }
}

impl<'a> heed::BytesDecode<'a> for FacetKey {
    type DItem = FacetKey;

    fn bytes_decode(_bytes: &'a [u8]) -> Option<Self::DItem> {
        todo!()        
    }
}


/// contains facet info
#[derive(Clone)]
pub struct Facets {
    pub(crate) facets: heed::Database<FacetKey, CowSlice<DocumentId>>,
}

impl Facets {
    // we use sdset::SetBuf to ensure the docids are sorted.
    pub fn put_facets(&self, writer: &mut RwTxn<MainT>, facet_key: FacetKey, doc_ids: &Set<DocumentId>) -> ZResult<()> {
        // TODO error reporting
        self.facets.put(writer, &facet_key, doc_ids)
    }

    pub fn document_ids<'txn>(
        &self,
        reader: &'txn RoTxn<MainT>,
        facet_key: FacetKey,
    ) -> Result<Option<Cow<'txn, [DocumentId]>>, Error> {
        // TODO error reporting
        self.facets.get(reader, &facet_key).map_err(Error::from)
    }

    pub fn update(&self, writer: &mut RwTxn<MainT>, facet_map: HashMap<FacetKey, Vec<DocumentId>>) -> ZResult<()>{
        for (key, mut document_ids) in facet_map {
            document_ids.sort_unstable();
            let set = Set::new_unchecked(&document_ids);
            self.put_facets(writer, key, set)?;
        }
        Ok(())
    }
}
