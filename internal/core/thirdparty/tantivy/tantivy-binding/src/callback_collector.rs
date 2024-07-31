use libc::c_void;
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId,
};

#[derive(Clone, Copy)]
pub struct BitsetType {
    pub inner: *mut c_void,
}

unsafe impl Send for BitsetType {}
unsafe impl Sync for BitsetType {}

pub type CallbackOnOffsetFn = unsafe extern "C" fn(*mut c_void, u32);

pub struct CallbackCollector {
    callback: CallbackOnOffsetFn,
    bitset: BitsetType,
}

impl CallbackCollector {
    pub fn new(callback: CallbackOnOffsetFn, bitset: *mut c_void) -> Self {
        CallbackCollector {
            callback,
            bitset: BitsetType { inner: bitset },
        }
    }
}

type ResultFruitType = ();

impl Collector for CallbackCollector {
    type Fruit = ResultFruitType;

    type Child = CallbackChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(CallbackChildCollector {
            callback: self.callback,
            bitset: self.bitset,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, _segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        Ok(())
    }
}

pub struct CallbackChildCollector {
    callback: CallbackOnOffsetFn,
    bitset: BitsetType,
}

impl SegmentCollector for CallbackChildCollector {
    type Fruit = ResultFruitType;

    fn collect(&mut self, doc: DocId, _score: tantivy::Score) {
        unsafe {
            (self.callback)(self.bitset.inner, doc);
        }
    }

    fn harvest(self) -> Self::Fruit {
        ()
    }
}
