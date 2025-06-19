pub struct DirObjectBuilder<RecordId: Clone + Debug + Eq + Hash> {
    storage: Arc<dyn DirStorage<RecordId>>,
    record_id: RecordId,
}

impl<RecordId: Clone + Debug + Eq + Hash> DirObjectBuilder<RecordId> {
    pub fn new(storage: Arc<dyn DirStorage<RecordId>>, record_id: RecordId) -> Self {
        Self { storage, record_id }
    }

    pub fn build(self) -> DirObject {
        DirObject {
            storage: self.storage,
            record_id: self.record_id,
            meta: DirMeta {}, // Initialize with default metadata
        }
    }
}
