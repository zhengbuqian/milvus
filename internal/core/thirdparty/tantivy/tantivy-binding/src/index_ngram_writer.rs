use std::{mem::size_of, sync::Arc};

use log::info;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexSettings, IndexWriter, SingleSegmentIndexWriter};

use crate::error::{Result, TantivyBindingError};
use crate::index_ngram_document::{NgramBatchData, NgramDocument};
use crate::index_writer::IndexWriterWrapper;
use crate::index_writer_v7::index_writer::finish_index_writer;

const NGRAM_TOKENIZER: &str = "ngram";
const REGULAR_NGRAM_DOCUMENT_BATCH_SIZE: usize = 16;
#[cfg(test)]
const REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE: usize = REGULAR_NGRAM_DOCUMENT_BATCH_SIZE;
const NGRAM_MEMORY_BASE_BYTES: u64 = 4 * 1024 * 1024;
const NGRAM_MEMORY_MIN_RESERVE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NgramRow<'a> {
    pub(crate) doc_id: u32,
    pub(crate) value: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub(crate) enum NgramWriteStatus {
    Applied = 0,
    ReplayRequiredBeforeBatch = 1,
    ReplayRequiredAfterBatch = 2,
    ReplayRequiredBeforeFinish = 3,
}

impl NgramWriteStatus {
    pub(crate) fn requires_replay(self) -> bool {
        self != Self::Applied
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NgramWriterMode {
    Regular,
    Direct { soft_limit_bytes: u64 },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NgramMemorySummary {
    pub(crate) logical_rows: u64,
    pub(crate) occurrence_upper_bound: u64,
    pub(crate) term_bytes_upper_bound: u64,
}

fn saturating_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn square_sum(value: u128) -> u128 {
    value * (value + 1) * (2 * value + 1) / 6
}

fn ngram_value_memory_summary(value_len: usize, min_gram: usize, max_gram: usize) -> (u64, u64) {
    let value_len = value_len as u128;
    let min_gram = min_gram as u128;
    let max_gram = (max_gram as u128).min(value_len);
    if min_gram > max_gram {
        return (0, 0);
    }

    let gram_width_count = max_gram - min_gram + 1;
    let gram_width_sum = gram_width_count * (min_gram + max_gram) / 2;
    let gram_width_square_sum = square_sum(max_gram) - square_sum(min_gram.saturating_sub(1));
    let occurrence_upper_bound = gram_width_count * (value_len + 1) - gram_width_sum;
    let term_bytes_upper_bound = (value_len + 1) * gram_width_sum - gram_width_square_sum;
    (
        saturating_u128_to_u64(occurrence_upper_bound),
        saturating_u128_to_u64(term_bytes_upper_bound),
    )
}

fn ngram_tokenizer_scratch_upper_bound(value_len: usize, max_gram: usize) -> u64 {
    let value_len = value_len as u128;
    let max_gram = max_gram as u128;
    let frontier_count = (value_len + 1).min(max_gram + 1);
    // CodepointFrontiers has no size hint, so Vec grows geometrically while
    // collecting it. Two times the final length, with Vec's small-allocation
    // floor, is a conservative bound for the live capacity used here.
    let frontier_capacity = (frontier_count * 2).max(4);
    let frontier_bytes = frontier_capacity * size_of::<usize>() as u128;
    // Token::text retains the largest emitted token. A UTF-8 codepoint uses at
    // most four bytes, and String growth can temporarily reserve up to roughly
    // twice the requested token length.
    let max_token_bytes = value_len.min(max_gram.saturating_mul(4));
    let token_capacity = if max_token_bytes == 0 {
        0
    } else {
        (max_token_bytes * 2).max(8)
    };
    saturating_u128_to_u64(frontier_bytes + token_capacity)
}

fn ngram_rows_tokenizer_scratch_upper_bound(rows: &[NgramRow<'_>], max_gram: usize) -> u64 {
    rows.iter()
        .filter_map(|row| row.value)
        .map(|value| ngram_tokenizer_scratch_upper_bound(value.len(), max_gram))
        .max()
        .unwrap_or(0)
}

impl NgramMemorySummary {
    pub(crate) fn from_rows(rows: &[NgramRow<'_>], min_gram: usize, max_gram: usize) -> Self {
        let mut summary = Self::default();
        summary.add_rows(rows, min_gram, max_gram);
        summary
    }

    pub(crate) fn add_rows(&mut self, rows: &[NgramRow<'_>], min_gram: usize, max_gram: usize) {
        self.logical_rows = self.logical_rows.saturating_add(rows.len() as u64);
        for row in rows {
            let Some(value) = row.value else {
                continue;
            };
            let (occurrences, term_bytes) =
                ngram_value_memory_summary(value.len(), min_gram, max_gram);
            self.occurrence_upper_bound = self.occurrence_upper_bound.saturating_add(occurrences);
            self.term_bytes_upper_bound = self.term_bytes_upper_bound.saturating_add(term_bytes);
        }
    }

    pub(crate) fn estimated_working_memory_bytes(self) -> u64 {
        let estimated = u128::from(NGRAM_MEMORY_BASE_BYTES)
            + u128::from(self.logical_rows) * 16
            + u128::from(self.term_bytes_upper_bound)
            + u128::from(self.occurrence_upper_bound) * 128;
        saturating_u128_to_u64(estimated)
    }

    pub(crate) fn required_memory_bytes(
        self,
        observed_peak_bytes: u64,
        batch_owned_bytes: u64,
        tokenizer_scratch_bytes: u64,
        soft_limit_bytes: u64,
    ) -> u64 {
        let persistent = self
            .estimated_working_memory_bytes()
            .max(observed_peak_bytes);
        let transient = batch_owned_bytes.saturating_add(tokenizer_scratch_bytes);
        let reserve = NGRAM_MEMORY_MIN_RESERVE_BYTES.max(soft_limit_bytes / 4);
        persistent.saturating_add(transient).saturating_add(reserve)
    }
}

pub(crate) fn ngram_required_memory_bytes(
    logical_rows: u64,
    occurrence_upper_bound: u64,
    term_bytes_upper_bound: u64,
    tokenizer_scratch_bytes: u64,
    observed_peak_bytes: u64,
    soft_limit_bytes: u64,
) -> u64 {
    NgramMemorySummary {
        logical_rows,
        occurrence_upper_bound,
        term_bytes_upper_bound,
    }
    .required_memory_bytes(
        observed_peak_bytes,
        0,
        tokenizer_scratch_bytes,
        soft_limit_bytes,
    )
}

pub(crate) enum NgramWriterBackend {
    Regular {
        index_writer: IndexWriter<NgramDocument>,
        index: Arc<Index>,
    },
    Direct {
        index_writer: SingleSegmentIndexWriter<NgramDocument>,
        soft_limit_bytes: u64,
        memory_summary: NgramMemorySummary,
        replay_required: bool,
    },
}

pub(crate) struct NgramIndexWriterWrapperImpl {
    pub(crate) field: Field,
    pub(crate) backend: NgramWriterBackend,
    min_gram: usize,
    max_gram: usize,
    last_doc_id: Option<u32>,
}

impl NgramIndexWriterWrapperImpl {
    fn validate_doc_ids(&self, rows: &[NgramRow<'_>]) -> Result<()> {
        let mut previous_doc_id = self.last_doc_id;
        for row in rows {
            if let Some(previous) = previous_doc_id {
                if row.doc_id <= previous {
                    return Err(TantivyBindingError::InvalidArgument(format!(
                        "ngram document IDs must be strictly increasing across batches: {} after {}",
                        row.doc_id, previous
                    )));
                }
            }
            previous_doc_id = Some(row.doc_id);
        }
        Ok(())
    }

    fn direct_memory_status(
        &mut self,
        projected_summary: NgramMemorySummary,
        batch_owned_bytes: u64,
        tokenizer_scratch_bytes: u64,
        replay_status: NgramWriteStatus,
    ) -> NgramWriteStatus {
        let NgramWriterBackend::Direct {
            index_writer,
            soft_limit_bytes,
            replay_required,
            ..
        } = &mut self.backend
        else {
            return NgramWriteStatus::Applied;
        };
        if *replay_required {
            return replay_status;
        }
        let observed_peak =
            u64::try_from(index_writer.estimated_finalize_base_mem_usage()).unwrap_or(u64::MAX);
        let required = projected_summary.required_memory_bytes(
            observed_peak,
            batch_owned_bytes,
            tokenizer_scratch_bytes,
            *soft_limit_bytes,
        );
        if required > *soft_limit_bytes {
            *replay_required = true;
            replay_status
        } else {
            NgramWriteStatus::Applied
        }
    }

    pub(crate) fn check_memory_before_finish(&mut self) -> Result<NgramWriteStatus> {
        let summary = match &self.backend {
            NgramWriterBackend::Regular { .. } => return Ok(NgramWriteStatus::Applied),
            NgramWriterBackend::Direct { memory_summary, .. } => *memory_summary,
        };
        Ok(self.direct_memory_status(summary, 0, 0, NgramWriteStatus::ReplayRequiredBeforeFinish))
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        if self.check_memory_before_finish()?.requires_replay() {
            return Err(TantivyBindingError::InternalError(
                "NGRAM direct writer must be replayed with the regular writer before finish"
                    .to_string(),
            ));
        }
        match self.backend {
            NgramWriterBackend::Regular {
                index_writer,
                index,
            } => finish_index_writer(index_writer, index, false),
            NgramWriterBackend::Direct { index_writer, .. } => {
                let index = index_writer.finalize()?;
                let metas = index.searchable_segment_metas()?;
                let segment_ids: Vec<_> =
                    metas.iter().map(|meta| meta.id().uuid_string()).collect();
                info!("tantivy index_writer finish, segments: {:?}", segment_ids);
                Ok(())
            }
        }
    }
}

fn for_each_contiguous_ngram_batch<'rows, 'value, E>(
    rows: &'rows [NgramRow<'value>],
    batch_size: usize,
    mut callback: impl FnMut(u32, &'rows [NgramRow<'value>]) -> std::result::Result<(), E>,
) -> std::result::Result<(), E> {
    let batch_size = batch_size.max(1);
    let mut batch_begin = 0;

    while batch_begin < rows.len() {
        let mut batch_end = batch_begin + 1;
        while batch_end < rows.len()
            && batch_end - batch_begin < batch_size
            && rows[batch_end - 1].doc_id.checked_add(1) == Some(rows[batch_end].doc_id)
        {
            batch_end += 1;
        }

        callback(rows[batch_begin].doc_id, &rows[batch_begin..batch_end])?;
        batch_begin = batch_end;
    }

    Ok(())
}

fn build_ngram_schema(field_name: &str) -> (Schema, Field) {
    let mut schema_builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer(NGRAM_TOKENIZER)
        .set_fieldnorms(false)
        .set_index_option(IndexRecordOption::Basic);
    let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
    let field = schema_builder.add_text_field(field_name, text_options);
    schema_builder.enable_user_specified_doc_id();
    (schema_builder.build(), field)
}

impl IndexWriterWrapper {
    // create a text writer according to `tanviy_index_version`.
    // version 7 is the latest version and is what we should use in most cases.
    // We may also build with version 5 for compatibility for reader nodes with older versions.
    pub(crate) fn create_ngram_writer(
        field_name: &str,
        path: &str,
        min_gram: usize,
        max_gram: usize,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<IndexWriterWrapper> {
        Self::create_ngram_writer_with_mode(
            field_name,
            path,
            min_gram,
            max_gram,
            num_threads,
            overall_memory_budget_in_bytes,
            NgramWriterMode::Direct {
                soft_limit_bytes: u64::MAX,
            },
        )
    }

    pub(crate) fn create_ngram_writer_with_mode(
        field_name: &str,
        path: &str,
        min_gram: usize,
        max_gram: usize,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
        mode: NgramWriterMode,
    ) -> Result<IndexWriterWrapper> {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(min_gram, max_gram, false)?)
            .dynamic()
            .build();

        let (schema, field) = build_ngram_schema(field_name);

        let settings = IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_dir(path)?;
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let backend = match mode {
            NgramWriterMode::Regular => {
                let index_writer: IndexWriter<NgramDocument> =
                    index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
                index_writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
                NgramWriterBackend::Regular {
                    index_writer,
                    index: Arc::new(index),
                }
            }
            NgramWriterMode::Direct { soft_limit_bytes } => NgramWriterBackend::Direct {
                index_writer: SingleSegmentIndexWriter::new(index, overall_memory_budget_in_bytes)?,
                soft_limit_bytes,
                memory_summary: NgramMemorySummary::default(),
                replay_required: false,
            },
        };

        Ok(IndexWriterWrapper::NgramV7(NgramIndexWriterWrapperImpl {
            field,
            backend,
            min_gram,
            max_gram,
            last_doc_id: None,
        }))
    }

    pub(crate) fn add_ngram_rows(&mut self, rows: &[NgramRow<'_>]) -> Result<NgramWriteStatus> {
        let IndexWriterWrapper::NgramV7(writer) = self else {
            return Err(TantivyBindingError::InternalError(
                "ngram batch submission requires an NGRAM V7 writer".to_string(),
            ));
        };
        if rows.is_empty() {
            return Ok(NgramWriteStatus::Applied);
        }
        writer.validate_doc_ids(rows)?;
        let field = writer.field;

        let total_bytes = rows.iter().try_fold(0usize, |total, row| {
            let value_len = row.value.map_or(0, str::len);
            total.checked_add(value_len).ok_or_else(|| {
                TantivyBindingError::InvalidArgument(
                    "ngram batch text byte count overflows usize".to_string(),
                )
            })
        })?;
        u32::try_from(total_bytes).map_err(|_| {
            TantivyBindingError::InvalidArgument(
                "ngram batch text exceeds the u32 document range".to_string(),
            )
        })?;
        let mut validated_start = 0u32;
        for row in rows {
            if let Some(value) = row.value {
                if validated_start == u32::MAX {
                    return Err(TantivyBindingError::InvalidArgument(
                        "ngram present value starts at the reserved absent offset".to_string(),
                    ));
                }
                validated_start =
                    validated_start
                        .checked_add(value.len() as u32)
                        .ok_or_else(|| {
                            TantivyBindingError::InvalidArgument(
                                "ngram document range overflows u32".to_string(),
                            )
                        })?;
            }
        }

        if let NgramWriterBackend::Regular { index_writer, .. } = &mut writer.backend {
            for_each_contiguous_ngram_batch(
                rows,
                REGULAR_NGRAM_DOCUMENT_BATCH_SIZE,
                |first_doc_id, batch| {
                    let batch_total_bytes =
                        batch.iter().map(|row| row.value.map_or(0, str::len)).sum();
                    let mut batch_text = String::with_capacity(batch_total_bytes);
                    for row in batch {
                        if let Some(value) = row.value {
                            batch_text.push_str(value);
                        }
                    }
                    let batch_data = Arc::new(NgramBatchData::new(field, batch_text)?);
                    let mut start = 0u32;
                    let documents = batch.iter().map(move |row| match row.value {
                        Some(value) => {
                            let len = value.len() as u32;
                            let document =
                                NgramDocument::from_validated_range(batch_data.clone(), start, len);
                            start += len;
                            document
                        }
                        None => NgramDocument::absent(batch_data.clone()),
                    });
                    index_writer
                        .add_documents_with_doc_id(first_doc_id, documents)
                        .map(|_| ())
                        .map_err(TantivyBindingError::from)
                },
            )?;
        } else {
            let memory_summary = match &writer.backend {
                NgramWriterBackend::Direct { memory_summary, .. } => *memory_summary,
                NgramWriterBackend::Regular { .. } => unreachable!(),
            };
            let mut projected_summary = memory_summary;
            projected_summary.add_rows(rows, writer.min_gram, writer.max_gram);
            let tokenizer_scratch_bytes =
                ngram_rows_tokenizer_scratch_upper_bound(rows, writer.max_gram);
            let pre_status = writer.direct_memory_status(
                projected_summary,
                total_bytes as u64,
                tokenizer_scratch_bytes,
                NgramWriteStatus::ReplayRequiredBeforeBatch,
            );
            if pre_status.requires_replay() {
                return Ok(pre_status);
            }

            let mut text = String::with_capacity(total_bytes);
            for row in rows {
                if let Some(value) = row.value {
                    text.push_str(value);
                }
            }
            let batch_data = Arc::new(NgramBatchData::new(field, text)?);
            let mut start = 0u32;
            let documents = rows.iter().map(move |row| {
                let document = match row.value {
                    Some(value) => {
                        let len = value.len() as u32;
                        let document =
                            NgramDocument::from_validated_range(batch_data.clone(), start, len);
                        start += len;
                        document
                    }
                    None => NgramDocument::absent(batch_data.clone()),
                };
                (row.doc_id, document)
            });
            let NgramWriterBackend::Direct {
                index_writer,
                memory_summary,
                ..
            } = &mut writer.backend
            else {
                unreachable!();
            };
            index_writer.add_documents_with_doc_ids(documents)?;
            *memory_summary = projected_summary;
            let post_status = writer.direct_memory_status(
                projected_summary,
                0,
                0,
                NgramWriteStatus::ReplayRequiredAfterBatch,
            );
            if post_status.requires_replay() {
                writer.last_doc_id = rows.last().map(|row| row.doc_id);
                return Ok(post_status);
            }
        }

        writer.last_doc_id = rows.last().map(|row| row.doc_id);
        Ok(NgramWriteStatus::Applied)
    }

    pub(crate) fn check_ngram_memory(&mut self) -> Result<NgramWriteStatus> {
        let IndexWriterWrapper::NgramV7(writer) = self else {
            return Err(TantivyBindingError::InternalError(
                "ngram memory check requires an NGRAM V7 writer".to_string(),
            ));
        };
        writer.check_memory_before_finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        ffi::c_void,
        path::Path,
        sync::Arc,
    };

    use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
    use tantivy::{
        schema::IndexRecordOption, DocSet, Index, IndexWriter, TantivyDocument, TERMINATED,
    };
    use tempfile::TempDir;

    use super::{
        build_ngram_schema, for_each_contiguous_ngram_batch, NgramMemorySummary, NgramRow,
        NgramWriteStatus, NgramWriterBackend, NgramWriterMode, NGRAM_MEMORY_MIN_RESERVE_BYTES,
        NGRAM_TOKENIZER, REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
    };
    use crate::{
        error::{Result, TantivyBindingError},
        index_ngram_document::{NgramBatchData, NgramDocument},
        index_reader::IndexReaderWrapper,
        index_writer::IndexWriterWrapper,
        util::set_bitset,
    };

    fn assert_ngram_operation_is_typed_error(result: Result<()>, operation: &str) {
        let Err(TantivyBindingError::InternalError(message)) = result else {
            panic!("{operation} must return a typed internal binding error");
        };
        assert!(message.contains("NGRAM-specific"), "{message}");
    }

    fn open_finished_ngram_reader(path: &Path) -> IndexReaderWrapper {
        let index = Index::open_in_dir(path).unwrap();
        IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap()
    }

    #[test]
    fn ngram_memory_summary_counts_occurrences_and_term_bytes() {
        let value = "x".repeat(64);
        let rows = [
            NgramRow {
                doc_id: 0,
                value: Some(value.as_str()),
            },
            NgramRow {
                doc_id: 1,
                value: None,
            },
        ];

        let summary = NgramMemorySummary::from_rows(&rows, 3, 4);

        assert_eq!(summary.logical_rows, 2);
        assert_eq!(summary.occurrence_upper_bound, 62 + 61);
        assert_eq!(summary.term_bytes_upper_bound, 62 * 3 + 61 * 4);
    }

    #[test]
    fn direct_ngram_writer_requests_replay_before_exceeding_soft_limit() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer_with_mode(
            "test",
            dir.path().to_str().unwrap(),
            3,
            4,
            1,
            15_000_000,
            NgramWriterMode::Direct {
                soft_limit_bytes: 1,
            },
        )
        .unwrap();
        let value = "x".repeat(64);

        let status = writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some(value.as_str()),
            }])
            .unwrap();

        assert_eq!(status, NgramWriteStatus::ReplayRequiredBeforeBatch);
        assert!(writer.finish().is_err());
        let index = Index::open_in_dir(dir.path()).unwrap();
        assert!(index.load_metas().unwrap().segments.is_empty());
    }

    #[test]
    fn direct_ngram_writer_accounts_for_zero_token_tokenizer_scratch() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer_with_mode(
            "test",
            dir.path().to_str().unwrap(),
            65_537,
            65_537,
            1,
            15_000_000,
            NgramWriterMode::Direct {
                soft_limit_bytes: u64::MAX,
            },
        )
        .unwrap();
        let value = "x".repeat(65_536);
        let rows = [NgramRow {
            doc_id: 0,
            value: Some(value.as_str()),
        }];
        let projected_summary = NgramMemorySummary::from_rows(&rows, 65_537, 65_537);
        assert_eq!(projected_summary.occurrence_upper_bound, 0);
        assert_eq!(projected_summary.term_bytes_upper_bound, 0);

        let IndexWriterWrapper::NgramV7(ngram_writer) = &mut writer else {
            panic!("NGRAM writer must use the V7 implementation");
        };
        let NgramWriterBackend::Direct {
            index_writer,
            soft_limit_bytes,
            ..
        } = &mut ngram_writer.backend
        else {
            panic!("explicit direct mode must use the direct backend");
        };
        let observed_peak =
            u64::try_from(index_writer.estimated_finalize_base_mem_usage()).unwrap_or(u64::MAX);
        let without_tokenizer_scratch = projected_summary
            .estimated_working_memory_bytes()
            .max(observed_peak.saturating_add(value.len() as u64));
        let tokenizer_scratch = ((value.len() as u64 + 1)
            .saturating_mul(2 * std::mem::size_of::<usize>() as u64))
        .saturating_add(value.len() as u64);
        *soft_limit_bytes = without_tokenizer_scratch
            .saturating_add(NGRAM_MEMORY_MIN_RESERVE_BYTES)
            .saturating_add(tokenizer_scratch / 2);
        assert!(*soft_limit_bytes < 256 * 1024 * 1024);

        let status = writer.add_ngram_rows(&rows).unwrap();

        assert_eq!(status, NgramWriteStatus::ReplayRequiredBeforeBatch);
    }

    #[test]
    fn direct_ngram_writer_requests_replay_before_finish_after_limit_is_lowered() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer_with_mode(
            "test",
            dir.path().to_str().unwrap(),
            3,
            4,
            1,
            15_000_000,
            NgramWriterMode::Direct {
                soft_limit_bytes: u64::MAX,
            },
        )
        .unwrap();
        let value = "unique-ngram-value".repeat(8);

        let add_status = writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 7,
                value: Some(value.as_str()),
            }])
            .unwrap();
        assert_eq!(add_status, NgramWriteStatus::Applied);

        let IndexWriterWrapper::NgramV7(writer) = &mut writer else {
            panic!("NGRAM writer must use the V7 implementation");
        };
        {
            let NgramWriterBackend::Direct {
                index_writer,
                soft_limit_bytes,
                replay_required,
                ..
            } = &mut writer.backend
            else {
                panic!("explicit direct mode must use the direct backend");
            };
            let finalize_estimate =
                u64::try_from(index_writer.estimated_finalize_base_mem_usage()).unwrap_or(u64::MAX);
            assert!(finalize_estimate > 1);
            assert!(!*replay_required);
            *soft_limit_bytes = finalize_estimate - 1;
        }

        let finish_status = writer.check_memory_before_finish().unwrap();

        assert_eq!(finish_status, NgramWriteStatus::ReplayRequiredBeforeFinish);
        let NgramWriterBackend::Direct {
            replay_required, ..
        } = &writer.backend
        else {
            panic!("explicit direct mode must use the direct backend");
        };
        assert!(*replay_required);
    }

    #[test]
    fn regular_ngram_writer_ignores_direct_soft_limit() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer_with_mode(
            "test",
            dir.path().to_str().unwrap(),
            3,
            4,
            1,
            15_000_000,
            NgramWriterMode::Regular,
        )
        .unwrap();
        let value = "x".repeat(64);

        let status = writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 3,
                value: Some(value.as_str()),
            }])
            .unwrap();

        assert_eq!(status, NgramWriteStatus::Applied);
        writer.finish().unwrap();
        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.segment_count, 1);
        assert_eq!(snapshot.max_docs, vec![4]);
    }

    #[test]
    fn ngram_writer_uses_lightweight_document_variant() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        assert!(matches!(writer, IndexWriterWrapper::NgramV7(_)));
    }

    #[test]
    fn direct_ngram_writer_rejects_commit_while_active() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        assert_ngram_operation_is_typed_error(writer.commit(), "commit before finish");
    }

    #[test]
    fn direct_ngram_writer_rejects_reader_while_active() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        let Err(TantivyBindingError::InternalError(message)) = writer.create_reader(set_bitset)
        else {
            panic!("create reader before finish must return a typed internal binding error");
        };
        assert!(message.contains("finish"), "{message}");
    }

    #[test]
    fn direct_ngram_finish_disables_dedicated_docstore_compression() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        writer.finish().unwrap();

        let index = Index::open_in_dir(dir.path()).unwrap();
        assert!(!index.settings().docstore_compress_dedicated_thread);
    }

    #[test]
    fn ngram_writer_rejects_non_ngram_operations_with_typed_errors() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let c_strings: [*const libc::c_char; 0] = [];
        let byte_ptrs: [*const u8; 0] = [];
        let lengths: [usize; 0] = [];
        let offsets: [*const i64; 0] = [];

        assert_ngram_operation_is_typed_error(writer.add("value", Some(0)), "generic add");
        assert_ngram_operation_is_typed_error(
            writer.add_array(["value"], Some(0)),
            "generic array add",
        );
        assert_ngram_operation_is_typed_error(writer.add_json("{}", Some(0)), "JSON add");
        assert_ngram_operation_is_typed_error(
            writer.add_json_batch(&c_strings, 0),
            "JSON batch add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_json(&c_strings, Some(0)),
            "JSON array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_keywords(&c_strings, Some(0)),
            "keyword array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_array_keywords_with_len(&byte_ptrs, &lengths, Some(0)),
            "length-delimited keyword array add",
        );
        assert_ngram_operation_is_typed_error(
            writer.add_json_key_stats(&c_strings, &offsets, &lengths),
            "JSON key stats add",
        );
        assert_ngram_operation_is_typed_error(writer.manual_merge(), "manual merge");
    }

    fn rows(count: usize, first_doc_id: u32) -> Vec<NgramRow<'static>> {
        (0..count)
            .map(|index| NgramRow {
                doc_id: first_doc_id + index as u32,
                value: Some("value"),
            })
            .collect()
    }

    fn grouped_batches(rows: &[NgramRow<'_>]) -> Vec<(u32, usize)> {
        let mut batches = Vec::new();
        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                batches.push((first_doc_id, batch.len()));
                Ok::<_, ()>(())
            },
        )
        .unwrap();
        batches
    }

    #[test]
    fn ngram_batch_grouping_handles_boundaries() {
        let batch_size = REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE;
        let next_batch_doc_id = 100 + batch_size as u32;

        assert_eq!(grouped_batches(&rows(0, 100)), Vec::new());
        assert_eq!(grouped_batches(&rows(1, 100)), vec![(100, 1)]);
        assert_eq!(
            grouped_batches(&rows(batch_size - 1, 100)),
            vec![(100, batch_size - 1)]
        );
        assert_eq!(
            grouped_batches(&rows(batch_size, 100)),
            vec![(100, batch_size)]
        );
        assert_eq!(
            grouped_batches(&rows(batch_size + 1, 100)),
            vec![(100, batch_size), (next_batch_doc_id, 1)]
        );
    }

    #[test]
    fn ngram_batch_grouping_splits_at_gaps_without_dropping_absent_rows() {
        let rows = [
            NgramRow {
                doc_id: 7,
                value: Some("first"),
            },
            NgramRow {
                doc_id: 8,
                value: None,
            },
            NgramRow {
                doc_id: 9,
                value: Some(""),
            },
            NgramRow {
                doc_id: 15,
                value: Some("after-gap"),
            },
            NgramRow {
                doc_id: 16,
                value: None,
            },
        ];
        let mut seen_values = Vec::new();
        let mut batches = Vec::new();

        for_each_contiguous_ngram_batch(
            &rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                batches.push((first_doc_id, batch.len()));
                seen_values.extend(batch.iter().map(|row| row.value));
                Ok::<_, ()>(())
            },
        )
        .unwrap();

        assert_eq!(batches, vec![(7, 3), (15, 2)]);
        assert_eq!(
            seen_values,
            vec![Some("first"), None, Some(""), Some("after-gap"), None]
        );
    }

    #[test]
    fn direct_ngram_add_rows_validates_the_whole_slice_before_writing() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let mut invalid_rows = rows(REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE + 1, 0);
        invalid_rows[REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE].doc_id = 0;

        assert!(writer.add_ngram_rows(&invalid_rows).is_err());

        writer
            .add_ngram_rows(&[NgramRow {
                doc_id: 0,
                value: Some("recovered"),
            }])
            .unwrap();
        writer.finish().unwrap();

        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.segment_count, 1);
        assert_eq!(snapshot.max_docs, vec![1]);
        assert_eq!(snapshot.postings.get(b"re".as_slice()), Some(&vec![0]));
    }

    #[test]
    fn v7_batch_submission_accepts_an_exact_size_document_iterator() {
        let dir = TempDir::new().unwrap();
        let writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let IndexWriterWrapper::NgramV7(mut writer) = writer else {
            panic!("ngram writer must use the NGRAM Tantivy V7 variant");
        };
        let field = writer.field;
        let text = "alphangram测试".to_string();
        let batch = Arc::new(NgramBatchData::new(field, text).unwrap());
        let ranges = [(0, 5), (5, 0), (5, "ngram测试".len() as u32)];
        let documents = ranges
            .into_iter()
            .map(|(start, len)| NgramDocument::present(batch.clone(), start, len).unwrap());

        let NgramWriterBackend::Direct { index_writer, .. } = &mut writer.backend else {
            panic!("default NGRAM writer must use the direct backend");
        };
        index_writer
            .add_documents_with_doc_ids([4, 5, 6].into_iter().zip(documents))
            .unwrap();
        writer.finish().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
        assert_eq!(reader.count().unwrap(), 7);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct IndexSnapshot {
        segment_count: usize,
        max_docs: Vec<u32>,
        postings: BTreeMap<Vec<u8>, Vec<u32>>,
    }

    fn index_snapshot(path: &Path) -> IndexSnapshot {
        let index = Index::open_in_dir(path).unwrap();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let field = index.schema().get_field("test").unwrap();
        let mut max_docs = Vec::new();
        let mut postings = BTreeMap::<Vec<u8>, Vec<u32>>::new();

        for segment_reader in searcher.segment_readers() {
            max_docs.push(segment_reader.max_doc());
            let inverted_index = segment_reader.inverted_index(field).unwrap();
            let mut terms = inverted_index.terms().stream().unwrap();
            while terms.advance() {
                let term = terms.key().to_vec();
                let mut term_postings = inverted_index
                    .read_postings_from_terminfo(terms.value(), IndexRecordOption::Basic)
                    .unwrap();
                let docs = postings.entry(term).or_default();
                while term_postings.doc() != TERMINATED {
                    docs.push(term_postings.doc());
                    term_postings.advance();
                }
            }
        }
        max_docs.sort_unstable();
        for docs in postings.values_mut() {
            docs.sort_unstable();
        }

        IndexSnapshot {
            segment_count: searcher.segment_readers().len(),
            max_docs,
            postings,
        }
    }

    fn build_default_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(2, 3, false).unwrap())
            .dynamic()
            .build();
        let (schema, field) = build_ngram_schema("test");
        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let mut writer: IndexWriter<TantivyDocument> =
            index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                let documents = batch.iter().map(|row| {
                    let mut document = TantivyDocument::default();
                    if let Some(value) = row.value {
                        document.add_text(field, value);
                    }
                    document
                });
                writer
                    .add_documents_with_doc_id(first_doc_id, documents)
                    .map(|_| ())
            },
        )
        .unwrap();
        writer.commit().unwrap();
    }

    fn build_regular_lightweight_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(2, 3, false).unwrap())
            .dynamic()
            .build();
        let (schema, field) = build_ngram_schema("test");
        let index = Index::create_in_dir(path, schema).unwrap();
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let mut writer: IndexWriter<NgramDocument> =
            index.writer_with_num_threads(1, 15_000_000).unwrap();
        writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        for_each_contiguous_ngram_batch(
            rows,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            |first_doc_id, batch| {
                let total_bytes = batch.iter().map(|row| row.value.map_or(0, str::len)).sum();
                let mut text = String::with_capacity(total_bytes);
                for row in batch {
                    if let Some(value) = row.value {
                        text.push_str(value);
                    }
                }
                let batch_data = Arc::new(NgramBatchData::new(field, text).unwrap());
                let mut start = 0u32;
                let documents = batch.iter().map(move |row| match row.value {
                    Some(value) => {
                        let len = value.len() as u32;
                        let document =
                            NgramDocument::from_validated_range(batch_data.clone(), start, len);
                        start += len;
                        document
                    }
                    None => NgramDocument::absent(batch_data.clone()),
                });
                writer
                    .add_documents_with_doc_id(first_doc_id, documents)
                    .map(|_| ())
            },
        )
        .unwrap();
        writer.commit().unwrap();
    }

    fn build_finished_ngram_index(path: &Path, rows: &[NgramRow<'_>]) {
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            path.to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        writer.add_ngram_rows(rows).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn direct_and_regular_lightweight_writers_match_postings_and_max_doc() {
        let values = [
            Some("alpha"),
            None,
            Some(""),
            Some("nul\0byte"),
            Some("ngram测试"),
            None,
        ];
        let doc_ids = [40, 41, 42, 100, 101, 109];
        let rows: Vec<_> = doc_ids
            .into_iter()
            .zip(values)
            .map(|(doc_id, value)| NgramRow { doc_id, value })
            .collect();
        let regular_dir = TempDir::new().unwrap();
        let direct_dir = TempDir::new().unwrap();

        build_regular_lightweight_document_index(regular_dir.path(), &rows);
        build_finished_ngram_index(direct_dir.path(), &rows);

        let regular = index_snapshot(regular_dir.path());
        let direct = index_snapshot(direct_dir.path());
        assert_eq!(direct, regular);
        assert_eq!(direct.segment_count, 1);
        assert_eq!(direct.max_docs, vec![110]);
    }

    fn build_lightweight_document_index(path: &Path, rows: &[NgramRow<'_>]) {
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            path.to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        writer.add_ngram_rows(rows).unwrap();
        writer.finish().unwrap();
    }

    fn assert_default_and_lightweight_equivalent(rows: &[NgramRow<'_>]) -> IndexSnapshot {
        let default_dir = TempDir::new().unwrap();
        let lightweight_dir = TempDir::new().unwrap();
        build_default_document_index(default_dir.path(), rows);
        build_lightweight_document_index(lightweight_dir.path(), rows);

        let default_snapshot = index_snapshot(default_dir.path());
        let lightweight_snapshot = index_snapshot(lightweight_dir.path());
        assert_eq!(default_snapshot, lightweight_snapshot);
        lightweight_snapshot
    }

    #[test]
    fn lightweight_documents_match_default_documents_around_batch_boundaries() {
        for count in [
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE - 1,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE,
            REFERENCE_NGRAM_DOCUMENT_BATCH_SIZE + 1,
        ] {
            let values: Vec<_> = (0..count)
                .map(|index| format!("row-{index:02}-测试"))
                .collect();
            let rows: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(index, value)| NgramRow {
                    doc_id: 20 + index as u32,
                    value: Some(value.as_str()),
                })
                .collect();

            let snapshot = assert_default_and_lightweight_equivalent(&rows);
            assert_eq!(snapshot.segment_count, 1);
            assert_eq!(snapshot.max_docs, vec![20 + count as u32]);
        }
    }

    #[test]
    fn lightweight_documents_preserve_gaps_empty_values_and_trailing_absent_rows() {
        let values = [
            Some("alpha"),
            None,
            Some(""),
            Some("nul\0byte"),
            Some("ngram测试"),
            None,
        ];
        let doc_ids = [40, 41, 42, 100, 101, 109];
        let rows: Vec<_> = doc_ids
            .into_iter()
            .zip(values)
            .map(|(doc_id, value)| NgramRow { doc_id, value })
            .collect();

        let snapshot = assert_default_and_lightweight_equivalent(&rows);
        assert_eq!(snapshot.segment_count, 1);
        assert_eq!(snapshot.max_docs, vec![110]);
    }

    #[test]
    fn lightweight_documents_own_text_after_input_rows_are_dropped() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        {
            let values = [String::from("alpha测试"), String::from("nul\0byte")];
            let rows = [
                NgramRow {
                    doc_id: 3,
                    value: Some(values[0].as_str()),
                },
                NgramRow {
                    doc_id: 4,
                    value: Some(values[1].as_str()),
                },
            ];
            writer.add_ngram_rows(&rows).unwrap();
        }
        writer.finish().unwrap();

        let snapshot = index_snapshot(dir.path());
        assert_eq!(snapshot.max_docs, vec![5]);
        assert!(snapshot.postings.contains_key(b"al".as_slice()));
    }

    #[test]
    fn batched_ngram_documents_match_per_row_postings_and_max_doc() {
        let values: Vec<_> = (0..65)
            .map(|index| (100 + index, Some(format!("row-{index:02}-alpha"))))
            .chain([
                (200, Some(String::new())),
                (201, Some("nul\0byte".to_string())),
                (205, Some("ngram测试".to_string())),
                (209, None),
            ])
            .collect();
        let mut rows: Vec<_> = values
            .iter()
            .map(|(doc_id, value)| NgramRow {
                doc_id: *doc_id,
                value: value.as_deref(),
            })
            .collect();
        rows[1].value = None;
        rows[31].value = Some("");

        let reference_dir = TempDir::new().unwrap();
        let batched_dir = TempDir::new().unwrap();
        build_default_document_index(reference_dir.path(), &rows);
        let mut batched = IndexWriterWrapper::create_ngram_writer(
            "test",
            batched_dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        batched.add_ngram_rows(&rows[..33]).unwrap();
        batched.add_ngram_rows(&rows[33..65]).unwrap();
        batched.add_ngram_rows(&rows[65..]).unwrap();
        batched.finish().unwrap();

        assert_eq!(
            index_snapshot(reference_dir.path()),
            index_snapshot(batched_dir.path())
        );
    }

    #[test]
    fn test_create_ngram_writer() {
        let dir = TempDir::new().unwrap();
        let _ = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            1,
            2,
            1,
            15000000,
        )
        .unwrap();
    }

    #[test]
    fn test_ngram_writer() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15000000,
        )
        .unwrap();

        let values = [
            "university",
            "anthropology",
            "economics",
            "history",
            "victoria",
            "basics",
            "economiCs",
        ];
        let rows: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(doc_id, value)| NgramRow {
                doc_id: doc_id as u32,
                value: Some(value),
            })
            .collect();
        writer.add_ngram_rows(&rows).unwrap();

        writer.finish().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("ic", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![2, 4, 5].into_iter().collect::<HashSet<u32>>());
    }

    #[test]
    fn test_ngram_writer_chinese() {
        let dir = TempDir::new().unwrap();
        let mut writer = IndexWriterWrapper::create_ngram_writer(
            "test",
            dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15000000,
        )
        .unwrap();

        let values = [
            "ngram测试",
            "测试ngram",
            "测试ngram测试",
            "你好世界",
            "ngram需要被测试",
        ];
        let rows: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(doc_id, value)| NgramRow {
                doc_id: doc_id as u32,
                value: Some(value),
            })
            .collect();
        writer.add_ngram_rows(&rows).unwrap();

        writer.finish().unwrap();

        let reader = open_finished_ngram_reader(dir.path());
        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 1, 2, 4].into_iter().collect::<HashSet<u32>>());

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("m测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![0, 2].into_iter().collect::<HashSet<u32>>());

        let mut res: HashSet<u32> = HashSet::new();
        reader
            .ngram_match_query("需要被测试", 2, 3, &mut res as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(res, vec![4].into_iter().collect::<HashSet<u32>>());
    }
}
