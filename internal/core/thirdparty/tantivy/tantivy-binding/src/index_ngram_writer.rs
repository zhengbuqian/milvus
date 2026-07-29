use std::sync::Arc;

use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{NgramTokenizer, TextAnalyzer};
use tantivy::{Index, TantivyDocument};

use crate::error::{Result, TantivyBindingError};
use crate::index_writer::IndexWriterWrapper;
use crate::index_writer_v7::IndexWriterWrapperImpl;

const NGRAM_TOKENIZER: &str = "ngram";
// Tantivy's regular writer bounds the channel by messages, so keep each
// submission small enough to preserve pipeline backpressure.
pub(crate) const NGRAM_DOCUMENT_BATCH_SIZE: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NgramRow<'a> {
    pub(crate) doc_id: u32,
    pub(crate) value: Option<&'a str>,
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
        let tokenizer = TextAnalyzer::builder(NgramTokenizer::new(
            min_gram as usize,
            max_gram as usize,
            false,
        )?)
        .dynamic()
        .build();

        let (schema, field) = build_ngram_schema(field_name);

        let index = Index::create_in_dir(path, schema)?;
        index.tokenizers().register(NGRAM_TOKENIZER, tokenizer);
        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;
        // Ngram writers are only used for sealed index builds, which end with
        // an explicit merge-all in finish(); background merges would only
        // waste IO and race with it.
        index_writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));

        Ok(IndexWriterWrapper::V7(IndexWriterWrapperImpl {
            field,
            index_writer,
            index: Arc::new(index),
            enable_user_specified_doc_id: true,
            id_field: None,
            // Sealed-build only; merge-all runs in finish().
            enable_background_merge: false,
        }))
    }

    pub(crate) fn add_ngram_rows(&mut self, rows: &[NgramRow<'_>]) -> Result<()> {
        let IndexWriterWrapper::V7(writer) = self else {
            return Err(TantivyBindingError::InternalError(
                "ngram batch submission requires a Tantivy V7 writer".to_string(),
            ));
        };
        let field = writer.field;

        for_each_contiguous_ngram_batch(rows, NGRAM_DOCUMENT_BATCH_SIZE, |first_doc_id, batch| {
            let documents = batch.iter().map(|row| {
                let mut document = TantivyDocument::default();
                if let Some(value) = row.value {
                    document.add_text(field, value);
                }
                document
            });
            writer.add_documents_with_doc_id(first_doc_id, documents)
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        ffi::c_void,
        path::Path,
    };

    use tantivy::{schema::IndexRecordOption, DocSet, Index, TantivyDocument, TERMINATED};
    use tempfile::TempDir;

    use super::{for_each_contiguous_ngram_batch, NgramRow, NGRAM_DOCUMENT_BATCH_SIZE};
    use crate::{index_writer::IndexWriterWrapper, util::set_bitset};

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
        for_each_contiguous_ngram_batch(rows, NGRAM_DOCUMENT_BATCH_SIZE, |first_doc_id, batch| {
            batches.push((first_doc_id, batch.len()));
            Ok::<_, ()>(())
        })
        .unwrap();
        batches
    }

    #[test]
    fn ngram_batch_grouping_handles_boundaries() {
        let batch_size = NGRAM_DOCUMENT_BATCH_SIZE;
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

        for_each_contiguous_ngram_batch(&rows, NGRAM_DOCUMENT_BATCH_SIZE, |first_doc_id, batch| {
            batches.push((first_doc_id, batch.len()));
            seen_values.extend(batch.iter().map(|row| row.value));
            Ok::<_, ()>(())
        })
        .unwrap();

        assert_eq!(batches, vec![(7, 3), (15, 2)]);
        assert_eq!(
            seen_values,
            vec![Some("first"), None, Some(""), Some("after-gap"), None]
        );
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
        let IndexWriterWrapper::V7(mut writer) = writer else {
            panic!("ngram writer must use Tantivy V7");
        };
        let field = writer.field;
        let documents = ["alpha", "", "ngram测试"].into_iter().map(|value| {
            let mut document = TantivyDocument::default();
            document.add_text(field, value);
            document
        });

        writer.add_documents_with_doc_id(4, documents).unwrap();
        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
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

    fn add_rows_one_at_a_time(writer: &mut IndexWriterWrapper, rows: &[NgramRow<'_>]) {
        for row in rows {
            let doc_id = Some(i64::from(row.doc_id));
            match row.value {
                Some(value) => writer.add(value, doc_id).unwrap(),
                None => writer
                    .add_array(std::iter::empty::<&str>(), doc_id)
                    .unwrap(),
            }
        }
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
        let mut reference = IndexWriterWrapper::create_ngram_writer(
            "test",
            reference_dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();
        let mut batched = IndexWriterWrapper::create_ngram_writer(
            "test",
            batched_dir.path().to_str().unwrap(),
            2,
            3,
            1,
            15_000_000,
        )
        .unwrap();

        add_rows_one_at_a_time(&mut reference, &rows);
        batched.add_ngram_rows(&rows[..33]).unwrap();
        batched.add_ngram_rows(&rows[33..65]).unwrap();
        batched.add_ngram_rows(&rows[65..]).unwrap();
        reference.commit().unwrap();
        batched.commit().unwrap();

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

        writer.add("university", Some(0)).unwrap();
        writer.add("anthropology", Some(1)).unwrap();
        writer.add("economics", Some(2)).unwrap();
        writer.add("history", Some(3)).unwrap();
        writer.add("victoria", Some(4)).unwrap();
        writer.add("basics", Some(5)).unwrap();
        writer.add("economiCs", Some(6)).unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
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

        writer.add("ngram测试", Some(0)).unwrap();
        writer.add("测试ngram", Some(1)).unwrap();
        writer.add("测试ngram测试", Some(2)).unwrap();
        writer.add("你好世界", Some(3)).unwrap();
        writer.add("ngram需要被测试", Some(4)).unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader(set_bitset).unwrap();
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
