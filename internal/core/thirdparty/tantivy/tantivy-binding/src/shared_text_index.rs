use log::info;
use std::sync::Arc;

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::Column;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::TextAnalyzer;
use tantivy::{DocId, Index, IndexReader, IndexWriter, ReloadPolicy, Score, SegmentOrdinal, SegmentReader, Term};

use crate::analyzer::{create_analyzer, standard_analyzer};
use crate::error::{Result, TantivyBindingError};

/// SharedTextIndex is designed for growing segments to share a single tantivy index.
/// Multiple segments can write to the same index, distinguished by segment_id field.
/// This significantly reduces memory overhead compared to per-segment indexes.
///
/// Schema:
/// - text_field: TEXT (with positions for phrase_match)
/// - _segment_id: u64 (INDEXED | FAST) - for filtering by segment
/// - _local_doc_id: u64 (FAST) - maps back to segment-local doc id
pub struct SharedTextIndexWriter {
    text_field: Field,
    segment_id_field: Field,
    local_doc_id_field: Field,
    index_writer: IndexWriter,
    index: Arc<Index>,
    tokenizer_name: String,
}

pub struct SharedTextIndexReader {
    text_field: Field,
    segment_id_field: Field,
    local_doc_id_field: Field,
    reader: IndexReader,
    index: Arc<Index>,
}

fn build_shared_text_schema(
    field_name: &str,
    tokenizer_name: &str,
) -> (Schema, Field, Field, Field) {
    let mut schema_builder = Schema::builder();

    // Text field with positions for phrase matching
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(tokenizer_name)
        .set_fieldnorms(false)
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default().set_indexing_options(indexing);
    let text_field = schema_builder.add_text_field(field_name, text_options);

    // Segment ID field - indexed for filtering, fast for reading
    let segment_id_field = schema_builder.add_u64_field(
        "_segment_id",
        NumericOptions::default().set_indexed().set_fast(),
    );

    // Local doc ID field - fast only, for reading back the original offset
    let local_doc_id_field =
        schema_builder.add_u64_field("_local_doc_id", NumericOptions::default().set_fast());

    let schema = schema_builder.build();
    (schema, text_field, segment_id_field, local_doc_id_field)
}

impl SharedTextIndexWriter {
    pub fn new(
        field_name: &str,
        tokenizer_name: &str,
        analyzer_params: &str,
        num_threads: usize,
        overall_memory_budget_in_bytes: usize,
    ) -> Result<Self> {
        info!(
            "create shared text index writer, field_name: {}, tokenizer: {}",
            field_name, tokenizer_name
        );

        let tokenizer = create_analyzer(analyzer_params)?;
        let (schema, text_field, segment_id_field, local_doc_id_field) =
            build_shared_text_schema(field_name, tokenizer_name);

        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register(tokenizer_name, tokenizer.clone());

        let index_writer =
            index.writer_with_num_threads(num_threads, overall_memory_budget_in_bytes)?;

        Ok(Self {
            text_field,
            segment_id_field,
            local_doc_id_field,
            index_writer,
            index: Arc::new(index),
            tokenizer_name: tokenizer_name.to_string(),
        })
    }

    /// Add a text document with segment_id and local_doc_id
    pub fn add_text(&mut self, segment_id: u64, local_doc_id: u64, text: &str) -> Result<()> {
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(self.text_field, text);
        doc.add_u64(self.segment_id_field, segment_id);
        doc.add_u64(self.local_doc_id_field, local_doc_id);
        self.index_writer.add_document(doc)?;
        Ok(())
    }

    /// Add multiple texts for a segment in batch
    pub fn add_texts(
        &mut self,
        segment_id: u64,
        texts: &[&str],
        offset_begin: u64,
    ) -> Result<()> {
        for (i, text) in texts.iter().enumerate() {
            self.add_text(segment_id, offset_begin + i as u64, text)?;
        }
        Ok(())
    }

    /// Delete all documents belonging to a segment
    pub fn delete_segment(&mut self, segment_id: u64) -> Result<()> {
        let term = Term::from_field_u64(self.segment_id_field, segment_id);
        self.index_writer.delete_term(term);
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.index_writer.commit()?;
        Ok(())
    }

    pub fn create_reader(&self) -> Result<SharedTextIndexReader> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(TantivyBindingError::TantivyError)?;

        Ok(SharedTextIndexReader {
            text_field: self.text_field,
            segment_id_field: self.segment_id_field,
            local_doc_id_field: self.local_doc_id_field,
            reader,
            index: self.index.clone(),
        })
    }

    pub fn register_tokenizer(&self, tokenizer_name: &str, tokenizer: TextAnalyzer) {
        self.index.tokenizers().register(tokenizer_name, tokenizer);
    }

    pub fn get_index(&self) -> Arc<Index> {
        self.index.clone()
    }
}

impl SharedTextIndexReader {
    pub fn reload(&self) -> Result<()> {
        self.reader
            .reload()
            .map_err(TantivyBindingError::TantivyError)
    }

    /// Match query filtered by segment_id, returns local_doc_ids
    pub fn match_query(&self, segment_id: u64, query_str: &str) -> Result<Vec<u64>> {
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.text_field)
            .unwrap_or(standard_analyzer(vec![]))
            .clone();

        use tantivy::tokenizer::TokenStream;
        let mut token_stream = tokenizer.token_stream(query_str);
        let mut terms: Vec<Term> = Vec::new();
        while token_stream.advance() {
            let token = token_stream.token();
            terms.push(Term::from_field_text(self.text_field, &token.text));
        }

        if terms.is_empty() {
            return Ok(vec![]);
        }

        // Build text query
        let text_query = BooleanQuery::new_multiterms_query(terms);

        // Build segment filter
        let segment_filter = TermQuery::new(
            Term::from_field_u64(self.segment_id_field, segment_id),
            IndexRecordOption::Basic,
        );

        // Combine: text_query AND segment_filter
        let combined_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(text_query)),
            (Occur::Must, Box::new(segment_filter)),
        ]);

        self.search_local_doc_ids(&combined_query)
    }

    /// Match query with minimum should match, filtered by segment_id
    pub fn match_query_with_minimum(
        &self,
        segment_id: u64,
        query_str: &str,
        min_should_match: usize,
    ) -> Result<Vec<u64>> {
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.text_field)
            .unwrap_or(standard_analyzer(vec![]))
            .clone();

        use tantivy::tokenizer::TokenStream;
        let mut token_stream = tokenizer.token_stream(query_str);
        let mut terms: Vec<Term> = Vec::new();
        while token_stream.advance() {
            let token = token_stream.token();
            terms.push(Term::from_field_text(self.text_field, &token.text));
        }

        if terms.is_empty() {
            return Ok(vec![]);
        }

        // Build text subqueries
        let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
        for term in terms {
            subqueries.push((
                Occur::Should,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }

        let effective_min = std::cmp::max(1, min_should_match);
        let text_query = BooleanQuery::with_minimum_required_clauses(subqueries, effective_min);

        // Build segment filter
        let segment_filter = TermQuery::new(
            Term::from_field_u64(self.segment_id_field, segment_id),
            IndexRecordOption::Basic,
        );

        // Combine: text_query AND segment_filter
        let combined_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(text_query)),
            (Occur::Must, Box::new(segment_filter)),
        ]);

        self.search_local_doc_ids(&combined_query)
    }

    /// Phrase match query filtered by segment_id
    pub fn phrase_match_query(
        &self,
        segment_id: u64,
        query_str: &str,
        slop: u32,
    ) -> Result<Vec<u64>> {
        let mut tokenizer = self
            .index
            .tokenizer_for_field(self.text_field)
            .unwrap_or(standard_analyzer(vec![]))
            .clone();

        use tantivy::tokenizer::TokenStream;
        let mut token_stream = tokenizer.token_stream(query_str);
        let mut terms: Vec<Term> = Vec::new();
        let mut positions: Vec<usize> = Vec::new();

        while token_stream.advance() {
            let token = token_stream.token();
            positions.push(token.position);
            terms.push(Term::from_field_text(self.text_field, &token.text));
        }

        if terms.is_empty() {
            return Ok(vec![]);
        }

        // Build segment filter
        let segment_filter = TermQuery::new(
            Term::from_field_u64(self.segment_id_field, segment_id),
            IndexRecordOption::Basic,
        );

        // For single term, use simple term query
        if terms.len() == 1 {
            let text_query = BooleanQuery::new_multiterms_query(terms);
            let combined_query = BooleanQuery::new(vec![
                (Occur::Must, Box::new(text_query)),
                (Occur::Must, Box::new(segment_filter)),
            ]);
            return self.search_local_doc_ids(&combined_query);
        }

        // Build phrase query
        let terms_with_offset: Vec<_> = positions.into_iter().zip(terms.into_iter()).collect();
        let phrase_query = PhraseQuery::new_with_offset_and_slop(terms_with_offset, slop);

        // Combine: phrase_query AND segment_filter
        let combined_query = BooleanQuery::new(vec![
            (Occur::Must, Box::new(phrase_query)),
            (Occur::Must, Box::new(segment_filter)),
        ]);

        self.search_local_doc_ids(&combined_query)
    }

    /// Internal method to search and collect local_doc_ids
    fn search_local_doc_ids(&self, query: &dyn tantivy::query::Query) -> Result<Vec<u64>> {
        let searcher = self.reader.searcher();
        let collector = LocalDocIdCollector {
            local_doc_id_field: self.local_doc_id_field,
        };
        
        let local_doc_ids = searcher
            .search(query, &collector)
            .map_err(TantivyBindingError::TantivyError)?;

        Ok(local_doc_ids)
    }

    pub fn register_tokenizer(&self, tokenizer_name: &str, tokenizer: TextAnalyzer) {
        self.index.tokenizers().register(tokenizer_name, tokenizer);
    }
}

// Custom collector to collect local_doc_ids from fast field
struct LocalDocIdCollector {
    local_doc_id_field: Field,
}

struct LocalDocIdChildCollector {
    local_doc_ids: Vec<u64>,
    column: Column<u64>,
}

impl Collector for LocalDocIdCollector {
    type Fruit = Vec<u64>;
    type Child = LocalDocIdChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column = segment
            .fast_fields()
            .u64("_local_doc_id")?;
        
        Ok(LocalDocIdChildCollector {
            local_doc_ids: Vec::new(),
            column,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut merged = Vec::new();
        for fruit in segment_fruits {
            merged.extend(fruit);
        }
        Ok(merged)
    }
}

impl SegmentCollector for LocalDocIdChildCollector {
    type Fruit = Vec<u64>;

    fn collect_block(&mut self, docs: &[DocId]) {
        self.local_doc_ids
            .extend(self.column.values_for_docs_flatten(docs));
    }

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.collect_block(&[doc]);
    }

    fn harvest(self) -> Self::Fruit {
        self.local_doc_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_shared_text_index_basic() {
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "default",
            "{}",
            1,
            50_000_000,
        )
        .unwrap();

        // Add docs for segment 1
        writer.add_text(1, 0, "hello world").unwrap();
        writer.add_text(1, 1, "hello rust").unwrap();

        // Add docs for segment 2
        writer.add_text(2, 0, "hello python").unwrap();
        writer.add_text(2, 1, "goodbye world").unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // Query segment 1 for "hello"
        let results = reader.match_query(1, "hello").unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0, 1].into_iter().collect::<HashSet<u64>>());

        // Query segment 2 for "hello"
        let results = reader.match_query(2, "hello").unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0].into_iter().collect::<HashSet<u64>>());

        // Query segment 1 for "world"
        let results = reader.match_query(1, "world").unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0].into_iter().collect::<HashSet<u64>>());

        // Query segment 2 for "world"
        let results = reader.match_query(2, "world").unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![1].into_iter().collect::<HashSet<u64>>());
    }

    #[test]
    fn test_shared_text_index_segment_isolation() {
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "default",
            "{}",
            1,
            50_000_000,
        )
        .unwrap();

        // Add same text to different segments with different local_doc_ids
        writer.add_text(100, 0, "unique content").unwrap();
        writer.add_text(200, 5, "unique content").unwrap();
        writer.add_text(300, 10, "different content").unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // Each segment should only see its own docs
        let results = reader.match_query(100, "unique").unwrap();
        assert_eq!(results, vec![0]);

        let results = reader.match_query(200, "unique").unwrap();
        assert_eq!(results, vec![5]);

        let results = reader.match_query(300, "unique").unwrap();
        assert!(results.is_empty());

        let results = reader.match_query(300, "different").unwrap();
        assert_eq!(results, vec![10]);
    }

    #[test]
    fn test_shared_text_index_delete_segment() {
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "default",
            "{}",
            1,
            50_000_000,
        )
        .unwrap();

        writer.add_text(1, 0, "hello world").unwrap();
        writer.add_text(2, 0, "hello world").unwrap();
        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // Both segments have data
        assert!(!reader.match_query(1, "hello").unwrap().is_empty());
        assert!(!reader.match_query(2, "hello").unwrap().is_empty());

        // Delete segment 1
        writer.delete_segment(1).unwrap();
        writer.commit().unwrap();
        reader.reload().unwrap();

        // Segment 1 should be empty, segment 2 should still have data
        assert!(reader.match_query(1, "hello").unwrap().is_empty());
        assert!(!reader.match_query(2, "hello").unwrap().is_empty());
    }

    #[test]
    fn test_shared_text_index_phrase_match() {
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "default",
            "{}",
            1,
            50_000_000,
        )
        .unwrap();

        writer.add_text(1, 0, "hello world today").unwrap();
        writer.add_text(1, 1, "hello beautiful world").unwrap();
        writer.add_text(2, 0, "hello world").unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // Exact phrase match with slop=0
        let results = reader.phrase_match_query(1, "hello world", 0).unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0].into_iter().collect::<HashSet<u64>>());

        // Phrase match with slop=1 should also match "hello beautiful world"
        let results = reader.phrase_match_query(1, "hello world", 1).unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0, 1].into_iter().collect::<HashSet<u64>>());

        // Segment 2 should have its own results
        let results = reader.phrase_match_query(2, "hello world", 0).unwrap();
        assert_eq!(results, vec![0]);
    }

    #[test]
    fn test_shared_text_index_min_should_match() {
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "default",
            "{}",
            1,
            50_000_000,
        )
        .unwrap();

        writer.add_text(1, 0, "a b").unwrap();
        writer.add_text(1, 1, "a c").unwrap();
        writer.add_text(1, 2, "b c").unwrap();
        writer.add_text(1, 3, "a b c").unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // min=1: any token matches
        let results = reader.match_query_with_minimum(1, "a b", 1).unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0, 1, 2, 3].into_iter().collect::<HashSet<u64>>());

        // min=2: at least 2 tokens must match
        let results = reader.match_query_with_minimum(1, "a b c", 2).unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0, 1, 2, 3].into_iter().collect::<HashSet<u64>>());

        // min=3: all 3 tokens must match
        let results = reader.match_query_with_minimum(1, "a b c", 3).unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![3].into_iter().collect::<HashSet<u64>>());
    }

    #[test]
    fn test_shared_text_index_jieba() {
        let params = r#"{"tokenizer": "jieba"}"#;
        let mut writer = SharedTextIndexWriter::new(
            "text",
            "jieba",
            params,
            1,
            50_000_000,
        )
        .unwrap();

        writer.add_text(1, 0, "北京大学").unwrap();
        writer.add_text(1, 1, "清华大学").unwrap();
        writer.add_text(2, 0, "北京天气").unwrap();

        writer.commit().unwrap();

        let reader = writer.create_reader().unwrap();

        // Query segment 1 for "北京"
        let results = reader.match_query(1, "北京").unwrap();
        assert_eq!(results, vec![0]);

        // Query segment 2 for "北京"
        let results = reader.match_query(2, "北京").unwrap();
        assert_eq!(results, vec![0]);

        // Query segment 1 for "大学"
        let results = reader.match_query(1, "大学").unwrap();
        let result_set: HashSet<u64> = results.into_iter().collect();
        assert_eq!(result_set, vec![0, 1].into_iter().collect::<HashSet<u64>>());
    }
}
