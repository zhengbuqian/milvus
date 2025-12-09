use std::ffi::{c_char, c_void, CStr};

use crate::array::RustResult;
use crate::cstr_to_str;
use crate::log::init_log;
use crate::shared_text_index::{SharedTextIndexReader, SharedTextIndexWriter};
use crate::util::create_binding;
use crate::analyzer::create_analyzer;

// ==================== Writer APIs ====================

#[no_mangle]
pub extern "C" fn tantivy_create_shared_text_writer(
    field_name: *const c_char,
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
    num_threads: usize,
    overall_memory_budget_in_bytes: usize,
) -> RustResult {
    init_log();
    let field_name_str = cstr_to_str!(field_name);
    let tokenizer_name_str = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);

    match SharedTextIndexWriter::new(
        field_name_str,
        tokenizer_name_str,
        params,
        num_threads,
        overall_memory_budget_in_bytes,
    ) {
        Ok(writer) => RustResult::from_ptr(create_binding(writer)),
        Err(err) => RustResult::from_error(format!(
            "create shared text writer failed: {} param: {}",
            err.to_string(),
            params,
        )),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_add_text(
    ptr: *mut c_void,
    segment_id: u64,
    local_doc_id: u64,
    text: *const c_char,
) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    let text_str = cstr_to_str!(text);
    unsafe {
        match (*writer).add_text(segment_id, local_doc_id, text_str) {
            Ok(_) => RustResult::from_ptr(std::ptr::null_mut()),
            Err(err) => RustResult::from_error(err.to_string()),
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_add_texts(
    ptr: *mut c_void,
    segment_id: u64,
    texts: *const *const c_char,
    num_texts: usize,
    offset_begin: u64,
) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    unsafe {
        for i in 0..num_texts {
            let text_ptr = *texts.add(i);
            if text_ptr.is_null() {
                continue;
            }
            let text_str = match CStr::from_ptr(text_ptr).to_str() {
                Ok(s) => s,
                Err(e) => return RustResult::from_error(format!("invalid utf8: {}", e)),
            };
            if let Err(err) = (*writer).add_text(segment_id, offset_begin + i as u64, text_str) {
                return RustResult::from_error(err.to_string());
            }
        }
        RustResult::from_ptr(std::ptr::null_mut())
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_delete_segment(
    ptr: *mut c_void,
    segment_id: u64,
) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    unsafe {
        match (*writer).delete_segment(segment_id) {
            Ok(_) => RustResult::from_ptr(std::ptr::null_mut()),
            Err(err) => RustResult::from_error(err.to_string()),
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_commit(ptr: *mut c_void) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    unsafe {
        match (*writer).commit() {
            Ok(_) => RustResult::from_ptr(std::ptr::null_mut()),
            Err(err) => RustResult::from_error(err.to_string()),
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_create_reader(ptr: *mut c_void) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    unsafe {
        match (*writer).create_reader() {
            Ok(reader) => RustResult::from_ptr(create_binding(reader)),
            Err(err) => RustResult::from_error(err.to_string()),
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_writer_register_tokenizer(
    ptr: *mut c_void,
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
) -> RustResult {
    let writer = ptr as *mut SharedTextIndexWriter;
    let tokenizer_name_str = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);

    match create_analyzer(params) {
        Ok(tokenizer) => unsafe {
            (*writer).register_tokenizer(tokenizer_name_str, tokenizer);
            RustResult::from_ptr(std::ptr::null_mut())
        },
        Err(err) => RustResult::from_error(err.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_shared_text_writer(ptr: *mut c_void) {
    if !ptr.is_null() {
        let _ = std::panic::catch_unwind(|| {
            unsafe {
                drop(Box::from_raw(ptr as *mut SharedTextIndexWriter));
            }
        });
    }
}

// ==================== Reader APIs ====================

#[no_mangle]
pub extern "C" fn tantivy_shared_text_reader_reload(ptr: *mut c_void) -> RustResult {
    let reader = ptr as *mut SharedTextIndexReader;
    unsafe {
        match (*reader).reload() {
            Ok(_) => RustResult::from_ptr(std::ptr::null_mut()),
            Err(err) => RustResult::from_error(err.to_string()),
        }
    }
}

/// Match query and return local_doc_ids as array
/// The caller is responsible for freeing the returned array using tantivy_free_u64_array
#[no_mangle]
pub extern "C" fn tantivy_shared_text_reader_match_query(
    ptr: *mut c_void,
    segment_id: u64,
    query: *const c_char,
    result_len: *mut usize,
) -> RustResult {
    let reader = ptr as *mut SharedTextIndexReader;
    let query_str = cstr_to_str!(query);
    unsafe {
        match (*reader).match_query(segment_id, query_str) {
            Ok(results) => {
                *result_len = results.len();
                if results.is_empty() {
                    RustResult::from_ptr(std::ptr::null_mut())
                } else {
                    let boxed = results.into_boxed_slice();
                    let ptr = Box::into_raw(boxed) as *mut u64;
                    RustResult::from_ptr(ptr as *mut c_void)
                }
            }
            Err(err) => {
                *result_len = 0;
                RustResult::from_error(err.to_string())
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_reader_match_query_with_minimum(
    ptr: *mut c_void,
    segment_id: u64,
    query: *const c_char,
    min_should_match: usize,
    result_len: *mut usize,
) -> RustResult {
    let reader = ptr as *mut SharedTextIndexReader;
    let query_str = cstr_to_str!(query);
    unsafe {
        match (*reader).match_query_with_minimum(segment_id, query_str, min_should_match) {
            Ok(results) => {
                *result_len = results.len();
                if results.is_empty() {
                    RustResult::from_ptr(std::ptr::null_mut())
                } else {
                    let boxed = results.into_boxed_slice();
                    let ptr = Box::into_raw(boxed) as *mut u64;
                    RustResult::from_ptr(ptr as *mut c_void)
                }
            }
            Err(err) => {
                *result_len = 0;
                RustResult::from_error(err.to_string())
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_reader_phrase_match_query(
    ptr: *mut c_void,
    segment_id: u64,
    query: *const c_char,
    slop: u32,
    result_len: *mut usize,
) -> RustResult {
    let reader = ptr as *mut SharedTextIndexReader;
    let query_str = cstr_to_str!(query);
    unsafe {
        match (*reader).phrase_match_query(segment_id, query_str, slop) {
            Ok(results) => {
                *result_len = results.len();
                if results.is_empty() {
                    RustResult::from_ptr(std::ptr::null_mut())
                } else {
                    let boxed = results.into_boxed_slice();
                    let ptr = Box::into_raw(boxed) as *mut u64;
                    RustResult::from_ptr(ptr as *mut c_void)
                }
            }
            Err(err) => {
                *result_len = 0;
                RustResult::from_error(err.to_string())
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivy_shared_text_reader_register_tokenizer(
    ptr: *mut c_void,
    tokenizer_name: *const c_char,
    analyzer_params: *const c_char,
) -> RustResult {
    let reader = ptr as *mut SharedTextIndexReader;
    let tokenizer_name_str = cstr_to_str!(tokenizer_name);
    let params = cstr_to_str!(analyzer_params);

    match create_analyzer(params) {
        Ok(tokenizer) => unsafe {
            (*reader).register_tokenizer(tokenizer_name_str, tokenizer);
            RustResult::from_ptr(std::ptr::null_mut())
        },
        Err(err) => RustResult::from_error(err.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_shared_text_reader(ptr: *mut c_void) {
    if !ptr.is_null() {
        let _ = std::panic::catch_unwind(|| {
            unsafe {
                drop(Box::from_raw(ptr as *mut SharedTextIndexReader));
            }
        });
    }
}

#[no_mangle]
pub extern "C" fn tantivy_free_u64_array(ptr: *mut u64, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        }
    }
}
