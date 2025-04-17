#pragma once

#include <memory>

#include "common/Channel.h"
#include "parquet/arrow/reader.h"

namespace milvus {

struct ArrowDataWrapper {
    ArrowDataWrapper() = default;
    ArrowDataWrapper(std::shared_ptr<arrow::RecordBatchReader> reader,
                     std::shared_ptr<parquet::arrow::FileReader> arrow_reader,
                     std::shared_ptr<uint8_t[]> file_data)
        : reader(std::move(reader)),
          arrow_reader(std::move(arrow_reader)),
          file_data(std::move(file_data)) {
    }
    std::shared_ptr<arrow::RecordBatchReader> reader;
    // file reader must outlive the record batch reader
    std::shared_ptr<parquet::arrow::FileReader> arrow_reader;
    // underlying file data memory, must outlive the arrow reader
    std::shared_ptr<uint8_t[]> file_data;
    std::vector<std::shared_ptr<arrow::Table>> arrow_tables;
};
using ArrowReaderChannel = Channel<std::shared_ptr<milvus::ArrowDataWrapper>>;

}  // namespace milvus
