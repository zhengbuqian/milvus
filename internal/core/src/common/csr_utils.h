// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <vector>
#include <string.h>
#include <csignal>

#include "pb/schema.pb.h"
#include "common/EasyAssert.h"


// TODO(SPARSE): current impl is not efficient.
namespace milvus::sparse {

static inline int64_t csr_byte_size(int32_t rows, int64_t nnz) {
    return nnz * (sizeof(float) + sizeof(int32_t)) + (rows + 4) * sizeof(int32_t);
}

static inline void print_csr(const void* csr) {
    const int32_t* int_data = static_cast<const int32_t*>(csr);
    int32_t rows = int_data[0];
    int32_t dim = int_data[1];
    int32_t nnz = int_data[2];
    // for debugging purpose, do not print large CSR
    if (rows > 30 || dim > 30) {
        return;
    }

    const int32_t* indptr = &int_data[3];
    const int32_t* indices = &int_data[3 + rows + 1];
    const float* values = reinterpret_cast<const float*>(&int_data[3 + rows + 1 + nnz]);

    std::cout << "Rows: " << rows << ", Dim: " << dim << ", NNZ: " << nnz << std::endl;

    std::cout << "Indptr: ";
    for (int32_t i = 0; i <= rows; ++i) {
        std::cout << indptr[i] << " ";
    }
    std::cout << std::endl;

    std::cout << "Indices: ";
    for (int32_t i = 0; i < nnz; ++i) {
        std::cout << indices[i] << " ";
    }
    std::cout << std::endl;

    std::cout << "Values: ";
    for (int32_t i = 0; i < nnz; ++i) {
        std::cout << values[i] << " ";
    }
    std::cout << std::endl;

    for (int32_t row = 0; row < rows; ++row) {
        int32_t start = indptr[row];
        int32_t end = indptr[row + 1];
        int32_t current_index = 0;

        for (int32_t j = start; j < end; ++j) {
            for (; current_index < indices[j]; ++current_index) {
                std::cout << "0 ";
            }
            std::cout << values[j] << " ";
            ++current_index;
        }
        for (; current_index < dim; ++current_index) {
            std::cout << "0 ";
        }
        std::cout << std::endl;
    }
}

static inline bool validate_csr(const void* csr_bytes, int64_t length) {
    const int32_t* int_data = static_cast<const int32_t*>(csr_bytes);
    int32_t rows = int_data[0];
    int32_t dim = int_data[1];
    int32_t nnz = int_data[2];

    int64_t expected_length = (3 + (rows + 1) + nnz) * sizeof(int32_t) + nnz * sizeof(float);
    if (length != expected_length) {
        std::cerr << "Invalid length: expected " << expected_length << ", got " << length << std::endl;
        print_csr(csr_bytes);
        return false;
    }

    const int32_t* indptr = &int_data[3];
    const int32_t* indices = &int_data[3 + rows + 1];
    const float* values = reinterpret_cast<const float*>(&int_data[3 + rows + 1 + nnz]);

    // Verify indptr[0] == 0 and indptr[i] <= indptr[i + 1]
    if (indptr[0] != 0) {
        std::cerr << "Invalid indptr: first element is not zero" << std::endl;
        print_csr(csr_bytes);
        return false;
    }
    if (indptr[row] != nnz) {
        std::cerr << "Invalid indptr: last element is not nnz" << std::endl;
        print_csr(csr_bytes);
        return false;
    }
    for (int32_t i = 0; i < rows; ++i) {
        if (indptr[i] > indptr[i + 1]) {
            std::cerr << "Invalid indptr: not non-decreasing at position " << i << std::endl;
            print_csr(csr_bytes);
            return false;
        }
    }

    // Verify no indices is greater than or equal to dim
    for (int32_t i = 0; i < nnz; ++i) {
        if (indices[i] >= dim || indices[i] < 0) {
            std::cerr << "Invalid index at position " << i << ": out of bounds" << std::endl;
            print_csr(csr_bytes);
            return false;
        }
    }

    // Verify no duplicate indices in any row
    for (int32_t i = 0; i < rows; ++i) {
        for (int32_t j = indptr[i]; j < indptr[i + 1] - 1; ++j) {
            if (indices[j] >= indices[j + 1]) {
                std::cerr << "Invalid indices: duplicate or unsorted indices in row " << i << std::endl;
                print_csr(csr_bytes);
                return false;
            }
        }
    }

    return true;
}

class SparseMatrix {
 public:
    SparseMatrix() {}

    SparseMatrix(const void* data) : SparseMatrix() {
        append(data);
    }

    SparseMatrix(SparseMatrix&& other) noexcept
        : dim_(other.dim_),
          nnz_(other.nnz_),
          contents_(std::move(other.contents_)) {
    }

    // convert and append CSR bytes to the matrix
    void append(const void* input) {
        int32_t rows, dim, nnz;
        const int32_t* indptr;
        const int32_t* indices;
        auto ptr = static_cast<const int32_t*>(input);
        rows = *ptr++;
        dim = *ptr++;
        nnz = *ptr++;
        AssertInfo(validate_csr(input, csr_byte_size(rows, nnz)), "SparseMatrix append from raw pointer: invalid input CSR");
        indptr = ptr;
        ptr += rows + 1;
        indices = ptr;
        ptr += nnz;
        const float* data = static_cast<const float*>(static_cast<const void*>(ptr));
        dim_ = std::max(dim_, dim);
        nnz_ += nnz;
        for (int i = 0; i < rows; ++i) {
            contents_.emplace_back();
            auto& row = contents_.back();
            int32_t row_dim = 0;
            for (int j = indptr[i]; j < indptr[i + 1]; ++j) {
                row.data_.emplace_back(data[j], indices[j]);
            }
            row.dim_ = row_dim + 1;
        }
    }

    void append(const::milvus::proto::schema::SparseFloatArray& target) {
        dim_ = std::max(dim_, target.dim());
        nnz_ += target.nnz();
        for (int i = 0; i < target.contents().size(); ++i) {
            contents_.emplace_back();
            auto& row = contents_.back();
            int32_t row_dim = 0;
            for (int j = 0; j < target.contents(i).values().data().size(); ++j) {
                row.data_.emplace_back(target.contents(i).values().data(j), target.contents(i).indices().data(j));
            }
            row.dim_ = row_dim + 1;
        }
    }

    void append(const SparseMatrix& other) {
        dim_ = std::max(dim_, other.dim_);
        nnz_ += other.nnz_;
        for (auto& row : other.contents_) {
            contents_.emplace_back();
            contents_.back().data_.insert(contents_.back().data_.end(), row.data_.begin(), row.data_.end());
            contents_.back().dim_ = row.dim_;
        }
    }

    // caller is responsible for freeing the memory
    const void* to_bytes() const {
        auto size = csr_byte_size(rows(), nnz_);
        auto res = new char[size];
        int32_t* ptr = static_cast<int32_t*>(static_cast<void*>(res));
        *ptr++ = rows();
        *ptr++ = dim_;
        *ptr++ = nnz_;

        int32_t* indptr = ptr;
        int32_t* indices = indptr + rows() + 1;
        float* data = static_cast<float*>(static_cast<void*>(indices + nnz_));

        indptr[0] = 0;
        for (int i = 0; i < rows(); ++i) {
            auto& row = contents_[i];
            indptr[i + 1] = indptr[i] + row.data_.size();
            for (auto& p : row.data_) {
                *indices++ = p.second;
                *data++ = p.first;
            }
        }
        return res;
    }

    bool empty() const {
        return rows() == 0;
    }

    int64_t size() const {
        return csr_byte_size(rows(), nnz_);
    }

    // returns the CSR byte size of a single row
    int64_t size(ssize_t offset) const {
        // count shape and first of indptr towards the first row
        auto s = sizeof(int32_t) + (sizeof(int32_t) + sizeof(float)) * (contents_[offset].data_.size());
        if (offset == 0) {
            s += 4 * sizeof(int32_t);
        }
        return s;
    }

    int64_t rows() const {
        return contents_.size();
    }

    int64_t dim() const {
        return dim_;
    }

 private:
    struct Row {
        int32_t dim_ = 0;
        std::vector<std::pair<float, int32_t>> data_;
    };

    int32_t dim_ = 0;
    int32_t nnz_ = 0;
    std::vector<Row> contents_;
};

}  // namespace milvus::sparse
