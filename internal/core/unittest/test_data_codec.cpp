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

#include <gtest/gtest.h>

#include "storage/DataCodec.h"
#include "storage/InsertData.h"
#include "storage/IndexData.h"
#include "storage/Util.h"
#include "common/Consts.h"
#include "common/Json.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

using namespace milvus;

TEST(storage, InsertDataBool) {
    FixedVector<bool> data = {true, false, true, false, true};
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::BOOL);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::BOOL);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<bool> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt8) {
    FixedVector<int8_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT8);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT8);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt16) {
    FixedVector<int16_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT16);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int16_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt32) {
    FixedVector<int32_t> data = {true, false, true, false, true};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT32);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT32);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int32_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataInt64) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::INT64);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<int64_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataString) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VARCHAR);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VARCHAR);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<std::string> new_data(data.size());
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] =
            *static_cast<const std::string*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->Size(i), data[i].size());
    }
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloat) {
    FixedVector<float> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::FLOAT);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<float> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataDouble) {
    FixedVector<double> data = {1.0, 2.0, 3.0, 4.2, 5.3};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::DOUBLE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::DOUBLE);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<double> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloatVector) {
    std::vector<float> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VECTOR_FLOAT, DIM);
    field_data->FillFieldData(data.data(), data.size() / DIM);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() / DIM);
    std::vector<float> new_data(data.size());
    memcpy(new_data.data(),
           new_payload->Data(),
           new_payload->get_num_rows() * sizeof(float) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataSparseFloat) {
    auto n_rows = 100;
    auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
        n_rows, kTestSparseDim, kTestSparseVectorDensity);
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_SPARSE_FLOAT, kTestSparseDim, n_rows);
    field_data->FillFieldData(vecs.get(), n_rows);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_TRUE(new_payload->get_data_type() ==
                storage::DataType::VECTOR_SPARSE_FLOAT);
    ASSERT_EQ(new_payload->get_num_rows(), n_rows);
    auto new_data = static_cast<const knowhere::sparse::SparseRow<float>*>(
        new_payload->Data());

    for (auto i = 0; i < n_rows; ++i) {
        auto& original = vecs[i];
        auto& new_vec = new_data[i];
        ASSERT_EQ(original.size(), new_vec.size());
        for (auto j = 0; j < original.size(); ++j) {
            ASSERT_EQ(original[j].id, new_vec[j].id);
            ASSERT_EQ(original[j].val, new_vec[j].val);
        }
    }
}

TEST(storage, InsertDataBinaryVector) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 16;
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VECTOR_BINARY, DIM);
    field_data->FillFieldData(data.data(), data.size() * 8 / DIM);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_BINARY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() * 8 / DIM);
    std::vector<uint8_t> new_data(data.size());
    memcpy(new_data.data(), new_payload->Data(), new_payload->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataFloat16Vector) {
    std::vector<float16> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_FLOAT16, DIM);
    field_data->FillFieldData(data.data(), data.size() / DIM);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_FLOAT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() / DIM);
    std::vector<float16> new_data(data.size());
    memcpy(new_data.data(),
           new_payload->Data(),
           new_payload->get_num_rows() * sizeof(float16) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataBFloat16Vector) {
    std::vector<bfloat16> data = {1, 2, 3, 4, 5, 6, 7, 8};
    int DIM = 2;
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_BFLOAT16, DIM);
    field_data->FillFieldData(data.data(), data.size() / DIM);

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::VECTOR_BFLOAT16);
    ASSERT_EQ(new_payload->get_num_rows(), data.size() / DIM);
    std::vector<bfloat16> new_data(data.size());
    memcpy(new_data.data(),
           new_payload->Data(),
           new_payload->get_num_rows() * sizeof(bfloat16) * DIM);
    ASSERT_EQ(data, new_data);
}

TEST(storage, IndexData) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8};
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT8);
    field_data->FillFieldData(data.data(), data.size());

    storage::IndexData index_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    index_data.SetFieldDataMeta(field_data_meta);
    index_data.SetTimestamps(0, 100);
    storage::IndexMeta index_meta{102, 103, 104, 1};
    index_data.set_index_meta(index_meta);

    auto serialized_bytes = index_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_index_data = storage::DeserializeFileData(serialized_data_ptr,
                                                       serialized_bytes.size());
    ASSERT_EQ(new_index_data->GetCodecType(), storage::IndexDataType);
    ASSERT_EQ(new_index_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_field_data = new_index_data->GetFieldData();
    ASSERT_EQ(new_field_data->get_data_type(), storage::DataType::INT8);
    ASSERT_EQ(new_field_data->Size(), data.size());
    std::vector<uint8_t> new_data(data.size());
    memcpy(new_data.data(), new_field_data->Data(), new_field_data->Size());
    ASSERT_EQ(data, new_data);
}

TEST(storage, InsertDataStringArray) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test_array1");
    field_string_data.mutable_string_data()->add_data("test_array2");
    field_string_data.mutable_string_data()->add_data("test_array3");
    field_string_data.mutable_string_data()->add_data("test_array4");
    field_string_data.mutable_string_data()->add_data("test_array5");
    auto string_array = Array(field_string_data);
    FixedVector<Array> data = {string_array};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertData insert_data(field_data);
    storage::FieldDataMeta field_data_meta{100, 101, 102, 103};
    insert_data.SetFieldDataMeta(field_data_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::StorageType::Remote);
    std::shared_ptr<uint8_t[]> serialized_data_ptr(serialized_bytes.data(),
                                                   [&](uint8_t*) {});
    auto new_insert_data = storage::DeserializeFileData(
        serialized_data_ptr, serialized_bytes.size());
    ASSERT_EQ(new_insert_data->GetCodecType(), storage::InsertDataType);
    ASSERT_EQ(new_insert_data->GetTimeRage(),
              std::make_pair(Timestamp(0), Timestamp(100)));
    auto new_payload = new_insert_data->GetFieldData();
    ASSERT_EQ(new_payload->get_data_type(), storage::DataType::ARRAY);
    ASSERT_EQ(new_payload->get_num_rows(), data.size());
    FixedVector<Array> new_data(data.size());
    for (int i = 0; i < data.size(); ++i) {
        new_data[i] = *static_cast<const Array*>(new_payload->RawValue(i));
        ASSERT_EQ(new_payload->Size(i), data[i].byte_size());
        ASSERT_TRUE(data[i].operator==(new_data[i]));
    }
}
