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

#include "FilterBitsNode.h"
#include "common/Tracer.h"
#include "fmt/format.h"

#include "monitor/Monitor.h"
#include "exec/expression/ConjunctExpr.h"

namespace milvus {
namespace exec {
namespace {
bool
IsIndexOnlyTree(const std::shared_ptr<Expr>& node,
                const segcore::SegmentInternalInterface* segment) {
    if (!node) {
        return false;
    }
    if (node->name() == "PhyConjunctFilterExpr") {
        const auto& inputs =
            const_cast<std::shared_ptr<Expr>&>(node)->GetInputsRef();
        if (inputs.empty()) {
            return false;
        }
        for (const auto& in : inputs) {
            if (!IsIndexOnlyTree(in, segment)) {
                return false;
            }
        }
        return true;
    }
    if (!node->IsSource()) {
        return false;
    }
    auto* seg_expr = dynamic_cast<SegmentExpr*>(node.get());
    if (seg_expr == nullptr) {
        return false;
    }
    if (seg_expr->CanUseIndex()) {
        return true;
    }
    // Treat PK on sealed segment as index-capable (PK term path uses search_ids / sorted-by-pk fast path)
    if (segment && segment->type() == SegmentType::Sealed) {
        auto col_info_opt = node->GetColumnInfo();
        if (col_info_opt.has_value()) {
            auto col_info = col_info_opt.value();
            auto& schema = segment->get_schema();
            if (schema.get_primary_field_id().has_value() &&
                schema.get_primary_field_id().value() == col_info.field_id_) {
                return true;
            }
        }
    }
    return false;
}
}  // namespace

PhyFilterBitsNode::PhyFilterBitsNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::FilterBitsNode>& filter)
    : Operator(driverctx,
               filter->output_type(),
               operator_id,
               filter->id(),
               "PhyFilterBitsNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> filters;
    filter_expr_ = filter->filter();
    filters.emplace_back(filter_expr_);
    exprs_ = std::make_unique<ExprSet>(filters, exec_context);
    need_process_rows_ = query_context_->get_active_count();
    num_processed_rows_ = 0;
}

void
PhyFilterBitsNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

bool
PhyFilterBitsNode::AllInputProcessed() {
    if (num_processed_rows_ == need_process_rows_) {
        input_ = nullptr;
        return true;
    }
    return false;
}

bool
PhyFilterBitsNode::IsFinished() {
    return AllInputProcessed();
}

RowVectorPtr
PhyFilterBitsNode::GetOutput() {
    if (AllInputProcessed()) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhyFilterBitsNode::Execute", tracer::GetRootSpan(), true);
    tracer::AddEvent(fmt::format("input_rows: {}", need_process_rows_));

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();

    // Fast path: whole expression tree is index-only -> evaluate once with large batch
    {
        auto root = exprs_->expr(0);
        if (IsIndexOnlyTree(root, query_context_->get_segment())) {
            tracer::AddEvent("fast_path_index_only: true");
            auto active_count = query_context_->get_active_count();
            auto qcfg = std::make_shared<QueryConfig>(
                std::unordered_map<std::string, std::string>{
                    {QueryConfig::kExprEvalBatchSize,
                     fmt::format("{}", active_count)}});
            auto fast_qctx = std::make_shared<QueryContext>(
                query_context_->query_id(),
                query_context_->get_segment(),
                query_context_->get_active_count(),
                query_context_->get_query_timestamp(),
                query_context_->get_collection_ttl(),
                query_context_->get_consistency_level(),
                query_context_->get_plan_options(),
                qcfg);
            fast_qctx->set_op_context(query_context_->get_op_context());

            ExecContext fast_exec_ctx(fast_qctx.get());
            std::vector<expr::TypedExprPtr> filters;
            filters.emplace_back(filter_expr_);
            auto fast_exprs = std::make_unique<ExprSet>(filters, &fast_exec_ctx);
            EvalCtx fast_eval_ctx(&fast_exec_ctx, fast_exprs.get());
            std::vector<VectorPtr> fast_results;
            fast_exprs->Eval(0, 1, true, fast_eval_ctx, fast_results);

            AssertInfo(fast_results.size() == 1 && fast_results[0] != nullptr,
                       "PhyFilterBitsNode fast path result invalid");
            auto col_vec =
                std::dynamic_pointer_cast<ColumnVector>(fast_results[0]);
            AssertInfo(col_vec && col_vec->IsBitmap(),
                       "PhyFilterBitsNode fast path result should be bitmap");
            // mirror normal path semantics: flip to produce filter mask
            TargetBitmap bitset;
            TargetBitmapView view(col_vec->GetRawData(), col_vec->size());
            bitset.append(view);
            bitset.flip();
            TargetBitmap valid_bitset;
            TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                        col_vec->size());
            valid_bitset.append(valid_view);
            AssertInfo(bitset.size() == need_process_rows_,
                       "fast path bitset size: {}, need_process_rows_: {}",
                       bitset.size(),
                       need_process_rows_);
            num_processed_rows_ = need_process_rows_;
            std::vector<VectorPtr> col_res;
            col_res.push_back(std::make_shared<ColumnVector>(std::move(bitset),
                                                             std::move(valid_bitset)));
            std::chrono::high_resolution_clock::time_point scalar_end =
                std::chrono::high_resolution_clock::now();
            double scalar_cost =
                std::chrono::duration<double, std::micro>(scalar_end -
                                                          scalar_start)
                    .count();
            milvus::monitor::internal_core_search_latency_scalar.Observe(
                scalar_cost / 1000);
            tracer::AddEvent(
                fmt::format("fast_path_output_rows: {}", need_process_rows_));
            return std::make_shared<RowVector>(col_res);
        } else {
            tracer::AddEvent("fast_path_index_only: false");
        }
    }

    EvalCtx eval_ctx(operator_context_->get_exec_context(), exprs_.get());

    TargetBitmap bitset;
    TargetBitmap valid_bitset;
    while (num_processed_rows_ < need_process_rows_) {
        exprs_->Eval(0, 1, true, eval_ctx, results_);

        AssertInfo(results_.size() == 1 && results_[0] != nullptr,
                   "PhyFilterBitsNode result size should be size one and not "
                   "be nullptr");

        if (auto col_vec =
                std::dynamic_pointer_cast<ColumnVector>(results_[0])) {
            if (col_vec->IsBitmap()) {
                auto col_vec_size = col_vec->size();
                TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
                bitset.append(view);
                TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                            col_vec_size);
                valid_bitset.append(valid_view);
                num_processed_rows_ += col_vec_size;
            } else {
                ThrowInfo(ExprInvalid,
                          "PhyFilterBitsNode result should be bitmap");
            }
        } else {
            ThrowInfo(ExprInvalid,
                      "PhyFilterBitsNode result should be ColumnVector");
        }
    }
    bitset.flip();
    AssertInfo(bitset.size() == need_process_rows_,
               "bitset size: {}, need_process_rows_: {}",
               bitset.size(),
               need_process_rows_);
    Assert(valid_bitset.size() == need_process_rows_);

    auto filtered_count = bitset.count();
    auto filter_ratio =
        bitset.size() != 0 ? 1 - float(filtered_count) / bitset.size() : 0;
    milvus::monitor::internal_core_expr_filter_ratio.Observe(filter_ratio);
    // num_processed_rows_ = need_process_rows_;
    std::vector<VectorPtr> col_res;
    col_res.push_back(std::make_shared<ColumnVector>(std::move(bitset),
                                                     std::move(valid_bitset)));
    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    milvus::monitor::internal_core_search_latency_scalar.Observe(scalar_cost /
                                                                 1000);

    tracer::AddEvent(fmt::format("output_rows: {}, filtered: {}",
                                 need_process_rows_ - filtered_count,
                                 filtered_count));

    return std::make_shared<RowVector>(col_res);
}

}  // namespace exec
}  // namespace milvus
