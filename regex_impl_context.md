# Regex Implementation Context

## What's already done
- `plan.proto`: `RegexMatch = 16` added to OpType enum
- Proto regenerated: `pkg/proto/planpb/plan.pb.go` has `OpType_RegexMatch`
- `Plan.g4`: `REGEXMATCH: '=~'` and `REGEXNOTMATCH: '!~'` tokens + grammar rules added
- ANTLR regenerated: `generated/plan_parser.go`, `plan_visitor.go`, `plan_base_visitor.go` have RegexMatch/RegexNotMatch contexts

## Design decisions
- **Substring matching**: use `RE2::PartialMatch` (not FullMatch)
- `!~` is syntactic sugar: parser wraps result in NOT expression
- NULL field → NULL result (SQL standard)
- Case-insensitive `(?i)` → skip ngram optimization, brute force
- Regex-to-LIKE optimization: deferred to later phase

## Key reference: how LIKE works (follow same pattern)
- Grammar: `expr LIKE StringLiteral # Like` (Plan.g4:19)
- Go visitor: `VisitLike()` at `parser_visitor.go:489` builds `UnaryRangeExpr`
- C++ UnaryCompare: `case proto::plan::Match:` at `UnaryExpr.h:80`
- C++ UnaryElementFuncForMatch: `UnaryExpr.h:100` — pre-builds LikePatternMatcher
- C++ dispatch in UnaryExpr.cpp: `case proto::plan::Match:` at lines 493, 948, 1473, 1709
- C++ IsLikeExpr: `Expr.cpp:461`
- C++ CanUseIndexForOp: `Expr.h:~2007`
- C++ RegexMatcher (FullMatch): `RegexQuery.h:48`
- C++ Types.h formatter: `Types.h:~1036`
- C++ NgramInvertedIndex: `NgramInvertedIndex.cpp` — split_by_wildcard, CanHandleLiteral, ExecutePhase1, ExecutePhase2

## Testing
- Go tests: `go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/parser/planparserv2/ -run TestExpr_RegexMatch`
- C++ build: `cd /home/zilliz/milvus/internal/core && ./compile.sh compile`
- C++ UT: `cd /home/zilliz/milvus/internal/core && ./compile.sh runcpput "TestName"`
