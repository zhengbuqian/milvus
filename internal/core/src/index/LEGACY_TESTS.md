# 遗留测试：保留作参考，暂不删除

这 18 个 `*Test.cpp` 测的是阶段 1 已经退役的公开接口（`IndexBase`、`ScalarIndex<T>` 的 24 个 virtual、
`IndexFactory` 的巨型分派 switch、`InApplyFilter`/`InApplyCallback`、`HybridScalarIndex` 运行时转发类）。
它们**编译不过**——被测的头文件已经不存在了。

保留它们是有意的：重写测试时，这批文件是"这一类索引到底要保证什么"的唯一完整记录。设计文档
[§6.2](../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md#62-loader-与-io-注入) 与
[§9](../../../../docs/design-docs/design_docs/core_refactor/01-scalar-index.md#9-消费者对接) 要求每种索引类型一张
往返一致性测试矩阵（Builder → Artifact → Serialize → Loader → Reader，多实现逐位一致），那份矩阵还没写。
在对应索引类型的新测试写完之前，不要删这里的任何一个文件。

## 旧测试 → 新的索引类型目录

| 遗留测试 | 覆盖的旧类 | 新的所属组件 |
|---|---|---|
| `BitmapIndexTest.cpp` | `BitmapIndex` | `scalar/bitmap/` |
| `BitmapIndexArrayTest.cpp` | `BitmapIndex` + sort/string-sort 的 nested 路径 | `scalar/bitmap/`、`scalar/sort/`（元素级，见 §5.8） |
| `BoolIndexTest.cpp` | `BoolIndex`（类型别名）、`ScalarIndexSort` | `scalar/bitmap/` |
| `ScalarIndexSortTest.cpp` | `ScalarIndexSort` | `scalar/sort/` |
| `StringIndexSortTest.cpp` | `StringIndexSort`（自带 pImpl 与带版本二进制格式） | `scalar/sort/` |
| `StringIndexTest.cpp` | `StringIndexMarisa` | `scalar/marisa/` |
| `InvertedIndexTest.cpp` | `InvertedIndexTantivy` | `scalar/inverted/` |
| `InvertedIndexArrayTest.cpp` | `InvertedIndexTantivy` 的 nested 路径 | `scalar/inverted/` |
| `NgramInvertedIndexTest.cpp` | `NgramInvertedIndex`（含已删除的 Phase2） | `scalar/ngram/` + exec 侧 refine |
| `TextMatchIndexTest.cpp` | `TextMatchIndex` 四个构造函数 | `scalar/text/` + `growing/` |
| `FMIndexTest.cpp` | `FMIndex`（含 `ShouldUseOp` 代价护栏） | `scalar/fmindex/` |
| `RTreeIndexTest.cpp` | `RTreeIndex` | `scalar/spatial/` |
| `RTreeIndexWrapperTest.cpp` | `RTreeIndexWrapper`（已按 build/query 两模式拆开） | `scalar/spatial/` |
| `JsonFlatIndexTest.cpp` | `JsonFlatIndex` | `scalar/json/` |
| `JsonIndexTest.cpp` | `JsonScalarIndexWrapper` | `scalar/json/` |
| `JsonPathIndexTest.cpp` | `JsonHybridScalarIndex` + path cast index | `scalar/json/`、`scalar/auto/` |
| `HybridScalarIndexTest.cpp` | `HybridScalarIndex` 运行时转发 | `scalar/auto/`——语义变了：选型成为构建期决策，运行时不再有转发对象可测（§6.3） |
| `ScalarIndexTest.cpp` | 跨索引类型：`ScalarIndex<T>` 的公共接口 | 无单一所属组件；它测的那层接口定义已按 §5 拆成各最小接口 |

## 重写时要注意的三处语义变化

1. **`HybridScalarIndexTest` 无法直接改写。** 旧测试断言的是"运行时按基数转发到 bitmap 或 inverted"，
   而新设计里 `auto` 这一类索引在 `Seal()` 之后就不存在了——测试对象变成"Builder 选型是否正确 + Loader 是否
   打开了选中的那一类"，断言点从运行时移到构建期产物元数据。
2. **`NgramInvertedIndexTest` 里覆盖 Phase2 的用例属于 exec，不属于索引。** 索引侧只需断言候选是超集
   （`ReaderCaps.exact == false`），精确验证的用例迁到 exec 的 refine 路径（与 geometry 同构，见 §5.4）。
3. **凡断言 `In`/`Range` 返回 bitmap 尺寸的用例，注意 `Count()` 现在是 reader 自身坐标系的基数**
   （§4.2）：元素级索引返回的是元素总数，不是行数。旧测试里隐含"尺寸 == 行数"的断言在 nested
   索引类型上不再成立。
