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

// Registration entry point for the vector families.
//
// See core_refactor/01-scalar-index.md §11.2 rule 4 ("split the factory by
// family: family-level loader/builder registries replace `IndexFactory`'s God
// switch") and §6.2 (`IndexLoader::Family()`).
//
// Canonical registry keys are `families::kVectorMem` and
// `families::kVectorDisk` from `index/Families.h`; knowhere index-type
// spellings remain constructor parameters, not dynamic registry families.
// `VectorFamilies.cpp` registers each stateless loader once and the implemented
// disk builder once per supported physical value type. The memory builder is
// intentionally not registered until its input convention is complete.
