// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "indexbuilder/BuildDriver.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"

namespace milvus::indexbuilder {

// Family-local construction keeps VECTOR_ARRAY's compact parent views out of
// the shared IndexBuilder contract. VECTOR_ARRAY side inputs remain an
// explicit pre-I/O failure in this checkpoint; Disk materialization is
// delivered through the family-local bridge below.
BuildDriverPtr
MakeVectorBuildDriver(DataType field_type,
                      DataType value_type,
                      const index::IndexFamily& family,
                      const index::BuildParams& params);

// Family-local side-input bridge. These functions accept only an ordinary
// vector driver that declared side_inputs and are valid after its primary feed
// completes and before Seal consumes it.
const VectorPrimaryLayout&
GetVectorPrimaryLayout(const BuildDriver& driver);

void
DeliverVectorScalarInfo(BuildDriver& driver, VectorScalarInfo scalar_info);

void
DeliverVectorDiskInputs(BuildDriver& driver, VectorDiskBuildInputs inputs);

void
ValidateVectorDiskInputDriver(const BuildDriver& driver);

}  // namespace milvus::indexbuilder
