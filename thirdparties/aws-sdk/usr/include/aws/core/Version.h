/*
  * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License").
  * You may not use this file except in compliance with the License.
  * A copy of the License is located at
  *
  *  http://aws.amazon.com/apache2.0
  *
  * or in the "license" file accompanying this file. This file is distributed
  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
namespace Version
{
    AWS_CORE_API const char* GetVersionString();
    AWS_CORE_API unsigned GetVersionMajor();
    AWS_CORE_API unsigned GetVersionMinor();
    AWS_CORE_API unsigned GetVersionPatch();
    AWS_CORE_API const char* GetCompilerVersionString();
} //namespace Version
} //namespace Aws
