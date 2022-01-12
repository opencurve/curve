/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Date: Mon Sep  6 16:18:16 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/test/utils/protobuf_message_utils.h"

#include <limits>
#include <memory>
#include <string>

namespace curvefs {
namespace test {

std::unique_ptr<Message> GenerateAnDefaultInitializedMessage(
    const std::string& name) {
    const google::protobuf::Descriptor* desc =
        google::protobuf::DescriptorPool::generated_pool()
            ->FindMessageTypeByName(name);

    if (!desc) {
        return nullptr;
    }

    std::unique_ptr<google::protobuf::Message> message(
        google::protobuf::MessageFactory::generated_factory()
            ->GetPrototype(desc)
            ->New());

    const auto* refl = message->GetReflection();

    for (int i = 0; i < desc->field_count(); ++i) {
        auto* fieldDesc = desc->field(i);

        if (!fieldDesc->is_required()) {
            continue;
        }

        switch (fieldDesc->type()) {
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                refl->SetDouble(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                refl->SetFloat(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_INT64:
                refl->SetInt64(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
                refl->SetUInt64(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_INT32:
                refl->SetInt32(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                refl->SetUInt64(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                refl->SetUInt32(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
                refl->SetBool(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_STRING:
                refl->SetString(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_GROUP:
                return message;
                break;
            case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
                {
                    auto* subMessage = refl->MutableMessage(message.get(), fieldDesc);  // NOLINT
                    auto generatedSubMessage = GenerateAnDefaultInitializedMessage(fieldDesc->message_type()->full_name());  // NOLINT
                    if (!generatedSubMessage ||
                        !generatedSubMessage->IsInitialized()) {
                        return message;
                    }
                    subMessage->CopyFrom(*(generatedSubMessage.get()));
                }
                break;
            case google::protobuf::FieldDescriptor::TYPE_BYTES:
                return message;
                break;
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
                refl->SetUInt32(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_ENUM: {
                auto* enumDesc = fieldDesc->enum_type();
                const google::protobuf::EnumValueDescriptor* valueDesc =
                    nullptr;
                for (int i = 0; i < std::numeric_limits<int>::max(); ++i) {
                    valueDesc = enumDesc->FindValueByNumber(i);
                    if (valueDesc) {
                        break;
                    }
                }
                if (!valueDesc) {
                    return message;
                }
                refl->SetEnum(message.get(), fieldDesc, valueDesc);
            } break;
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                refl->SetInt32(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                refl->SetInt64(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
                refl->SetInt32(message.get(), fieldDesc, {});
                break;
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
                refl->SetInt64(message.get(), fieldDesc, {});
                break;
            default:
                return message;
        }
    }

    return message;
}

}  // namespace test
}  // namespace curvefs
