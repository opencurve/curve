/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/event/EventStreamBuf.h>
#include <aws/core/utils/stream/ConcurrentStreamBuf.h>
#include <aws/core/utils/event/EventMessage.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/event/EventStreamEncoder.h>
#include <atomic>

namespace Aws
{
    namespace Client
    {
        class AWSAuthSigner;
    }
    namespace Utils
    {
        namespace Event
        {
            /**
             * A buffered I/O stream that binary-decodes the bits written to it according to the AWS event-stream spec.
             * The decoding process will result in invoking callbacks on the handler assigned to the decoder parameter.
             */
            class AWS_CORE_API EventStream : public Aws::IOStream
            {
            public:
                /**
                 * Instantiates the stream in decoding mode
                 * @param decoder decodes the stream from server side, so as to invoke related callback functions.
                 * @param eventStreamBufLength The length of the underlying buffer.
                 */
                EventStream(EventStreamDecoder& decoder, size_t bufferSize = Utils::Event::DEFAULT_BUF_SIZE);

            private:
                EventStream(const EventStream&) = delete;
                EventStream(EventStream&&) = delete;
                EventStream& operator=(const EventStream&) = delete;
                EventStream& operator=(EventStream&&) = delete;

                EventStreamBuf m_eventStreamBuf;
            };

            /**
             * A buffered I/O stream that binary-encodes the bits written to it according to the AWS event-stream spec.
             */
            class AWS_CORE_API EventEncoderStream : public Aws::IOStream
            {
            public:

                EventEncoderStream(size_t bufferSize = Aws::Utils::Event::DEFAULT_BUF_SIZE);

                /**
                 * Sets the signature seed used by event-stream events.
                 * Every event uses its previous event's signature to calculate its own signature.
                 * Setting this value affects the signature calculation of the first event.
                 */
                void SetSignatureSeed(const Aws::String& seed) { m_encoder.SetSignatureSeed(seed); }

                /**
                 * Writes an event-stream message to the underlying buffer.
                 */
                EventEncoderStream& WriteEvent(const Aws::Utils::Event::Message& msg);

                /**
                 * Sets the signer implementation used for every event.
                 */
                void SetSigner(Aws::Client::AWSAuthSigner* signer) { m_encoder.SetSigner(signer); }

                /**
                 * Allows a stream writer to communicate the end of the stream to a stream reader.
                 *
                 * Any writes to the stream after this call are not guaranteed to be read by another concurrent
                 * read thread.
                 */
                void Close() { m_streambuf.SetEof(); }

            private:
                Stream::ConcurrentStreamBuf m_streambuf;
                EventStreamEncoder m_encoder;
            };
        }
    }
}
