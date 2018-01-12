/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nebhale.r2dbc.postgresql.client;

import com.nebhale.r2dbc.postgresql.message.backend.BackendMessage;
import com.nebhale.r2dbc.postgresql.message.backend.CopyInResponse;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyData;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyDone;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyFail;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyMessage;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Objects;

import static com.nebhale.r2dbc.postgresql.util.Util.not;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#PROTOCOL-COPY">Copy in</a> message flow.
 */
public final class CopyInMessageFlow {

    private CopyInMessageFlow() {
    }

    /**
     * Execute the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#PROTOCOL-COPY">Copy in</a> message flow.
     *
     * @param client    the {@link Client} to exchange messages with
     * @param requests  the {@link CopyMessage}s to send.  This should be a stream of {@link CopyData} messages with either a {@link CopyDone} or {@link CopyFail} message at the end.
     * @param responses the {@link BackendMessage}s to watch until a {@link CopyInResponse} signals that {@code requests} can start sending
     * @return the messages received in response to this exchange
     * @throws NullPointerException if {@code client}, {@code messages}, or {@code responses} is {@code null}
     */
    public static Flux<BackendMessage> exchange(Client client, Publisher<CopyMessage> requests, Publisher<BackendMessage> responses) {
        Objects.requireNonNull(client, "client must not be null");
        Objects.requireNonNull(requests, "requests must not be null");
        Objects.requireNonNull(responses, "responses must not be null");

        return Flux.from(responses)
            .takeWhile(not(CopyInResponse.class::isInstance))
            .concatWith(client.exchange(requests));
    }

}
