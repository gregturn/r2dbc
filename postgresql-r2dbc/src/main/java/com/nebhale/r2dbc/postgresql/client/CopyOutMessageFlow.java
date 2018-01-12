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
import com.nebhale.r2dbc.postgresql.message.backend.CopyData;
import com.nebhale.r2dbc.postgresql.message.backend.CopyDone;
import com.nebhale.r2dbc.postgresql.message.backend.CopyOutResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

import static com.nebhale.r2dbc.postgresql.util.Util.not;

/**
 * A utility class that encapsulates the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#PROTOCOL-COPY">Copy out</a> message flow.
 */
public final class CopyOutMessageFlow {

    private CopyOutMessageFlow() {
    }

    /**
     * Execute the <a href="https://www.postgresql.org/docs/10/static/protocol-flow.html#PROTOCOL-COPY">Copy out</a> message flow.
     *
     * @param client    the {@link Client} to exchange messages with
     * @param responses the {@link BackendMessage}s to watch until {@link CopyOutResponse} signals that {@link CopyData} will start sending
     * @return the messages received in response to this exchange.  This should be a stream of {@link CopyData} messages with a {@link CopyDone} message at the end.
     * @throws NullPointerException if {@code client}, or {@code responses} is {@code null}
     */
    public static Flux<BackendMessage> exchange(Client client, Publisher<BackendMessage> responses) {
        Objects.requireNonNull(client, "client must not be null");
        Objects.requireNonNull(responses, "responses must not be null");

        return Flux.from(responses)
            .takeWhile(not(CopyOutResponse.class::isInstance))
            .concatWith(client.exchange(Mono.empty()));
    }

}
