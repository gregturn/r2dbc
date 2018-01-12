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

import com.nebhale.r2dbc.postgresql.message.Format;
import com.nebhale.r2dbc.postgresql.message.backend.BackendMessage;
import com.nebhale.r2dbc.postgresql.message.backend.CopyInResponse;
import com.nebhale.r2dbc.postgresql.message.backend.ParameterStatus;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyData;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyDone;
import com.nebhale.r2dbc.postgresql.message.frontend.CopyMessage;
import com.nebhale.r2dbc.postgresql.message.frontend.Query;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public final class CopyInMessageFlowTest {

    @Test
    public void exchange() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new Query("COPY FROM STDIN")).thenRespond(new ParameterStatus("test-name", "test-value"), new CopyInResponse(Collections.emptyList(), Format.TEXT))
                .expectRequest(new CopyData(Unpooled.buffer().writeInt(100))).thenRespond()
                .expectRequest(CopyDone.INSTANCE).thenRespond()
            .done()
            .build();
        // @formatter:off

        Flux<CopyMessage> requests = Flux.just(new CopyData(Unpooled.buffer().writeInt(100)), CopyDone.INSTANCE);
        Flux<BackendMessage> responses = client.exchange(Mono.just(new Query("COPY FROM STDIN")));

        CopyInMessageFlow
            .exchange(client, requests, responses)
            .as(StepVerifier::create)
            .expectNext(new ParameterStatus("test-name", "test-value"))
            .verifyComplete();
    }

    @Test
    public void exchangeNoClient() {
        assertThatNullPointerException().isThrownBy(() -> CopyInMessageFlow.exchange(null, Mono.empty(), Mono.empty()))
            .withMessage("client must not be null");
    }

    @Test
    public void exchangeNoRequests() {
        assertThatNullPointerException().isThrownBy(() -> CopyInMessageFlow.exchange(TestClient.NO_OP, null, Mono.empty()))
            .withMessage("requests must not be null");
    }

    @Test
    public void exchangeNoResponses() {
        assertThatNullPointerException().isThrownBy(() -> CopyInMessageFlow.exchange(TestClient.NO_OP, Mono.empty(), null))
            .withMessage("responses must not be null");
    }

}
