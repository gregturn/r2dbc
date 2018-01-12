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
import com.nebhale.r2dbc.postgresql.message.backend.CopyData;
import com.nebhale.r2dbc.postgresql.message.backend.CopyOutResponse;
import com.nebhale.r2dbc.postgresql.message.backend.ParameterStatus;
import com.nebhale.r2dbc.postgresql.message.frontend.Query;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public final class CopyOutMessageFlowTest {

    @Test
    public void exchange() {
        // @formatter:off
        Client client = TestClient.builder()
            .window()
                .expectRequest(new Query("COPY TO STDOUT"))
                    .thenRespond(new ParameterStatus("test-name", "test-value"), new CopyOutResponse(Collections.emptyList(), Format.TEXT), new CopyData(Unpooled.buffer().writeInt(100)))
            .done()
            .build();
        // @formatter:off

        Flux<BackendMessage> responses = client.exchange(Mono.just(new Query("COPY TO STDOUT")));

        CopyOutMessageFlow
            .exchange(client, responses)
            .as(StepVerifier::create)
            .expectNext(new ParameterStatus("test-name", "test-value"))
            .expectNext(new CopyData(Unpooled.buffer().writeInt(100)))
            .verifyComplete();
    }

    @Test
    public void exchangeNoClient() {
        assertThatNullPointerException().isThrownBy(() -> CopyOutMessageFlow.exchange(null, Mono.empty()))
            .withMessage("client must not be null");
    }

    @Test
    public void exchangeNoResponses() {
        assertThatNullPointerException().isThrownBy(() -> CopyOutMessageFlow.exchange(TestClient.NO_OP, null))
            .withMessage("responses must not be null");
    }

}
