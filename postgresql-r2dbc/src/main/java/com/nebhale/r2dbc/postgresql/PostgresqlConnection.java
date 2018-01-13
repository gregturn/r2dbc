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

package com.nebhale.r2dbc.postgresql;

import com.nebhale.r2dbc.Connection;
import com.nebhale.r2dbc.IsolationLevel;
import com.nebhale.r2dbc.Mutability;
import com.nebhale.r2dbc.postgresql.client.Client;
import com.nebhale.r2dbc.postgresql.client.SimpleQueryMessageFlow;
import com.nebhale.r2dbc.postgresql.client.TerminationMessageFlow;
import com.nebhale.r2dbc.postgresql.message.backend.CommandComplete;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.nebhale.r2dbc.postgresql.util.Util.not;

/**
 * An implementation of {@link Connection} for connecting to a PostgreSQL database.
 */
public final class PostgresqlConnection implements Connection<PostgresqlTransaction> {   // TODO: Find a better way to expose PostgresqlTransaction

    private final Client client;

    private final PostgresqlOperations delegate;

    PostgresqlConnection(Client client) {
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.delegate = new PostgresqlOperations(this.client);
    }

    @Override
    public Mono<PostgresqlTransaction> begin() {
        return SimpleQueryMessageFlow
            .exchange(this.client, "BEGIN")
            .takeWhile(not(CommandComplete.class::isInstance))
            .then(Mono.just(new PostgresqlTransaction(this.client)));
    }

    @Override
    public <T> Mono<T> close() {
        return TerminationMessageFlow.exchange(this.client)
            .doOnComplete(this.client::close)
            .then(Mono.empty());
    }

    /**
     * Returns a snapshot of the current parameter statuses.
     *
     * @return a snapshot of the current parameter statuses
     */
    public Map<String, String> getParameterStatus() {
        return this.client.getParameterStatus();
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if {@code query} is {@code null}
     */
    @Override
    public Flux<Flux<PostgresqlRow>> query(String query) {
        return this.delegate.query(query);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if {@code isolationLevel} is {@code null}
     */
    @Override
    public <T> Mono<T> setIsolationLevel(IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return SimpleQueryMessageFlow
            .exchange(this.client, String.format("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s", isolationLevel.asSql()))
            .takeWhile(not(CommandComplete.class::isInstance))
            .then(Mono.empty());
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if {@code mutability} is {@code null}
     */
    @Override
    public <T> Mono<T> setMutability(Mutability mutability) {
        Objects.requireNonNull(mutability, "mutability must not be null");

        return SimpleQueryMessageFlow
            .exchange(this.client, String.format("SET SESSION CHARACTERISTICS AS TRANSACTION %s", mutability.asSql()))
            .takeWhile(not(CommandComplete.class::isInstance))
            .then(Mono.empty());
    }

    @Override
    public String toString() {
        return "PostgresqlConnection{" +
            "client=" + this.client +
            ", delegate=" + this.delegate +
            '}';
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if {@code transaction} is {@code null}
     */
    @Override
    public <T> Flux<T> withTransaction(Function<PostgresqlTransaction, Publisher<T>> transaction) {
        Objects.requireNonNull(transaction, "transaction must not be null");

        return begin()
            .flatMapMany(tx ->
                Flux.from(transaction.apply(tx))
                    .concatWith(tx.commit())
                    .onErrorResume(t ->
                        tx.rollback()
                            .then(Mono.error(t))));
    }

}
