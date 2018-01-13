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

package com.nebhale.r2dbc.postgresql.util;

import java.util.function.Predicate;

/**
 * Miscellaneous utilities.
 */
public final class Util {

    private Util() {
    }

    /**
     * Negate a {@link Predicate}.  Exists primarily to handle {@code *.class::isInstance} negation.
     *
     * @param t   the {@link Predicate} to negate
     * @param <T> the type being tested
     * @return a negated {@link Predicate}
     */
    public static <T> Predicate<T> not(Predicate<T> t) {
        return t.negate();
    }

}
