/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.transaction.saga;

import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.resource.SagaTransactionResource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Saga transaction manager.
 *
 * @author zhaojun
 */
public class SagaTransactionManager {
    
    private static final ConcurrentMap<String, SagaTransaction> TRANSACTION_MAP = new ConcurrentHashMap<>();
    
    private static final ConcurrentMap<String, SagaTransactionResource> RESOURCE_MAP = new ConcurrentHashMap<>();
    
    public static void register(final String globalTxId, final String recoveryPolicy, final SagaPersistence sagaPersistence) {
        TRANSACTION_MAP.putIfAbsent(globalTxId, new SagaTransaction(globalTxId, recoveryPolicy));
        RESOURCE_MAP.put(globalTxId, new SagaTransactionResource(sagaPersistence));
    }
    
    public static SagaTransactionResource getCurrentTransactionResource() {
        return null != SagaTransactionContextHolder.getGlobalTxId() ? RESOURCE_MAP.get(SagaTransactionContextHolder.getGlobalTxId()) : null;
    }
    
    public static SagaTransactionResource getCurrentTransactionResource(final String globalTxId) {
        return RESOURCE_MAP.get(globalTxId);
    }
    
    public static SagaTransaction getCurrentSagaTransaction() {
        return null != SagaTransactionContextHolder.getGlobalTxId() ? TRANSACTION_MAP.get(SagaTransactionContextHolder.getGlobalTxId()) : null;
    }
    
    public static SagaTransaction getCurrentSagaTransaction(final String globalTxId) {
        return TRANSACTION_MAP.get(globalTxId);
    }
    
    public static void release(final String globalTxId) {
        RESOURCE_MAP.remove(globalTxId);
        TRANSACTION_MAP.remove(globalTxId);
    }
    
}
