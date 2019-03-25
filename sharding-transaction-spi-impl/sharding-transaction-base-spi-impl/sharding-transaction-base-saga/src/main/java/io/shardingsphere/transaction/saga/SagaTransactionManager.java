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

import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Saga transaction manager.
 *
 * @author zhaojun
 */
@RequiredArgsConstructor
public class SagaTransactionManager {
    
    private final SagaConfiguration sagaConfiguration;
    
    private final Map<String, SagaTransaction> sagaTransactionMap = new ConcurrentHashMap<>();
    
    private final ThreadLocal<SagaTransaction> currentTransaction = new ThreadLocal<>();
    
    /**
     * Saga transaction manager begin.
     */
    public void begin() {
        if (null == currentTransaction.get()) {
            SagaTransaction sagaTransaction = new SagaTransaction(sagaConfiguration.getRecoveryPolicy());
            sagaTransactionMap.put(sagaTransaction.getId(), sagaTransaction);
            currentTransaction.set(sagaTransaction);
        }
    }
    
    /**
     * Get current transaction.
     *
     * @return saga transaction
     */
    public SagaTransaction getCurrentTransaction() {
        return currentTransaction.get();
    }
}
