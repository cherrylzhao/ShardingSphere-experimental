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

/**
 * Saga transaction context holder.
 *
 * @author zhaojun
 */
public class SagaTransactionContextHolder {
    
    private static final ThreadLocal<String> GLOBAL_TX_ID = new ThreadLocal<>();
    
    private static final ThreadLocal<String> BRANCH_TX_ID = new ThreadLocal<>();
    
    public static void setGlobalTxId(final String globalTxId) {
        GLOBAL_TX_ID.set(globalTxId);
    }
    
    public static String getGlobalTxId() {
        return GLOBAL_TX_ID.get();
    }
    
    public static void setBranchTxId(final String branchTxId) {
        BRANCH_TX_ID.set(branchTxId);
    }
    
    public static String getBranchTxId() {
        return BRANCH_TX_ID.get();
    }
    
    public static void clear() {
        GLOBAL_TX_ID.remove();
        BRANCH_TX_ID.remove();
    }
}
