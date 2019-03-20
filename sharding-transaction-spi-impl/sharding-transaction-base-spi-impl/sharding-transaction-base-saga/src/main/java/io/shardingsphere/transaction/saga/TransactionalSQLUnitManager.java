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

import org.apache.shardingsphere.core.route.SQLUnit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Transactional SQL unit manager.
 *
 * @author zhaojun
 */
public class TransactionalSQLUnitManager {
    
    private static final ConcurrentMap<SQLUnit, TransactionalSQLUnit> SQL_UNIT_MAP = new ConcurrentHashMap<>();
    
    /**
     * Register SQL unit.
     * @param sqlUnit SQL unit
     */
    public static void register(final SQLUnit sqlUnit) {
        SQL_UNIT_MAP.putIfAbsent(sqlUnit, new TransactionalSQLUnit(sqlUnit, SagaTransactionContextHolder.getGlobalTxId(), SagaTransactionContextHolder.getBranchTxId()));
    }
    
    /**
     * Get transactional SQL unit.
     * @param sqlUnit SQL unit
     * @return transactional SQL unit
     */
    public static TransactionalSQLUnit getTransactionalSQLUnit(final SQLUnit sqlUnit) {
        return SQL_UNIT_MAP.get(sqlUnit);
    }
    
    
    /**
     * Remove SQL unit.
     * @param sqlUnit SQL unit
     */
    public static void removeSQLUnit(final SQLUnit sqlUnit) {
        SQL_UNIT_MAP.remove(sqlUnit);
    }
}
