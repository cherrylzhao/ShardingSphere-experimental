/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.shardingsphere.transaction.saga;

import io.shardingsphere.transaction.saga.config.SagaConfiguration;
import io.shardingsphere.transaction.saga.config.SagaConfigurationLoader;
import io.shardingsphere.transaction.saga.context.SagaBranchTransaction;
import io.shardingsphere.transaction.saga.context.SagaBranchTransactionGroup;
import io.shardingsphere.transaction.saga.context.SagaTransaction;
import io.shardingsphere.transaction.saga.persistence.SagaPersistence;
import io.shardingsphere.transaction.saga.persistence.SagaPersistenceLoader;
import io.shardingsphere.transaction.saga.revert.SQLRevertResult;
import io.shardingsphere.transaction.saga.servicecomb.SagaExecutionComponentFactory;
import io.shardingsphere.transaction.saga.servicecomb.definition.SagaDefinitionBuilder;
import io.shardingsphere.transaction.saga.servicecomb.transport.ShardingTransportFactory;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.servicecomb.saga.core.application.SagaExecutionComponent;
import org.apache.shardingsphere.core.constant.DatabaseType;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.core.route.SQLUnit;
import org.apache.shardingsphere.transaction.core.ResourceDataSource;
import org.apache.shardingsphere.transaction.core.TransactionType;
import org.apache.shardingsphere.transaction.spi.ShardingTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sharding transaction manager for Saga.
 *
 * @author yangyi
 */
public final class SagaShardingTransactionManager implements ShardingTransactionManager {
    
    public static final String CURRENT_TRANSACTION_KEY = "current_transaction";
    
    private final SagaConfiguration sagaConfiguration;
    
    @Getter
    private final SagaPersistence sagaPersistence;
    
    @Getter
    private final SagaExecutionComponent sagaExecutionComponent;
    
    private final Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();
    
    public SagaShardingTransactionManager() {
        sagaConfiguration = SagaConfigurationLoader.load();
        sagaPersistence = SagaPersistenceLoader.load(sagaConfiguration.getSagaPersistenceConfiguration());
        sagaExecutionComponent = SagaExecutionComponentFactory.createSagaExecutionComponent(sagaConfiguration, sagaPersistence);
    }
    
    public static SagaTransaction getCurrentTransaction() {
        return SagaTransactionManager.getCurrentSagaTransaction();
    }
    
    @Override
    public void init(final DatabaseType databaseType, final Collection<ResourceDataSource> resourceDataSources) {
        for (ResourceDataSource each : resourceDataSources) {
            if (dataSourceMap.containsKey(each.getOriginalName())) {
                throw new ShardingException("datasource {} has registered", each.getOriginalName());
            }
            dataSourceMap.put(each.getOriginalName(), each.getDataSource());
        }
    }
    
    @Override
    public TransactionType getTransactionType() {
        return TransactionType.BASE;
    }
    
    @Override
    public boolean isInTransaction() {
        return null != SagaTransactionContextHolder.getGlobalTxId();
    }
    
    @Override
    public Connection getConnection(final String dataSourceName) throws SQLException {
        Connection result = dataSourceMap.get(dataSourceName).getConnection();
        SagaTransactionManager.getCurrentTransactionResource().getConnections().putIfAbsent(dataSourceName, result);
        return result;
    }
    
    @Override
    public void begin() {
        if (null == SagaTransactionContextHolder.getGlobalTxId()) {
            String globalTxId = UUID.randomUUID().toString();
            SagaTransactionContextHolder.setGlobalTxId(globalTxId);
            SagaTransactionManager.register(globalTxId, sagaConfiguration.getRecoveryPolicy(), sagaPersistence);
            ShardingTransportFactory.getInstance().cacheTransport(SagaTransactionManager.getCurrentSagaTransaction());
        }
    }
    
    @Override
    public void commit() {
        if (null != SagaTransactionContextHolder.getGlobalTxId()
            && SagaTransactionManager.getCurrentSagaTransaction().isContainsException()) {
            submitToSagaEngine(false);
        }
        cleanTransaction();
    }
    
    @Override
    public void rollback() {
        if (null != SagaTransactionContextHolder.getGlobalTxId()) {
            submitToSagaEngine(isForcedRollback());
        }
        cleanTransaction();
    }
    
    private boolean isForcedRollback() {
        return !SagaTransactionManager.getCurrentSagaTransaction().isContainsException()
            && RecoveryPolicy.SAGA_BACKWARD_RECOVERY_POLICY.equals(sagaConfiguration.getRecoveryPolicy());
    }
    
    @SneakyThrows
    private void submitToSagaEngine(final boolean isForcedRollback) {
        SagaDefinitionBuilder sagaDefinitionBuilder = getSagaDefinitionBuilder();
        if (isForcedRollback) {
            sagaDefinitionBuilder.addRollbackRequest();
        }
        sagaExecutionComponent.run(sagaDefinitionBuilder.build());
    }
    
    private SagaDefinitionBuilder getSagaDefinitionBuilder() {
        SagaDefinitionBuilder result = new SagaDefinitionBuilder(sagaConfiguration.getRecoveryPolicy(),
            sagaConfiguration.getTransactionMaxRetries(), sagaConfiguration.getCompensationMaxRetries(), sagaConfiguration.getTransactionRetryDelayMilliseconds());
        for (SagaBranchTransactionGroup each : SagaTransactionManager.getCurrentSagaTransaction().getBranchTransactionGroups()) {
            result.switchParents();
            initSagaDefinitionForGroup(result, each);
        }
        return result;
    }
    
    private void initSagaDefinitionForGroup(final SagaDefinitionBuilder sagaDefinitionBuilder, final SagaBranchTransactionGroup sagaBranchTransactionGroup) {
        SagaTransaction currentTransaction = SagaTransactionManager.getCurrentSagaTransaction();
        for (SagaBranchTransaction each : sagaBranchTransactionGroup.getBranchTransactions()) {
            SQLRevertResult revertResult = currentTransaction.getRevertResults().containsKey(each) ? currentTransaction.getRevertResults().get(each) : new SQLRevertResult();
            sagaDefinitionBuilder.addChildRequest(
                String.valueOf(each.hashCode()), each.getDataSourceName(), each.getSql(), each.getParameterSets(), revertResult.getSql(), revertResult.getParameterSets());
        }
    }
    
    private void cleanTransaction() {
        String globalTxId = SagaTransactionContextHolder.getGlobalTxId();
        if (null != globalTxId) {
            cleanTransactionalSQLUnit(globalTxId);
            sagaPersistence.cleanSnapshot(globalTxId);
            SagaTransactionManager.release(globalTxId);
        }
        ShardingTransportFactory.getInstance().remove();
        SagaTransactionContextHolder.clear();
    }
    
    private void cleanTransactionalSQLUnit(final String globalTxId) {
        SagaTransaction sagaTransaction = SagaTransactionManager.getCurrentSagaTransaction(globalTxId);
        for (SQLUnit each : sagaTransaction.getTableUnitMap().keySet()) {
            TransactionalSQLUnitManager.removeSQLUnit(each);
        }
    }
    
    @Override
    public void close() {
        dataSourceMap.clear();
    }
}
