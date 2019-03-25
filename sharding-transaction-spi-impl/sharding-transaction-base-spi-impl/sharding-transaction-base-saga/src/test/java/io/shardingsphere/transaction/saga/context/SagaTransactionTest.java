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

package io.shardingsphere.transaction.saga.context;

import io.shardingsphere.transaction.saga.constant.ExecuteStatus;
import org.apache.servicecomb.saga.core.RecoveryPolicy;
import org.apache.shardingsphere.core.constant.SQLType;
import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.parser.sql.dml.DMLStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public final class SagaTransactionTest {
    
    private SagaTransaction sagaTransaction;

    @Mock
    private DMLStatement sqlStatement;
    
    @Mock
    private ShardingTableMetaData shardingTableMetaData;
    
    private final String sql = "UPDATE";
    
    @Before
    public void setUp() {
        sagaTransaction = new SagaTransaction(UUID.randomUUID().toString(), RecoveryPolicy.SAGA_FORWARD_RECOVERY_POLICY);
        when(sqlStatement.getType()).thenReturn(SQLType.DML);
    }
    
    @Test
    public void assertNextBranchTransactionGroup() {
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        assertNotNull(sagaTransaction.getCurrentBranchTransactionGroup());
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(1));
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        assertThat(sagaTransaction.getBranchTransactionGroups().size(), is(2));
    }
    
    @Test
    public void assertAddBranchTransactionToGroup() {
        sagaTransaction.nextBranchTransactionGroup(sql, sqlStatement, shardingTableMetaData);
        sagaTransaction.addBranchTransactionToGroup(new SagaBranchTransaction("", sql, null));
        assertThat(sagaTransaction.getCurrentBranchTransactionGroup().getBranchTransactions().size(), is(1));
    }
    
    @Test
    public void assertUpdateExecutionResultWithContainsException() {
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.FAILURE);
        assertThat(sagaTransaction.getExecutionResults().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResults().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResults().get(sagaBranchTransaction), is(ExecuteStatus.FAILURE));
        assertTrue(sagaTransaction.isContainsException());
    }
    
    @Test
    public void assertUpdateExecutionResultWithoutContainsException() {
        SagaBranchTransaction sagaBranchTransaction = mock(SagaBranchTransaction.class);
        sagaTransaction.updateExecutionResult(sagaBranchTransaction, ExecuteStatus.EXECUTING);
        assertThat(sagaTransaction.getExecutionResults().size(), is(1));
        assertTrue(sagaTransaction.getExecutionResults().containsKey(sagaBranchTransaction));
        assertThat(sagaTransaction.getExecutionResults().get(sagaBranchTransaction), is(ExecuteStatus.EXECUTING));
        assertFalse(sagaTransaction.isContainsException());
    }
}
