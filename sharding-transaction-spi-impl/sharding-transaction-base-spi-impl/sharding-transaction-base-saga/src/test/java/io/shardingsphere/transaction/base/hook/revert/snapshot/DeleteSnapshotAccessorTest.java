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

package io.shardingsphere.transaction.base.hook.revert.snapshot;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.base.hook.revert.executor.SQLRevertExecutorContext;
import org.apache.shardingsphere.core.optimize.sharding.statement.ShardingOptimizedStatement;
import org.apache.shardingsphere.core.parse.sql.segment.dml.predicate.WhereSegment;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DeleteStatement;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeleteSnapshotAccessorTest {
    
    @Mock
    private SQLRevertExecutorContext executorContext;
    
    @Mock
    private DeleteStatement deleteStatement;
    
    @Mock
    private WhereSegment whereSegment;
    
    @Mock
    private ShardingOptimizedStatement shardingOptimizedStatement;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    @Mock
    private ResultSetMetaData resultSetMetaData;
    
    private List<Object> parameters = new LinkedList<>();
    
    private DeleteSnapshotAccessor deleteSnapshotAccessor;
    
    @Before
    public void setUp() throws SQLException {
        when(executorContext.getShardingStatement()).thenReturn(shardingOptimizedStatement);
        when(executorContext.getActualTableName()).thenReturn("t_order_0");
        when(shardingOptimizedStatement.getSQLStatement()).thenReturn(deleteStatement);
        when(executorContext.getLogicSQL()).thenReturn("DELETE FROM t_order WHERE order_id = ?");
        when(whereSegment.getStartIndex()).thenReturn(20);
        when(whereSegment.getStopIndex()).thenReturn(37);
        when(deleteStatement.getWhere()).thenReturn(Optional.of(whereSegment));
        when(executorContext.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        deleteSnapshotAccessor = new DeleteSnapshotAccessor(executorContext);
    }
    
    @Test
    public void assertGetSnapshotSQLContext() {
        SnapshotSQLContext actual = deleteSnapshotAccessor.getSnapshotSQLContext(executorContext);
        assertThat(actual.getConnection(), is(connection));
        assertThat(actual.getParameters(), CoreMatchers.<Collection<Object>>is(parameters));
        assertThat(actual.getQueryColumnNames(), CoreMatchers.<Collection<String>>is(Collections.singleton("*")));
        assertThat(actual.getTableName(), is("t_order_0"));
        assertThat(actual.getWhereClause(), is("WHERE order_id = ?"));
    }
    
    @Test
    public void assertQueryUndoData() throws SQLException {
        deleteSnapshotAccessor.queryUndoData();
        verify(connection).prepareStatement("SELECT * FROM t_order_0 WHERE order_id = ? ");
    }
}