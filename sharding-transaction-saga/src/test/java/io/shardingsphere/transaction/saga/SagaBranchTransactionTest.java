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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public final class SagaBranchTransactionTest {
    
    private String dataSourceName = "dataSourceName";
    
    private String sql = "sql";
    
    @Test
    public void assertHashCode() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(dataSourceName, sql, getStringParameters());
        assertTrue(new SagaBranchTransaction(dataSourceName, sql, getStringParameters()).hashCode() == sagaBranchTransaction.hashCode());
        assertTrue(new SagaBranchTransaction(dataSourceName, sql, getMixedParameters()).hashCode() == sagaBranchTransaction.hashCode());
    }
    
    @Test
    public void assertEquals() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(dataSourceName, sql, getStringParameters());
        assertTrue(sagaBranchTransaction.equals(new SagaBranchTransaction(dataSourceName, sql, getStringParameters())));
        assertTrue(sagaBranchTransaction.equals(new SagaBranchTransaction(dataSourceName, sql, getMixedParameters())));
    }
    
    @Test
    public void assertToString() {
        SagaBranchTransaction sagaBranchTransaction = new SagaBranchTransaction(dataSourceName, sql, getStringParameters());
        assertTrue(new SagaBranchTransaction(dataSourceName, sql, getStringParameters()).toString().equals(sagaBranchTransaction.toString()));
        assertTrue(new SagaBranchTransaction(dataSourceName, sql, getMixedParameters()).toString().equals(sagaBranchTransaction.toString()));
    }
    
    private List<List<Object>> getStringParameters() {
        List<List<Object>> result = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        parameters.add("1");
        parameters.add("x");
        result.add(parameters);
        parameters = new ArrayList<>();
        parameters.add("2");
        parameters.add("y");
        return result;
    }
    
    private List<List<Object>> getMixedParameters() {
        List<List<Object>> result = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        parameters.add(1);
        parameters.add("x");
        result.add(parameters);
        parameters = new ArrayList<>();
        parameters.add(2);
        parameters.add("y");
        return result;
    }
}