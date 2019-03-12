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

package io.shardingsphere.transaction.saga.revert.impl.insert;

import com.google.common.base.Optional;
import io.shardingsphere.transaction.saga.revert.BaseRevertTest;
import io.shardingsphere.transaction.saga.revert.api.RevertContext;

import java.sql.SQLException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BaseInsertTest extends BaseRevertTest {
    
    public static final String REVERT_SQL = "DELETE FROM t_order_item_1 WHERE ORDER_ITEM_ID = ?";
    
    protected void asertRevertContext(final Optional<RevertContext> revertContext, final String revertSQL) throws SQLException {
        super.asertRevertContext(revertContext, revertSQL, 1);
        Iterator<Object> iterator = revertContext.get().getRevertParams().get(0).iterator();
        assertThat("Assert ORDER_ITEM_ID value error: ", (long) iterator.next(), is(ORDER_ITEM_ID));
    }

}