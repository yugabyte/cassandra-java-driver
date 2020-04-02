// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package com.yugabyte.driver.core.policies;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

/**
 * Inherits behavior from DefaultRetryPolicy with one exception - rethrows request error in case it
 * is an OperationTimedOutException.
 */
public class NoRetryOnClientTimeoutPolicy extends DefaultRetryPolicy {

  public static final NoRetryOnClientTimeoutPolicy INSTANCE = new NoRetryOnClientTimeoutPolicy();

  protected NoRetryOnClientTimeoutPolicy() {}

  @Override
  public RetryDecision onRequestError(
      Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
    if (e instanceof OperationTimedOutException) {
      return RetryDecision.rethrow();
    } else {
      return super.onRequestError(statement, cl, e, nbRetry);
    }
  }
}
