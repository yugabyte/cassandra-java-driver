/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection exception that has to do with the transport itself, i.e. that
 * suggests the node is down.
 */
public class TransportException extends ConnectionException {

    private static final long serialVersionUID = 0;

    private static final Logger logger = LoggerFactory.getLogger(TransportException.class);

    public TransportException(InetSocketAddress address, String msg, Throwable cause) {
        logger.debug("TransportException() constructor 1 InetSocketAddress: {}, msg {}, cause {}", address.getAddress(), msg, cause.getClass().getName());
        Thread.currentThread().getStackTrace();
        super(address, msg, cause);
    }

    public TransportException(InetSocketAddress address, String msg) {
        logger.debug("TransportException() constructor 2 InetSocketAddress: {}, msg {}", address.getAddress(), msg);
        Thread.currentThread().getStackTrace();
        super(address, msg);
    }

    @Override
    public TransportException copy() {
        return new TransportException(address, getRawMessage(), this);
    }

}
