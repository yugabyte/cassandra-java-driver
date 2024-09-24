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

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indicates that a connection to a host has encountered a problem
 * and that it should be closed.
 */
public class ConnectionException extends DriverException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    public final InetSocketAddress address;

    private static final Logger logger = LoggerFactory.getLogger(ConnectionException.class);

    public ConnectionException(InetSocketAddress address, String msg, Throwable cause) {
        logger.debug("ConnectionException() constructor 1 InetSocketAddress: {}, msg {}, cause {}", address.getAddress(), msg, cause.getClass().getName());
        Thread.currentThread().getStackTrace();
        super(msg, cause);
        this.address = address;
    }

    public ConnectionException(InetSocketAddress address, String msg) {
        logger.debug("ConnectionException() constructor 2 InetSocketAddress: {}, msg {}", address.getAddress(), msg);
        Thread.currentThread().getStackTrace();
        super(msg);
        this.address = address;
    }

    @Override
    public InetAddress getHost() {
        return address == null ? null : address.getAddress();
    }

    @Override
    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public String getMessage() {
        return address == null ? getRawMessage() : String.format("[%s] %s", address, getRawMessage());
    }

    @Override
    public ConnectionException copy() {
        return new ConnectionException(address, getRawMessage(), this);
    }

    String getRawMessage() {
        return super.getMessage();
    }

}
