package com.datastax.driver.core;

import io.netty.channel.*;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * {@link ChannelDuplexHandler} which consolidate {@link Channel#flush()} / {@link ChannelHandlerContext#flush()}
 * operations (which also includes
 * {@link Channel#writeAndFlush(Object)} / {@link Channel#writeAndFlush(Object, ChannelPromise)} and
 * {@link ChannelHandlerContext#writeAndFlush(Object)} /
 * {@link ChannelHandlerContext#writeAndFlush(Object, ChannelPromise)}).
 * <p/>
 * Flush operations are general speaking expensive as these may trigger a syscall on the transport level. Thus it is
 * in most cases (where write latency can be traded with throughput) a good idea to try to minimize flush operations
 * as much as possible.
 * <p/>
 * When {@link #flush(ChannelHandlerContext)} is called it will only pass it on to the next
 * {@link ChannelOutboundHandler} in the {@link ChannelPipeline} if no read loop is currently ongoing
 * as it will pick up any pending flushes when {@link #channelReadComplete(ChannelHandlerContext)} is trigged.
 * If {@code explicitFlushAfterFlushes} is reached the flush will also be forwarded as well.
 * <p/>
 * If the {@link Channel} becomes non-writable it will also try to execute any pending flush operations.
 * <p/>
 * The {@link FlushConsolidationHandler} should be put as first {@link ChannelHandler} in the
 * {@link ChannelPipeline} to have the best effect.
 */
public class FlushConsolidationHandler extends ChannelDuplexHandler {
    private final int explicitFlushAfterFlushes;
    private int flushPendingCount;
    private boolean readInProgress;
    private ChannelHandlerContext ctx;
    private Future<?> nextScheduledFlush;

    private final Runnable flushTask = new Runnable() {
        @Override
        public void run() {
            if (flushPendingCount > 0 && !readInProgress) {
                flushPendingCount = 0;
                ctx.flush();
                nextScheduledFlush = null;
            } // else we'll flush when the read completes
        }
    };

    /**
     * Create new instance which explicit flush after 256 pending flush operations latest.
     */
    public FlushConsolidationHandler() {
        this(256);
    }

    /**
     * Create new instance.
     *
     * @param explicitFlushAfterFlushes the number of flushes after which an explicit flush will be done.
     */
    public FlushConsolidationHandler(int explicitFlushAfterFlushes) {
        if (explicitFlushAfterFlushes <= 0) {
            throw new IllegalArgumentException("explicitFlushAfterFlushes: "
                    + explicitFlushAfterFlushes + " (expected: > 0)");
        }
        this.explicitFlushAfterFlushes = explicitFlushAfterFlushes;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (++flushPendingCount == explicitFlushAfterFlushes) {
            if (nextScheduledFlush != null) {
                nextScheduledFlush.cancel(false);
                nextScheduledFlush = null;
            }
            flushPendingCount = 0;
            ctx.flush();
        } else if (!readInProgress) {
            scheduleFlush(ctx);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // This may be the last event in the read loop, so flush now!
        resetReadAndFlushIfNeeded(ctx);
        ctx.fireChannelReadComplete();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        readInProgress = true;
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // To ensure we not miss to flush anything, do it now.
        resetReadAndFlushIfNeeded(ctx);
        ctx.fireExceptionCaught(cause);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        // Try to flush one last time if flushes are pending before disconnect the channel.
        resetReadAndFlushIfNeeded(ctx);
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        // Try to flush one last time if flushes are pending before close the channel.
        resetReadAndFlushIfNeeded(ctx);
        ctx.close(promise);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (!ctx.channel().isWritable()) {
            // The writability of the channel changed to false, so flush all consolidated flushes now to free up memory.
            flushIfNeeded(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        flushIfNeeded(ctx);
    }

    private void resetReadAndFlushIfNeeded(ChannelHandlerContext ctx) {
        readInProgress = false;
        flushIfNeeded(ctx);
    }

    private void flushIfNeeded(ChannelHandlerContext ctx) {
        if (flushPendingCount > 0) {
            if (nextScheduledFlush != null) {
                nextScheduledFlush.cancel(false);
                nextScheduledFlush = null;
            }
            flushPendingCount = 0;
            ctx.flush();
        }
    }

    private void scheduleFlush(final ChannelHandlerContext ctx) {
        if (nextScheduledFlush == null) {
            // Run as soon as possible, but still yield to give a chance for additional writes to enqueue.
            nextScheduledFlush = ctx.channel().eventLoop().schedule(flushTask, 0, TimeUnit.MILLISECONDS);
        }
    }
}
