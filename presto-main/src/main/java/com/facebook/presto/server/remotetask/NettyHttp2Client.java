/*
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
 */
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.RequestStats;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StaticBodyGenerator;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.server.smile.BaseResponse;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT;
import static io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE;
import static io.netty.handler.ssl.ApplicationProtocolNames.HTTP_2;
import static java.lang.String.format;

public class NettyHttp2Client
        implements HttpClient, Closeable
{
    private static final Logger log = Logger.get(NettyHttp2Client.class);
    private static final AttributeKey<Boolean> IN_POOL = AttributeKey.valueOf("inPool");

    private final Bootstrap bootstrap;
    private final EventLoopGroup group;
    private SslContext sslContext;
    private int nettyMaxStreamPerChannel;

    // Map with key: inetAddress, value : FixeChannelPool
    private final ChannelPoolMap<InetSocketAddress, FixedChannelPool> poolMap;

    private final ConcurrentHashMap<Channel, Integer> channelStreamCountMap = new ConcurrentHashMap<>();

    @Inject
    public NettyHttp2Client(TaskManagerConfig config)
    {
        int threadCount = config.getNettyEventLoopThreadCount();

        this.bootstrap = new Bootstrap();

        // Create an EventLoopGroup to handle the client's event loop
        if (config.isNettyEpollEnabled()) {
            this.group = new EpollEventLoopGroup(threadCount);
            // Create a Bootstrap instance to configure the client
            bootstrap.group(group).channel(EpollSocketChannel.class);
        }
        else {
            this.group = new NioEventLoopGroup(threadCount);
            bootstrap.group(group).channel(NioSocketChannel.class);
        }

        this.nettyMaxStreamPerChannel = config.getNettyMaxStreamPerChannel();

        File keyFile = new File("/var/facebook/x509_identities/client.pem");
        File trustCertificateFile = new File("/var/facebook/rootcanal/ca.pem");

        // Comment out the following if you want to try with HiveQueryRunner
        List<String> ciphers = Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384");
        try {
//            PrivateKey privateKey = loadPrivateKey(keyFile, Optional.of("password"));
//            X509Certificate[] certificateChain = readCertificateChain(keyFile).toArray(new X509Certificate[0]);
            X509Certificate[] trustChain = readCertificateChain(trustCertificateFile).toArray(new X509Certificate[0]);

            SslProvider provider = SslProvider.isAlpnSupported(SslProvider.OPENSSL) ? SslProvider.OPENSSL : SslProvider.JDK;
            this.sslContext = SslContextBuilder.forClient()
                    .sslProvider(provider)
                    .ciphers(ciphers)
                    .trustManager(trustChain)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(config.getNettyApplicationProtocol(), NO_ADVERTISE, ACCEPT, HTTP_2))
                    .build();
        }
        catch (Exception e) {
            log.error(format("NIKHIL error during bootstrap creation, Details: %s", e.getMessage()));
        }

        int maxConnectionsPerDestination = config.getNettyMaxConnectionsPerDestination();

        // Channel Handler provided to Channel Pool will override the channel initializer provided to the bootstrap
        this.poolMap = new AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool>()
        {
            @Override
            protected FixedChannelPool newPool(InetSocketAddress key)
            {
                return new FixedChannelPool(bootstrap.remoteAddress(key), new AbstractChannelPoolHandler()
                {
                    @Override
                    public void channelCreated(Channel ch)
                            throws Exception
                    {
                        // Add the SSL handler to the pipeline. Comment out this if running HiveQueryRunner
                        ch.pipeline().addFirst(sslContext.newHandler(ch.alloc()));

                        Http2FrameCodec http2FrameCodec = Http2FrameCodecBuilder.forClient()
                                .initialSettings(Http2Settings.defaultSettings()) // this is the default, but shows it can be changed.
                                .build();
                        ch.pipeline().addLast(http2FrameCodec);
                        ch.pipeline().addLast(new Http2MultiplexHandler(new SimpleChannelInboundHandler()
                        {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg)
                            {
                                // NOOP (this is the handler for 'inbound' streams, which is not relevant in this example)
                            }
                        }));
                    }
                }, ChannelHealthChecker.ACTIVE, FixedChannelPool.AcquireTimeoutAction.NEW, config.getNettyChannelAcquireWaitTime(), maxConnectionsPerDestination, Integer.MAX_VALUE);
            }
        };
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return null;
    }

    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        SettableFuture<BaseResponse<T>> listenableFuture = SettableFuture.create();
        InetSocketAddress address = new InetSocketAddress(request.getUri().getHost(), request.getUri().getPort());
        try {
            FixedChannelPool pool = poolMap.get(address);
            Future<Channel> f = pool.acquire();
            f.addListener(new FutureListener<Channel>()
            {
                @Override
                public void operationComplete(Future<Channel> f)
                {
                    if (f.isSuccess()) {
                        // Successfully acquired a channel from the pool
                        Channel channel = f.getNow();
                        channel.attr(IN_POOL).set(false);

                        // create a stream channel from channel
                        Http2StreamChannelBootstrap streamChannelBootstrap = new Http2StreamChannelBootstrap(channel);
                        Http2StreamChannel streamChannel = streamChannelBootstrap.open().syncUninterruptibly().getNow();
                        log.error("NIKHIL stream created");

                        // Increment stream count
                        channelStreamCountMap.put(channel, channelStreamCountMap.getOrDefault(channel, 0) + 1);

                        Http2ClientStreamFrameResponseHandler streamFrameResponseHandler =
                                new Http2ClientStreamFrameResponseHandler(listenableFuture, responseHandler, streamChannel, channel, pool, channelStreamCountMap);
                        streamChannel.pipeline().addLast(streamFrameResponseHandler);

                        // Send request (a HTTP/2 HEADERS frame - with ':method = GET' in this case)
                        DefaultHttp2Headers headers = new DefaultHttp2Headers();
                        headers.method("GET");
                        headers.path(request.getUri().toString());
                        headers.scheme("https");
                        for (Entry<String, String> entry : request.getHeaders().entries()) {
                            try {
                                headers.set(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
                            }
                            catch (Exception e) {
                                log.error(format("NIKHIL failed setting header: %s. skipping it", entry.getKey()));
                            }
                        }

                        if (request.getMethod().equals("GET")) {
                            // Send headers-only request
                            streamChannel.writeAndFlush(new DefaultHttp2HeadersFrame(headers, true));
                        }
                        else if (request.getMethod().equals("POST")) {
                            // Send headers followed by data
                            streamChannel.write(new DefaultHttp2HeadersFrame(headers, false));
                            byte[] payload = ((StaticBodyGenerator) request.getBodyGenerator()).getBody();
                            streamChannel.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.copiedBuffer(payload), true));
                        }

                        int streamCount = channelStreamCountMap.getOrDefault(channel, 0);
                        if (streamCount < nettyMaxStreamPerChannel) {
                            log.error(format("NIKHIL releasing channel back to pool. streamCount: %d", streamCount));
                            channel.attr(IN_POOL).set(true);
                            pool.release(channel);
                        }
                        else {
                            log.error(format("NIKHIL NOT releasing channel back to pool. streamCount: %d", streamCount));
                        }
                    }
                    else {
                        log.error(format("NIKHIL failed to acquire a channel from the pool. reason: %s", f.cause().getMessage()));
                    }
                }
            });
        }
        catch (Exception e) {
            log.error(format("NIKHIL http request send failure for %s. Message: %s", address.toString(), e.getMessage()));
        }

        return new HttpResponseFuture()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                return listenableFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled()
            {
                return listenableFuture.isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return listenableFuture.isDone();
            }

            @Override
            public Object get()
                    throws InterruptedException, ExecutionException
            {
                return listenableFuture.get();
            }

            @Override
            public Object get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                return listenableFuture.get(timeout, unit);
            }

            @Override
            public void addListener(Runnable listener, Executor executor)
            {
                listenableFuture.addListener(listener, executor);
            }

            @Override
            public String getState()
            {
                return "";
            }
        };
    }

    @Override
    public RequestStats getStats()
    {
        return null;
    }

    @Override
    public long getMaxContentLength()
    {
        return 0;
    }

    @Override
    public void close()
    {
        group.shutdownGracefully();
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    private static class Http2ClientStreamFrameResponseHandler
            extends SimpleChannelInboundHandler<Http2StreamFrame>
    {
        private List<Http2StreamFrame> frames = new ArrayList<>();
        private SettableFuture future;
        private ResponseHandler responseHandler;
        private Http2StreamChannel streamChannel;
        private Channel channel;
        private ChannelPool pool;
        private Map<Channel, Integer> channelStreamCountMap;

        public Http2ClientStreamFrameResponseHandler(
                SettableFuture listenableFuture,
                ResponseHandler responseHandler,
                Http2StreamChannel streamChannel,
                Channel channel,
                ChannelPool pool,
                Map<Channel, Integer> channelStreamCountMap)
        {
            this.future = listenableFuture;
            this.responseHandler = responseHandler;
            this.streamChannel = streamChannel;
            this.channel = channel;
            this.pool = pool;
            this.channelStreamCountMap = channelStreamCountMap;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http2StreamFrame msg)
                throws Exception
        {
            log.error(format("NIKHIL Received HTTP/2 'stream' frame: %s", msg));

            frames.add(msg);
            if ((msg instanceof Http2DataFrame && ((Http2DataFrame) msg).isEndStream()) ||
                    (msg instanceof Http2HeadersFrame && ((Http2HeadersFrame) msg).isEndStream())) {
                constructResponse();
            }
        }

        private void constructResponse()
                throws Exception
        {
            int statusCode = -1;
            ByteBuf content = null;
            int contentLength = 0;
            Http2Headers headers = null;

            for (Http2StreamFrame frame : frames) {
                if (frame instanceof Http2HeadersFrame) {
                    headers = ((Http2HeadersFrame) frame).headers();
                    statusCode = Integer.parseInt(headers.status().toString());
                    for (Entry<CharSequence, CharSequence> entry : headers) {
                        if (entry.getKey().toString().equalsIgnoreCase("content-length")) {
                            contentLength = Integer.parseInt(entry.getValue().toString());
                            log.error(format("NIKHIL content length: %d", contentLength));
                        }
                    }
                }
                else if (frame instanceof Http2DataFrame) {
                    content = ((Http2DataFrame) frame).content();
                }
            }

            byte[] contentResult = new byte[contentLength];
            if (content != null) {
                content.getBytes(0, contentResult, 0, contentLength);
                log.error(format("NIKHIL content.readableBytes: %d, contentLength: %d, content: %s", content.readableBytes(), contentLength, Arrays.toString(contentResult)));
            }

            if (statusCode == 200) {
                log.error(format("NIKHIL received 200 status OK, content:%s", Arrays.toString(contentResult)));
                int finalStatusCode = statusCode;
                int finalContentLength = contentLength;

                ByteBuf finalContent = content;
                Http2Headers finalHeaders = headers;
                boolean result = future.set(responseHandler.handle(null, new Response()
                {
                    @Override
                    public int getStatusCode()
                    {
                        return finalStatusCode;
                    }

                    @Override
                    public ListMultimap<HeaderName, String> getHeaders()
                    {
                        ListMultimap<HeaderName, String> result = ArrayListMultimap.create();
                        Iterator<Entry<CharSequence, CharSequence>> iterator = finalHeaders.iterator();
                        while (iterator.hasNext()) {
                            Entry<CharSequence, CharSequence> entry = iterator.next();
                            if (entry.getKey().toString().equalsIgnoreCase("Content-Type")) {
                                result.put(HeaderName.of("Content-Type"), entry.getValue().toString());
                            }
                            else if (entry.getKey().toString().equalsIgnoreCase("Content-Length")) {
                                result.put(HeaderName.of("Content-Length"), entry.getValue().toString());
                            }
                            else {
                                result.put(HeaderName.of(entry.getKey().toString()), entry.getValue().toString());
                            }
                        }
                        return result;
                    }

                    @Override
                    public long getBytesRead()
                    {
                        return finalContentLength;
                    }

                    @Override
                    public InputStream getInputStream()
                            throws IOException
                    {
                        return new ByteBufInputStream(finalContent);
                    }
                }));
            }
            else {
                log.error(format("NIKHIL received non 200 OK status: %d", statusCode));
                future.set(null);
            }

            // close the stream
            streamChannel.close();

            // decrement the open stream count
            channelStreamCountMap.put(channel, channelStreamCountMap.getOrDefault(channel, 0) - 1);

            // release channel if NOT released earlier
            if (!channel.attr(IN_POOL).get()) {
                log.error("NIKHIL ResponseHandler : releasing channel back to pool");
                channel.attr(IN_POOL).set(true);
                pool.release(channel);
            }
        }
    }
}

//    private static class StreamChannelPool
//    {
//        private final EventExecutor executor;
//        private final Bootstrap bootstrap;
//        private final int maxConnectionsPerHost;
//        private final int maxStreamPerChannel;
//        private AtomicInteger acquiredChannelCount;
//        private AtomicInteger acquiredStreamChannelCount;
//        private Channel channel;
//
//        private ConcurrentLinkedQueue<Http2StreamChannel> queue = new ConcurrentLinkedQueue<>();
//
//        public StreamChannelPool(Bootstrap bootstrap, SslContext sslContext, int maxConnectionsPerHost, int maxStreamPerChannel)
//        {
//            this.executor = bootstrap.config().group().next();
//            this.bootstrap = bootstrap.clone();
//            this.maxConnectionsPerHost = maxConnectionsPerHost;
//            this.maxStreamPerChannel = maxStreamPerChannel;
//            this.acquiredChannelCount = new AtomicInteger(0);
//            this.acquiredStreamChannelCount = new AtomicInteger(0);
//
//            bootstrap.handler(new ChannelInitializer<Channel>()
//            {
//                protected void initChannel(Channel ch)
//                        throws Exception
//                {
//                    // ensure that our 'trust all' SSL handler is the first in the pipeline if SSL is enabled.
//                    ch.pipeline().addFirst(sslContext.newHandler(ch.alloc()));
//
//                    Http2FrameCodec http2FrameCodec = Http2FrameCodecBuilder.forClient()
//                            .initialSettings(Http2Settings.defaultSettings()) // this is the default, but shows it can be changed.
//                            .build();
//                    ch.pipeline().addLast(http2FrameCodec);
//                    ch.pipeline().addLast(new Http2MultiplexHandler(new SimpleChannelInboundHandler()
//                    {
//                        @Override
//                        protected void channelRead0(ChannelHandlerContext ctx, Object msg)
//                        {
//                            // NOOP (this is the handler for 'inbound' streams, which is not relevant in this example)
//                        }
//                    }));
//                }
//            });
//        }
//
//        public Http2StreamChannel getStreamChannel()
//        {
//            if (channel == null || (acquiredStreamChannelCount == maxStreamPerChannel && acquiredChannelCount.incrementAndGet() < maxConnectionsPerHost)) {
//                // Start the client.
//                Channel channel = bootstrap.connect().syncUninterruptibly().channel();
//                Http2StreamChannelBootstrap streamChannelBootstrap = new Http2StreamChannelBootstrap(channel);
//                return streamChannelBootstrap.open().syncUninterruptibly().getNow();
//            }
//        }
//
//        public Future<Channel> acquire(final Promise<Channel> promise) {
//            try {
//                if (executor.inEventLoop()) {
//                    acquire0(promise);
//                } else {
//                    executor.execute(new Runnable() {
//                        @Override
//                        public void run() {
//                            acquire0(promise);
//                        }
//                    });
//                }
//            } catch (Throwable cause) {
//                promise.tryFailure(cause);
//            }
//            return promise;
//        }
//
//        private void acquire0(final Promise<Channel> promise) {
//            try {
//                assert executor.inEventLoop();
//
//                if (acquiredChannelCount.get() < maxConnectionsPerHost) {
//                    assert acquiredChannelCount.get() >= 0;
//
//                    // We need to create a new promise as we need to ensure the AcquireListener runs in the correct
//                    // EventLoop
//                    Promise<Channel> p = executor.newPromise();
//                    FixedChannelPool.AcquireListener l = new FixedChannelPool.AcquireListener(promise);
//                    l.acquired();
//                    p.addListener(l);
//                    super.acquire(p);
//                } else {
//                    if (pendingAcquireCount >= maxPendingAcquires) {
//                        tooManyOutstanding(promise);
//                    } else {
//                        FixedChannelPool.AcquireTask task = new FixedChannelPool.AcquireTask(promise);
//                        if (pendingAcquireQueue.offer(task)) {
//                            ++pendingAcquireCount;
//
//                            if (timeoutTask != null) {
//                                task.timeoutFuture = executor.schedule(timeoutTask, acquireTimeoutNanos,
//                                        TimeUnit.NANOSECONDS);
//                            }
//                        } else {
//                            tooManyOutstanding(promise);
//                        }
//                    }
//
//                    assert pendingAcquireCount > 0;
//                }
//            } catch (Throwable cause) {
//                promise.tryFailure(cause);
//            }
//
//        public void release(Http2StreamChannel streamChannel)
//        {
//            queue.add(streamChannel);
//        }
//    }
