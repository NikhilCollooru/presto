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
import io.netty.buffer.ByteBufInputStream;
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
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.security.pem.PemReader.loadPrivateKey;
import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;
import static java.lang.String.format;

public class NettyHttpClient
        implements HttpClient, Closeable
{
    private static final Logger log = Logger.get(NettyHttpClient.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup group;
    private SslContext sslContext;

    // Map with key: inetAddress, value : FixeChannelPool
    private final ChannelPoolMap<InetSocketAddress, FixedChannelPool> poolMap;

    @Inject
    public NettyHttpClient(TaskManagerConfig config)
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

        File keyFile = new File("/var/facebook/x509_identities/client.pem");
        File trustCertificateFile = new File("/var/facebook/rootcanal/ca.pem");

        // Comment out the following if you want to try with HiveQueryRunner
        List<String> ciphers = Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384");
        try {
            PrivateKey privateKey = loadPrivateKey(keyFile, Optional.of("password"));
            X509Certificate[] certificateChain = readCertificateChain(keyFile).toArray(new X509Certificate[0]);
            X509Certificate[] trustChain = readCertificateChain(trustCertificateFile).toArray(new X509Certificate[0]);
            this.sslContext = SslContextBuilder.forClient()
                    .keyManager(privateKey, certificateChain)
                    .trustManager(trustChain)
                    .ciphers(ciphers)
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
                        // Add the HttpClientCodec to the pipeline
                        ch.pipeline().addLast(new HttpClientCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(50000000));

                        // Add the SSL handler to the pipeline. Comment out this if running HiveQueryRunner
                        ch.pipeline().addFirst(sslContext.newHandler(ch.alloc()));
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

                        // Send a GET request to the server
                        DefaultFullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, request.getUri().toString());

                        // If its a POST request update the content and content-length
                        if (request.getMethod().equals("POST")) {
                            byte[] payload = ((StaticBodyGenerator) request.getBodyGenerator()).getBody();

                            // Create a new POST httpRequest
                            httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, request.getUri().toString());

                            // Write the POST payload into content field of the request
                            httpRequest.content().writeBytes(payload);

                            // Add content length as header, else the server will think nothing is there in content
                            httpRequest.headers().setInt("Content-Length", payload.length);
                        }

                        // Response Handler. Call the de-serializer to unwrap the response json
                        channel.pipeline().addLast(new HttpResponseHandler(listenableFuture, responseHandler, channel, pool));

                        // Add all the headers
                        httpRequest.headers().set("Host", request.getUri().getHost());
                        httpRequest.headers().set("Content-Type", "application/json;charset=utf-8");
                        httpRequest.headers().set("User-Agent", "NettyClient/1.0");
                        httpRequest.headers().set("Accept", "*/*");

                        // Do not close the connection when using connection pool. Because the whole point is to save connection creation cost
                        // httpRequest.headers().set("Connection", "close");
                        httpRequest.headers().set("Connection", "keep-alive");

                        for (Map.Entry<String, String> entry : request.getHeaders().entries()) {
                            httpRequest.headers().set(entry.getKey(), entry.getValue());
                        }

                        // Send the request to server
                        channel.writeAndFlush(httpRequest);
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

    private static class HttpResponseHandler
            extends SimpleChannelInboundHandler<FullHttpResponse>
    {
        SettableFuture future;
        ResponseHandler responseHandler;
        Channel channel;
        ChannelPool pool;

        public HttpResponseHandler(SettableFuture listenableFuture, ResponseHandler responseHandler, Channel channel, ChannelPool pool)
        {
            this.future = listenableFuture;
            this.responseHandler = responseHandler;
            this.channel = channel;
            this.pool = pool;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg)
                throws Exception
        {
//            // Print the response status
//            System.out.println("Response Status: " + msg.getStatus());
//            // Print the response headers
//            System.out.println("Response Headers:");
//            System.out.println(msg.headers());
//            // Print the response body
//            System.out.println("Response Body:");
//            System.out.println(msg.content().toString(io.netty.util.CharsetUtil.UTF_8));
            if (msg.status().code() == 200) {
                boolean isDone = false;
                boolean isCancelled = false;
                if (future.isDone()) {
                    isDone = true;
                    log.error("NIKHIL future already done");
                }
                if (future.isCancelled()) {
                    isCancelled = true;
                    log.error("NIKHIL future is cancelled");
                }

                boolean result = future.set(responseHandler.handle(null, new Response()
                {
                    @Override
                    public int getStatusCode()
                    {
                        return msg.getStatus().code();
                    }

                    @Override
                    public ListMultimap<HeaderName, String> getHeaders()
                    {
                        ListMultimap<HeaderName, String> result = ArrayListMultimap.create();
                        Iterator<Map.Entry<String, String>> iterator = msg.headers().iteratorAsString();
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> entry = iterator.next();
                            result.put(HeaderName.of(entry.getKey()), entry.getValue());
                        }
                        return result;
                    }

                    @Override
                    public long getBytesRead()
                    {
                        return Integer.parseInt(msg.headers().get("Content-Length"));
                    }

                    @Override
                    public InputStream getInputStream()
                            throws IOException
                    {
                        return new ByteBufInputStream(msg.content());
                    }
                }));

                if (!result) {
                    log.error(format("NIKHIL error setting the server result on the future. isDone: %s, isCancelled: %s", isDone, isCancelled));
                }
            }
            else {
                log.error(format("NIKHIL Non-Success response from Server. Check Details: %s", msg.content().toString(io.netty.util.CharsetUtil.UTF_8)));
                future.set(null);
            }

            // Remove the response handler (HttpResponseHandler) from pipeline before releasing the channel back to pool
            channel.pipeline().removeLast();

            // release channel
            pool.release(channel);
        }
    }
}
