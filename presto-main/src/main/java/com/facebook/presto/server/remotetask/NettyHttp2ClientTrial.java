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
//package com.facebook.presto.server.remotetask;
//
//import com.facebook.airlift.log.Logger;
//import com.facebook.presto.execution.TaskManagerConfig;
//import io.netty.handler.ssl.OpenSsl;
//import io.netty.handler.ssl.SslProvider;
//
//import javax.inject.Inject;
//
//public class NettyHttp2Client
//{
//    private static final Logger log = Logger.get(NettyHttp2Client.class);
//
//    @Inject
//    public NettyHttp2Client(TaskManagerConfig config)
//    {
//        SslProvider provider = OpenSsl.isAlpnSupported() ? SslProvider.OPENSSL : SslProvider.JDK;
//    }
//}

package com.facebook.presto.server.remotetask;

//import io.netty.bootstrap.Bootstrap;
//import io.netty.buffer.ByteBuf;
//import io.netty.buffer.Unpooled;
//import io.netty.channel.*;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.SocketChannel;
//import io.netty.channel.socket.nio.NioSocketChannel;
//import io.netty.handler.codec.http.*;
//import io.netty.handler.codec.http2.*;
//import io.netty.handler.ssl.*;
//import io.netty.util.CharsetUtil;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.stream.Collectors;
//import javax.net.ssl.SSLException;

///**
// * A Netty-based HTTP/2 client supporting both secure (h2) and cleartext (h2c) connections.
// *
// * <p>This client leverages HTTP/2's multiplexing capabilities to send multiple requests
// * concurrently over a single connection, improving performance by eliminating connection
// * establishment overhead and reducing latency.
// *
// * <p>HTTP/2 multiplexing allows multiple requests and responses to be sent in parallel over a
// * single connection without blocking each other, significantly improving throughput.
// *
// * <p>HTTP/2 stream prioritization allows clients to specify the relative importance of requests,
// * helping servers allocate resources efficiently. Priorities are expressed through weight (1-256)
// * and dependencies between streams. This enables more important resources (like HTML) to be
// * delivered before less critical ones (like images), improving perceived page load times.
// */
public class NettyHttp2ClientTrial
{
//    private final EventLoopGroup workerGroup;
//    private Channel channel;
//    private Http2Connection connection;
//    private Http2ConnectionEncoder encoder;
//    private Http2ConnectionDecoder decoder;
//    private Http2FrameWriter frameWriter;
//    private int streamId;
//    private final Map<Integer, CompletableFuture<FullHttpResponse>> responsePromises;
//    private final AtomicReference<String> connectionState = new AtomicReference<>("DISCONNECTED");
//
//    /** Initializes the HTTP/2 client. */
//    public NettyHttp2ClientTrial() {
//        this.workerGroup = new NioEventLoopGroup();
//        this.streamId = 3; // Stream ID 1 is used for the initial HTTP/2 connection
//        this.responsePromises = new ConcurrentHashMap<>();
//    }
//
//    /**
//     * Connects to an HTTP/2 server using the specified URI. Supports both secure (h2) and cleartext
//     * (h2c) connections.
//     *
//     * @param uri The URI to connect to
//     * @return A CompletableFuture that completes when the connection is established
//     */
//    public CompletableFuture<Void> connect(URI uri) {
//        CompletableFuture<Void> connectFuture = new CompletableFuture<>();
//        boolean ssl = uri.getScheme().equalsIgnoreCase("https");
//        int port = uri.getPort() > 0 ? uri.getPort() : (ssl ? 443 : 80);
//
//        try {
//            Bootstrap bootstrap = new Bootstrap();
//            bootstrap
//                    .group(workerGroup)
//                    .channel(NioSocketChannel.class)
//                    .option(ChannelOption.SO_KEEPALIVE, true)
//                    .handler(
//                            new ChannelInitializer<SocketChannel>() {
//                                @Override
//                                protected void initChannel(SocketChannel ch) throws Exception {
//                                    if (ssl) {
//                                        configureSsl(ch, uri.getHost());
//                                    } else {
//                                        configureCleartext(ch);
//                                    }
//                                }
//                            });
//
//            // Connect to the server
//            ChannelFuture channelFuture = bootstrap.connect(uri.getHost(), port);
//            channelFuture.addListener(
//                    (ChannelFutureListener)
//                            future -> {
//                                if (future.isSuccess()) {
//                                    channel = future.channel();
//                                    connectionState.set("CONNECTED");
//                                    connectFuture.complete(null);
//                                } else {
//                                    connectFuture.completeExceptionally(future.cause());
//                                }
//                            });
//        } catch (Exception e) {
//            connectFuture.completeExceptionally(e);
//        }
//
//        return connectFuture;
//    }
//
//    /** Configures the channel for SSL/TLS with ALPN for HTTP/2 support. */
//    private void configureSsl(SocketChannel ch, String host) throws SSLException {
//        SslContext sslContext =
//                SslContextBuilder.forClient()
//                        .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
//                        .applicationProtocolConfig(
//                                new ApplicationProtocolConfig(
//                                        ApplicationProtocolConfig.Protocol.ALPN,
//                                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
//                                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
//                                        ApplicationProtocolNames.HTTP_2))
//                        .build();
//
//        SslHandler sslHandler = sslContext.newHandler(ch.alloc(), host, 443);
//        Http2FrameCodecBuilder frameCodecBuilder = Http2FrameCodecBuilder.forClient();
//        Http2FrameCodec frameCodec = frameCodecBuilder.build();
//
//        // Store the connection and encoder/decoder for priority settings
//        this.connection = frameCodecBuilder.connection();
//
//        // Store encoder and decoder for request handling
//        this.encoder = frameCodecBuilder.encoder();
//        this.decoder = frameCodecBuilder.decoder();
//
//        // Store the frame writer for priority settings
//        this.frameWriter = frameCodecBuilder.frameWriter();
//
//        ch.pipeline().addLast(sslHandler, frameCodec, new Http2ClientResponseHandler());
//    }
//
//    /** Configures the channel for cleartext HTTP/2 (h2c) connections. */
//    private void configureCleartext(SocketChannel ch) {
//        Http2FrameCodecBuilder frameCodecBuilder = Http2FrameCodecBuilder.forClient();
//        Http2FrameCodec frameCodec = frameCodecBuilder.build();
//
//        // Store the connection and encoder/decoder for priority settings
//        this.connection = frameCodecBuilder.connection();
//
//        // Store encoder and decoder for request handling
//        this.encoder = frameCodecBuilder.encoder();
//        this.decoder = frameCodecBuilder.decoder();
//
//        // Store the frame writer for priority settings
//        this.frameWriter = frameCodecBuilder.frameWriter();
//
//        ch.pipeline().addLast(frameCodec, new Http2ClientResponseHandler());
//    }
//
//    /**
//     * Returns the current connection state.
//     *
//     * @return A string representing the current connection state ("CONNECTED" or "DISCONNECTED")
//     */
//    public String getConnectionState() {
//        return connectionState.get();
//    }
//
//    /**
//     * Sends a GET request to the specified path.
//     *
//     * @param path The path to send the request to
//     * @return A CompletableFuture that completes with the response
//     */
//    public CompletableFuture<FullHttpResponse> get(String path) {
//        return sendRequest(path, HttpMethod.GET, null);
//    }
//
//    /**
//     * Sends a POST request to the specified path with the given content.
//     *
//     * @param path The path to send the request to
//     * @param content The content to send
//     * @return A CompletableFuture that completes with the response
//     */
//    public CompletableFuture<FullHttpResponse> post(String path, String content) {
//        ByteBuf data = Unpooled.copiedBuffer(content, CharsetUtil.UTF_8);
//        DefaultHttp2Headers headers =
//                new DefaultHttp2Headers()
//                        .method(HttpMethod.POST.name())
//                        .path(path)
//                        .scheme("https")
//                        .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
//                        .set(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(data.readableBytes()));
//        return sendRequest(path, HttpMethod.POST, data, headers);
//    }
//
//    /**
//     * Sends multiple GET requests concurrently, leveraging HTTP/2 multiplexing.
//     *
//     * @param paths List of paths to request concurrently
//     * @return A CompletableFuture that completes with a list of responses in the same order as the
//     *     requests
//     */
//    public CompletableFuture<List<FullHttpResponse>> getMultiple(List<String> paths) {
//        List<CompletableFuture<FullHttpResponse>> futures =
//                paths.stream().map(this::get).collect(Collectors.toList());
//
//        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
//                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
//    }
//
//    /**
//     * Sends multiple POST requests concurrently, leveraging HTTP/2 multiplexing.
//     *
//     * @param pathsAndContents Map of paths to their corresponding content
//     * @return A CompletableFuture that completes with a list of responses
//     */
//    public CompletableFuture<List<FullHttpResponse>> postMultiple(
//            Map<String, String> pathsAndContents) {
//        List<CompletableFuture<FullHttpResponse>> futures = new ArrayList<>();
//
//        for (Map.Entry<String, String> entry : pathsAndContents.entrySet()) {
//            futures.add(post(entry.getKey(), entry.getValue()));
//        }
//
//        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
//                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
//    }
//
//    /**
//     * Sets the priority for a specific stream. HTTP/2 allows prioritization of streams to optimize
//     * resource allocation.
//     *
//     * @param streamId The stream ID to set priority for
//     * @param weight Weight between 1 and 256 (higher means higher priority)
//     * @param dependencyStreamId The stream this stream depends on (0 for no dependency)
//     * @param exclusive Whether this stream should be the exclusive dependent of its parent
//     * @return A ChannelFuture that completes when the priority is set
//     */
//    public ChannelFuture setPriority(
//            int streamId, int weight, int dependencyStreamId, boolean exclusive) {
//        if (channel == null || !channel.isActive()) {
//            return channel.newFailedFuture(new IllegalStateException("Client not connected"));
//        }
//
//        Http2Stream stream = connection.stream(streamId);
//        if (stream == null) {
//            return channel.newFailedFuture(
//                    new IllegalArgumentException("Stream does not exist: " + streamId));
//        }
//
//        return frameWriter.writePriority(
//                channel, streamId, dependencyStreamId, weight, exclusive, channel.newPromise());
//    }
//
//    /**
//     * Sends an HTTP request with the specified method and content.
//     *
//     * @param path The path to send the request to
//     * @param method The HTTP method to use
//     * @param content The content to send (may be null)
//     * @return A CompletableFuture that completes with the response
//     */
//    private CompletableFuture<FullHttpResponse> sendRequest(
//            String path, HttpMethod method, ByteBuf content) {
//        DefaultHttp2Headers headers =
//                new DefaultHttp2Headers().method(method.name()).path(path).scheme("https");
//        return sendRequest(path, method, content, headers);
//    }
//
//    /**
//     * Sends an HTTP request with the specified method, content, and headers.
//     *
//     * @param path The path to send the request to
//     * @param method The HTTP method to use
//     * @param content The content to send (may be null)
//     * @param headers The HTTP headers to use
//     * @return A CompletableFuture that completes with the response
//     */
//    private CompletableFuture<FullHttpResponse> sendRequest(
//            String path, HttpMethod method, ByteBuf content, Http2Headers headers) {
//        if (channel == null || !channel.isActive()) {
//            return CompletableFuture.failedFuture(new IllegalStateException("Client not connected"));
//        }
//
//        int currentStreamId = streamId;
//        streamId += 2; // Client-initiated streams are odd-numbered
//
//        CompletableFuture<FullHttpResponse> responseFuture = new CompletableFuture<>();
//        responsePromises.put(currentStreamId, responseFuture);
//
//        // Send the request
//        ChannelPromise promise = channel.newPromise();
//        if (content == null) {
//            // Send headers only for GET requests
//            channel.writeAndFlush(
//                    new DefaultHttp2HeadersFrame(headers, true).stream(currentStreamId), promise);
//        } else {
//            // Send headers and data for POST requests
//            channel.write(new DefaultHttp2HeadersFrame(headers, false).stream(currentStreamId), promise);
//            channel.writeAndFlush(
//                    new DefaultHttp2DataFrame(content, true).stream(currentStreamId), promise);
//        }
//
//        promise.addListener(
//                (ChannelFutureListener)
//                        future -> {
//                            if (!future.isSuccess()) {
//                                responsePromises.remove(currentStreamId);
//                                responseFuture.completeExceptionally(future.cause());
//                            }
//                        });
//
//        return responseFuture;
//    }
//
//    /** Closes the client and releases all resources. */
//    public void close() {
//        if (channel != null) {
//            connectionState.set("DISCONNECTED");
//            channel.close();
//        }
//        workerGroup.shutdownGracefully();
//    }
//
//    /** Handler for HTTP/2 responses. */
//    private class Http2ClientResponseHandler
//          extends ChannelInboundHandlerAdapter
//     {
//        private final Map<Integer, ByteBuf> responseBuffers = new ConcurrentHashMap<>();
//        private final Map<Integer, Http2Headers> responseHeaders = new ConcurrentHashMap<>();
//
//        @Override
//        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//            if (msg instanceof Http2HeadersFrame) {
//                Http2HeadersFrame headersFrame = (Http2HeadersFrame) msg;
//                int streamId = headersFrame.stream().id();
//                responseHeaders.put(streamId, headersFrame.headers());
//
//                if (headersFrame.isEndStream()) {
//                    // Headers only response
//                    completeResponse(streamId, Unpooled.EMPTY_BUFFER);
//                }
//            } else if (msg instanceof Http2DataFrame) {
//                Http2DataFrame dataFrame = (Http2DataFrame) msg;
//                int streamId = dataFrame.stream().id();
//
//                ByteBuf content = dataFrame.content();
//                ByteBuf existingContent = responseBuffers.getOrDefault(streamId, Unpooled.buffer());
//                existingContent.writeBytes(content);
//                responseBuffers.put(streamId, existingContent);
//
//                if (dataFrame.isEndStream()) {
//                    completeResponse(streamId, existingContent);
//                }
//            } else {
//                ctx.fireChannelRead(msg);
//            }
//        }
//
//        private void completeResponse(int streamId, ByteBuf content) {
//            CompletableFuture<FullHttpResponse> promise = responsePromises.remove(streamId);
//            if (promise != null) {
//                Http2Headers headers = responseHeaders.remove(streamId);
//                responseBuffers.remove(streamId);
//
//                if (headers != null) {
//                    DefaultFullHttpResponse response =
//                            new DefaultFullHttpResponse(
//                                    HttpVersion.HTTP_1_1,
//                                    HttpResponseStatus.valueOf(headers.status().toString()),
//                                    content);
//
//                    // Convert HTTP/2 headers to HTTP/1.1 headers
//                    headers.forEach(
//                            entry -> {
//                                response.headers().set(entry.getKey().toString(), entry.getValue().toString());
//                            });
//
//                    promise.complete(response);
//                } else {
//                    promise.completeExceptionally(
//                            new RuntimeException("No headers received for stream: " + streamId));
//                }
//            }
//        }
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
//            cause.printStackTrace();
//            ctx.close();
//        }
//    }
//
//    /** Example usage of the HTTP/2 client. */
//    public static void main(String[] args) {
//        NettyHttp2ClientTrial client = new NettyHttp2ClientTrial();
//        try {
//            // Connect to an HTTP/2 server
//            URI uri = new URI("https://nghttp2.org");
//            client.connect(uri).get();
//
//            // Demonstrate HTTP/2 multiplexing with concurrent requests
//            System.out.println("Sending multiple concurrent requests...");
//
//            // Create a list of paths to request
//            List<String> paths = new ArrayList<>();
//            paths.add("/");
//            paths.add("/blog/");
//            paths.add("/documentation/");
//
//            // Send multiple GET requests concurrently
//            CompletableFuture<List<FullHttpResponse>> multipleResponses = client.getMultiple(paths);
//            List<FullHttpResponse> responses = multipleResponses.get();
//
//            // Print all responses
//            for (int i = 0; i < responses.size(); i++) {
//                FullHttpResponse response = responses.get(i);
//                System.out.println("\nResponse for " + paths.get(i) + ":");
//                System.out.println("Status: " + response.status());
//                System.out.println("Content length: " + response.content().readableBytes());
//            }
//
//            // Demonstrate setting stream priorities
//            System.out.println("\nSetting stream priorities...");
//            client.setPriority(3, 200, 0, false); // Higher weight for first stream
//            client.setPriority(5, 100, 0, false); // Lower weight for second stream
//
//            // Demonstrate POST requests with multiplexing
//            System.out.println("\nSending multiple concurrent POST requests...");
//            Map<String, String> postsData = new ConcurrentHashMap<>();
//            postsData.put("/httpbin/post", "{\"message\":\"Hello HTTP/2\"}");
//            postsData.put("/httpbin/post2", "{\"message\":\"Multiplexed request\"}");
//
//            CompletableFuture<List<FullHttpResponse>> postResponses = client.postMultiple(postsData);
//            List<FullHttpResponse> postResults = postResponses.get();
//
//            for (FullHttpResponse postResponse : postResults) {
//                System.out.println("\nPOST Response:");
//                System.out.println("Status: " + postResponse.status());
//                System.out.println("Content: " + postResponse.content().toString(CharsetUtil.UTF_8));
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            // Close the client
//            client.close();
//        }
//    }
}
