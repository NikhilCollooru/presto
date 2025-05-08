package com.facebook.presto.server.remotetask;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.security.pem.PemReader.loadPrivateKey;
import static com.facebook.airlift.security.pem.PemReader.readCertificateChain;

public class TestNettyHttpClient
{
    public static void main(String[] args)
            throws Exception
    {
        // Create an EventLoopGroup to handle the client's event loop
        EventLoopGroup group = new NioEventLoopGroup(200);
        try {
            File keyFile = new File("/var/facebook/x509_identities/client.pem");
            File trustCertificateFile = new File("/var/facebook/rootcanal/ca.pem");
            List<String> ciphers = Arrays.asList("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_256_GCM_SHA384");
            PrivateKey privateKey = loadPrivateKey(keyFile, Optional.of("password"));
            X509Certificate[] certificateChain = readCertificateChain(keyFile).toArray(new X509Certificate[0]);
            X509Certificate[] trustChain = readCertificateChain(trustCertificateFile).toArray(new X509Certificate[0]);

            // Create a Bootstrap instance to configure the client
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel ch)
                                throws Exception
                        {
                            // Add the HttpClientCodec to the pipeline
                            ch.pipeline().addLast(new HttpClientCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(50000000));
                            // Add the SSL handler to the pipeline
                            SslContext sslCtx = SslContextBuilder.forClient()
                                    .keyManager(privateKey, certificateChain)
                                    .trustManager(trustChain)
                                    .ciphers(ciphers)
                                    .build();
                            ch.pipeline().addFirst(sslCtx.newHandler(ch.alloc()));
                            // Add a handler to print the response
                            ch.pipeline().addLast(new HttpResponseHandler());
                        }
                    });
            System.out.println("here1");

            ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap = new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
                @Override
                protected SimpleChannelPool newPool(InetSocketAddress key) {
                    return new SimpleChannelPool(bootstrap.remoteAddress(key), null);
                }
            };

            // Connect to the remote server
            String host = "twshared37023.03.vll5.facebook.com";
            int port = 7778;

            // depending on when you use addr1 or addr2 you will get different pools.
            final SimpleChannelPool pool = poolMap.get(new InetSocketAddress(host, port));
            Future<Channel> f = pool.acquire();
            f.addListener(new FutureListener<Channel>() {
                @Override
                public void operationComplete(Future<Channel> f) {
                    if (f.isSuccess()) {
                        Channel channel = f.getNow();
                        System.out.println("inside");
                        // Send a GET request to the server
                        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/task/20250425_220156_13048_ys8jj.1.0.1.0/status");
                        request.headers().set("Host", host);
                        request.headers().set("Content-Type", "application/json");
                        request.headers().set("User-Agent", "NettyClient/1.0");
                        request.headers().set("Accept", "*/*");
                        request.headers().set("Connection", "keep-alive");
                        channel.writeAndFlush(request);

                        // Release back to pool
                        pool.release(channel);
                    }
                }
            });

            Channel channel = bootstrap.connect(host, port).sync().channel();
            System.out.println("here2");
            // Send a GET request to the server
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/task/20250425_220156_13048_ys8jj.1.0.1.0/status");
            request.headers().set("Host", host);
            request.headers().set("Content-Type", "application/json");
            request.headers().set("User-Agent", "NettyClient/1.0");
            request.headers().set("Accept", "*/*");
            request.headers().set("Connection", "keep-alive");
            channel.writeAndFlush(request);
            System.out.println("here3");
            // Wait for the response
            channel.closeFuture().sync();
            System.out.println("here4");
        }
        finally {
            // Shut down the EventLoopGroup
            group.shutdownGracefully();
        }
    }

    private static class HttpResponseHandler2
            extends ChannelInboundHandlerAdapter
    {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg)
                throws Exception
        {
            if (msg instanceof FullHttpResponse || msg instanceof DefaultHttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                System.out.println("Response: " + response);
                if (response.status().code() == 302) {
                    String location = response.headers().get(HttpHeaders.Names.LOCATION);
                    if (location != null) {
                        System.out.println("Redirecting to: " + location);
                        URI uri = new URI(location);
                        // Handle the redirect by making a new request to the location
                        // This is a simplified example; in practice, you may need to handle cookies and other headers
                        DefaultFullHttpRequest newRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
                        newRequest.headers().set(HttpHeaders.Names.HOST, uri.getHost());
                        newRequest.headers().set(HttpHeaders.Names.USER_AGENT, "NettyClient/1.0");
                        newRequest.headers().set(HttpHeaders.Names.ACCEPT, "*/*");
                        newRequest.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                        ctx.channel().writeAndFlush(newRequest);
                    }
                }
            }
        }
    }

    private static class HttpResponseHandler
            extends SimpleChannelInboundHandler<FullHttpResponse>
    {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg)
                throws Exception
        {
            // Print the response status
            System.out.println("Response Status: " + msg.getStatus());
            // Print the response headers
            System.out.println("Response Headers:");
            System.out.println(msg.headers());
            // Print the response body
            System.out.println("Response Body:");
            System.out.println(msg.content().toString(io.netty.util.CharsetUtil.UTF_8));
        }
    }
}
