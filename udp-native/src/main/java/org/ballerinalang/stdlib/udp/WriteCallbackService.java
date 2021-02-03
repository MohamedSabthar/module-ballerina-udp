package org.ballerinalang.stdlib.udp;

import io.ballerina.runtime.api.Future;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.PromiseCombiner;

import java.util.LinkedList;

/**
 * WriteCallbackService used to write datagram fragments via channelPipeline.
 */
public class WriteCallbackService {
    Channel channel;
    LinkedList<DatagramPacket> fragments;
    Future callback;
    PromiseCombiner promiseCombiner;
    UdpService udpService;

    WriteCallbackService(DatagramPacket datagram, Future callback, Channel channel) {
        this.fragments = fragmentDatagram(datagram);
        this.promiseCombiner = new PromiseCombiner(ImmediateEventExecutor.INSTANCE);
        this.callback = callback;
        this.channel = channel;
    }

    public WriteCallbackService(DatagramPacket datagram, UdpService udpService, Channel channel) {
        fragments = fragmentDatagram(datagram);
        promiseCombiner = new PromiseCombiner(ImmediateEventExecutor.INSTANCE);
        this.udpService = udpService;
        this.channel = channel;
    }

    public synchronized void writeFragments() {
        while (fragments.size() > 0) {
            if (channel.isWritable()) {
                promiseCombiner.add(channel.writeAndFlush(fragments.poll()));
            } else {
                break;
            }
        }

        if (isWriteCalledForAllFragments()) {
            if (callback != null) {
                completeCallback();
            } else {
                callDispatch();
            }
        }
    }

    private LinkedList<DatagramPacket> fragmentDatagram(DatagramPacket datagram) {
        ByteBuf content = datagram.content();
        int contentSize = content.readableBytes();
        LinkedList<DatagramPacket> fragments = new LinkedList<>();

        while (contentSize > 0) {
            if (contentSize > Constants.DATAGRAM_DATA_SIZE) {
                fragments.add(datagram.replace(datagram.content().readBytes(Constants.DATAGRAM_DATA_SIZE)));
                contentSize -= Constants.DATAGRAM_DATA_SIZE;
            } else {
                fragments.add(datagram.replace(datagram.content().readBytes(contentSize)));
                contentSize = 0;
            }
        }
        return fragments;
    }

    private void completeCallback() {
        promiseCombiner.finish(channel.newPromise().addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                callback.complete(null);
            } else {
                callback.complete(Utils
                        .createSocketError("Failed to send data: " + future.cause().getMessage()));
            }
        }));
    }

    private void callDispatch() {
        promiseCombiner.finish(channel.newPromise().addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                Dispatcher.invokeOnError(udpService, "Failed to send data.");
            }
        }));
    }

    public boolean isWriteCalledForAllFragments() {
        return fragments.size() == 0;
    }
}
