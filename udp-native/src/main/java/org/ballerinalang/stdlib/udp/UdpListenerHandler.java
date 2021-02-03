/*
 * Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.stdlib.udp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

import java.util.LinkedList;

/**
 * {@link UdpListenerHandler} is a ChannelInboundHandler implementation for udp listener.
 */
public class UdpListenerHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private UdpService udpService;
    protected LinkedList<WriteCallbackService> writeCallbackServices = new LinkedList<>();

    public UdpListenerHandler(UdpService udpService) {
        this.udpService = udpService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx,
                                DatagramPacket datagramPacket) throws Exception {
        Dispatcher.invokeRead(udpService, datagramPacket, ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Dispatcher.invokeOnError(udpService, cause.getMessage());
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable() && writeCallbackServices.size() > 0) {
            WriteCallbackService writeCallbackService = writeCallbackServices.getFirst();
            if (writeCallbackService != null) {
                writeCallbackService.writeFragments();
                if (writeCallbackService.isWriteCalledForAllFragments()) {
                    writeCallbackServices.remove(writeCallbackService);
                }
            }
        }
    }

    public void addWriteCallback(WriteCallbackService writeCallbackService) {
        writeCallbackServices.addLast(writeCallbackService);
    }
}
