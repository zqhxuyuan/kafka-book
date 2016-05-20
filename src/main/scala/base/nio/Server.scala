package base.nio

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}

/**
  * Created by zhengqh on 16/5/6.
  */
class Server {


  def main(args: Array[String]) {
    val serverChannel: ServerSocketChannel = ServerSocketChannel.open
    serverChannel.bind(new InetSocketAddress(9999))
    val sc: SocketChannel = serverChannel.accept

    val localHost = sc.socket.getLocalAddress.getHostAddress
    val localPort = sc.socket.getLocalPort
    val remoteHost = sc.socket.getInetAddress.getHostAddress
    val remotePort = sc.socket.getPort
    val connectionId = localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort
    //192.168.6.52:9999-10.57.2.138:61088
    println(connectionId)

    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    val size: Int = sc.read(buffer)
    System.out.println("read size:" + size)
  }
}
