package base.nio

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

class Client {

  def main(args: Array[String]) {
    val sc: SocketChannel = SocketChannel.open
    sc.connect(new InetSocketAddress("192.168.6.52", 9999))

    val localHost = sc.socket.getLocalAddress.getHostAddress
    val localPort = sc.socket.getLocalPort
    val remoteHost = sc.socket.getInetAddress.getHostAddress
    val remotePort = sc.socket.getPort
    val connectionId = localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort
    //10.57.2.138:61088-192.168.6.52:9999
    println(connectionId)

    sc.write(ByteBuffer.wrap(new String("I'm a client").getBytes))
  }
}