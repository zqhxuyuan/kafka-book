package base.nio.scalableIO.reactor;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import static base.nio.scalableIO.Logger.err;
import static base.nio.scalableIO.Logger.log;
import static base.nio.scalableIO.ServerContext.execute;
import static base.nio.scalableIO.ServerContext.useThreadPool;

/**
 * Handlers������
 * <ul>
 * <li>�����������/дIO�¼�����Ӧ��ҵ���߼�</li>
 * <li>����AWT�е�ActionListeners������</li>
 * </ul>
 */
public abstract class Handler extends Thread {

	private enum State{
		CONNECTING(0),
		READING(SelectionKey.OP_READ),
		PROCESSING(2),
		WRITING(SelectionKey.OP_WRITE);
		
		private final int opBit;
		private State(int operateBit){
			opBit = operateBit;
		}
	}
	
	private State state;
	protected final SocketChannel clientChannel;
	protected final SelectionKey key;
	
	protected final ByteBuffer readBuf;
	protected final StringBuilder readData = new StringBuilder();
	protected ByteBuffer writeBuf;
	
	public Handler(Selector selector, SocketChannel clientChannel){
		this.state = State.CONNECTING;
		SelectionKey key = null;
		try {
			clientChannel.configureBlocking(false);
			//������ʹ��subSelector��ʱ���������Ϊʲô������Ϊʹ����������select�������������Ĳſ���
			//�����ʹ��reactor�صĻ���������Ϊ��ҪserverChannelע��selector��accept�¼����������Ӧ�ϲſ���ͨ������������
			key = clientChannel.register(selector, this.state.opBit);
			key.attach(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.clientChannel = clientChannel;
		this.key = key;
		this.readBuf = ByteBuffer.allocate(byteBufferSize());
		log(selector+" connect success...");
	}
	
	@Override
	public void run() {
		switch (state) {
			case CONNECTING:
				connect();
				break;
			case READING:
				readAndProcess();
				break;
			case WRITING:
				write();
				break;
			default:
				err("\nUnsupported State: "+state+" ! overlap processing with IO...");
		}
	}
	
	private void connect() {
		interestOps(State.READING);
	}

	/**
	 * But harder to overlap processing with IO<br/>
	 * Best when can first read all input a buffer<br/>
	 * <br>
	 * That why we used synchronized on read method!<br/>
	 * Just to protected read buffer And handler state...<br/>
	 * <br>
	 * ��ʵ���Ǻ����ص�IO�͹����̴߳���һ�£�����Reactor���̶߳�ĳ��key��IO��Ϻ������������̵߳Ĵ���
	 * ������Reactor���̴߳���ڶ���IO key��ʱ���ֻ���֮ǰ���Ǹ�key�Ķ�IO�¼�������֮ǰͬһ��key�Ĵ���δ��ɣ�
	 * ���ȴ�֮ǰ�Ĵ�����ɵĻ����ͻ���ֶ���߳�ͬʱ�����޸�Handler�������ݵ���������³���
	 * ��������Ȱ����ݶ�ȫ������buffer�оͿ��Թ���ˣ���
	 * 
	 * <p>�˴���synchronizedͬ����Ϊ�˷�ֹstate״̬�Լ���дbuffer�ڶ��̷߳����г��ֶ������ݣ�
	 * Debug���Ե�ʱ��ͬʱ����һ��SelectionKey��2���̣߳�
	 * <br>1��Reactor���߳�
	 * <br>2����������Ϻ���̴߳���Ļ����̳߳�����ִ��processAndHandOff���߳�
	 * <br>
	 * ���ܵ�һʹ��volatile����ԭ�ӱ�����ԭ������Ϊ�÷���Ϊ���ϲ�����check and act��
	 */
	private synchronized void readAndProcess(){
		doRead();
		doProcess();
	}
	
	private void doRead(){
		int readSize;
		try {
			while((readSize = clientChannel.read(readBuf)) > 0){
				readData.append(new String(Arrays.copyOfRange(readBuf.array(), 0, readSize)));
				readBuf.clear();
			}
			if(readSize == -1){
				disconnect();
				return;
			}
		} catch (IOException e) {
			e.printStackTrace();
			disconnect();
		}
		
		log("readed from client:"+readData+", "+readData.length());
	}
	
	private void doProcess(){
		if(readIsComplete()){
			state = State.PROCESSING;
			processAndInterestWrite();
		}
	}
	
	/**
	 * ������̿����ǱȽϺ�ʱ�ģ����Կɿ��ǽ��佻���̳߳ش���������Ϻ��ע�����Ȥ��write�¼�<p>
	 * Ȼ���������ڽ����̳߳ش������Կ�������ص�IO�Ķ��̴߳����״̬���⣬�����һ����ȫ������buffer��������ͬ��״̬��������
	 */
	private void processAndInterestWrite(){
		Processor processor = new Processor();
		if(useThreadPool){
			execute(processor);
		}else{
			processor.run();
		}
	}
	
	private final class Processor implements Runnable{
		@Override 
		public void run() { 
			processAndHandOff(); 
		}
	}
	
	private synchronized void processAndHandOff(){
		if(process()){
			interestOps(State.WRITING);
		}
	}
	
	//TODO �޸�Ϊ����output������output���������ʱ��ͷ���write��������ÿ�ζ�ʹ��wrap��newһ���µ�
	public boolean process(){
		log("process readData="+readData.toString());
		if(isQuit()){
			disconnect();
			return false;
		}
		
		writeBuf = ByteBuffer.wrap(readData.toString().getBytes());
		readData.delete(0, readData.length());
		return true;
	}
	
	private void write(){
		try {
			do{
				clientChannel.write(writeBuf);
			}while(!writeIsComplete());
		} catch (IOException e) {
			e.printStackTrace();
			disconnect();
		}
		
		String writeData = new String(Arrays.copyOf(writeBuf.array(), writeBuf.array().length));
		log("writed to client:"+writeData+", "+writeData.length());
		interestOps(State.READING);
	}
	
	/**
	 * �¼����¼��������İ�
	 * <ul>
	 * <li>����AWT�е�addActionListener��Ӽ�����/�۲���</li>
	 * </ul>
	 * ����Ҫ����key�ĸ�����key.attach������Ϊkeyһֱ��ʹ�õ��ǵ�ǰthisʵ����
	 * ��Reactor dispatch��ʱ������ǽ��ܣ�accept���ø�������Acceptorʵ����
	 * ������ǰ󶨵���key��ͬһ��Handlerʵ��
	 */
	private void interestOps(State state){
		this.state = state;
		key.interestOps(state.opBit);
	}
	
	public boolean isQuit(){
		return false;
	}
	
	private void disconnect(){
		try {
			clientChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		log("\nclient Address=��"+clientAddress(clientChannel)+"�� had already closed!!! ");
	}
	
	private static SocketAddress clientAddress(SocketChannel clientChannel){
		return clientChannel.socket().getRemoteSocketAddress();
	}
	
	public abstract int byteBufferSize();

	public abstract boolean readIsComplete();

	public abstract boolean writeIsComplete();

}
