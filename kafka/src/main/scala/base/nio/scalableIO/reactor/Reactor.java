package base.nio.scalableIO.reactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;

import static base.nio.scalableIO.Logger.log;

/**
 * Reactor��Ӧ��
 * <ul>
 * <li>��ʱ��Ӧ����ڵĶ�/дIO�¼�</li>
 * <li>���ַ������ʵ�Handler�������Ͻ���ҵ����</li>
 * <li>����AWT�еĵ����¼��ַ��߳�</li>
 * </ul>
 */
public abstract class Reactor extends Thread{

	protected final int port;
	protected final ServerSocketChannel serverChannel;
	protected final boolean isMainReactor;
	protected final boolean useMultipleReactors;
	protected final long timeout;
	protected Selector selector;
	
	public Reactor(int port, ServerSocketChannel serverChannel, boolean isMainReactor, boolean useMultipleReactors, long timeout){
		this.port = port;
		this.serverChannel = serverChannel;
		this.isMainReactor = isMainReactor;
		this.useMultipleReactors = useMultipleReactors;
		this.timeout = timeout;
	}
	
	@Override
	public void run(){
		try {
			init();
			while(!Thread.interrupted()){
				//������ʹ��������select��ʽ������accept��subReactor��selector��register��ʱ���һֱ����
				//�����޸�Ϊ���г�ʱ��select����selectNow��subReactor��selector��register�Ͳ���������
				//����ѡ���˴��г�ʱ��select����Ϊʹ��selectNow������ѭ���ᵼ��CPU쭸��ر��
				//�������ʹ��������select��ʽ������Ҫ֪�����������wakeup�������һֱ������ʹ�÷�������ʽ�Ͳ���Ҫwakeup��
				//selector.select();
				//if(selector.selectNow() > 0){
				if(selector.select(timeout) > 0){
					log(selector+" isMainReactor="+isMainReactor+" select...");
					Iterator<SelectionKey> keyIt = selector.selectedKeys().iterator();
					while(keyIt.hasNext()){
						SelectionKey key = keyIt.next();
						dispatch(key);
						keyIt.remove();
					}
				}
			}
			log(getClass().getSimpleName()+" end on "+port+" ..."+"\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void init() throws IOException{
		selector = Selector.open();
		log(selector+" isMainReactor="+isMainReactor);
		
		if(isMainReactor){
			//serverChannel = ServerSocketChannel.open();
			serverChannel.socket().bind(new InetSocketAddress(port));
			serverChannel.configureBlocking(false);
			SelectionKey key = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
			key.attach(newAcceptor(selector));
			log(getClass().getSimpleName()+" start on "+port+" ..."+"\n");
		}else{
			
		}
		
		//���ʹ��������select��ʽ���ҿ�������Ĵ���Ļ����൱�ڿ����˶��reactor�أ�������mainReactor��subReactor�Ĺ�ϵ��
		//SelectionKey key = serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		//key.attach(newAcceptor(selector, serverChannel));
	}
	
	public abstract Acceptor newAcceptor(Selector selector);
	
	/**
	 * �¼����¼��������İ�
	 * <ul>
	 * <li>����IO��/д�¼����¼���������һһ��Ӧ�İ�</li>
	 * </ul>
	 */
	private void dispatch(SelectionKey key){
		Runnable r = (Runnable)key.attachment();
		if(r != null){
			r.run();
		}
	}
	
}
