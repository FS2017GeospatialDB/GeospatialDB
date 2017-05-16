package edu.mines.csci370;

import org.apache.thrift.server.TThreadPoolServer;
//import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServlet;

//import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
//import org.apache.thrift.transport.TSocket;

import org.apache.thrift.protocol.TJSONProtocol;

import edu.mines.csci370.api.GeolocationService;

public class Server {

	public static GeoHandler handler;
	public static GeolocationService.Processor processor;

	public static void main(String[] args) {
		Database.initialize();
		
		try {
			handler = new GeoHandler();
			processor = new GeolocationService.Processor(handler);

			Runnable server = new Runnable() {
				@Override public void run() {
				  server(processor);
				}
			};

			new Thread(server).start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void server(GeolocationService.Processor processor) {
		try {
			/*TServerSocket serverTransport = new TServerSocket(8000);
			TServer server = new TThreadPoolServer(
					new TThreadPoolServer.Args(serverTransport).processor(processor));

			System.out.println("Starting server on port 8000 ...");
			server.serve();*/

      TServlet servlet = new TServlet(processor, new TJSONProtocol.Factory());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
		
