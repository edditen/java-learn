package com.tenchael.xmlrpcdemo;

import de.timroes.axmlrpc.XMLRPCClient;
import de.timroes.axmlrpc.XMLRPCException;
import de.timroes.axmlrpc.XMLRPCServerException;

import java.net.URL;

/**
 * Created by tengzhizhang on 2019/1/16.
 */
public class App {

	private static Object execute(String serviceName, Object... params) {
		try {
			XMLRPCClient client = new XMLRPCClient(new URL("http://localhost:19001/RPC2"));
			client.setLoginData("someuser", "somepass");
			return client.call(serviceName, params);
		} catch (XMLRPCServerException ex) {
			// The server throw an error.
			ex.printStackTrace();
		} catch (XMLRPCException ex) {
			// An error occured in the client.
			ex.printStackTrace();
		} catch (Exception ex) {
			// Any other exception
			ex.printStackTrace();
		}
		throw new RuntimeException("No result");
	}


	public static void main(String[] args) throws Exception {
		String helpInf = (String) execute("system.methodHelp", "system.listMethods");
		System.out.println(helpInf);
	}

}
