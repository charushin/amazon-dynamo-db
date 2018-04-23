package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	HashMap<String,Node> nodeHashMap = new HashMap<String, Node>();
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	static final int SERVER_PORT = 10000;
	public Uri mUri;
	DBHelper dbHelper;
	SQLiteDatabase sqlDB;
	String [] colNames={"key","value"};
	Node myNode=null;
	List<String> syncList= Collections.synchronizedList(new LinkedList<String>());
	//List<String> syncList = CopyOnWriteArrayList<String>();
	boolean receivedAll = false;
	boolean insert1= false, insert2= false;
	List<String> syncList2 = Collections.synchronizedList(new LinkedList<String>());

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		sqlDB=dbHelper.getWritableDatabase();
		int deletedRows=0;
		//check if only one avd - then all delete will be from here
		 if(selection.equals("@")){
			//If @, delete all from the current AVD
			deletedRows = sqlDB.delete(DBHelper.TABLE_NAME, null, null);
		}else if(selection.equals("*")){
			//If *, delete all from all AVD. - Delete all and forward request by appending port
			Log.d(TAG,"DELETE: DELETE ALL");
			deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection});
			Message deleteForward = new Message(myNode.getPort(), selection + ":" + myNode.getPort(), null, myNode.getPort(), "DELETE",null);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteForward.toString(), myNode.getSucc_port1());

		}else if(selection.contains("*")){
			//forwarded request
			Log.d(TAG,"DELETE: DELETE FORWARD");
			String key = selection.split(":")[0];
			String[] selectArgs = {key};
			String originPort = selection.split(":")[1];

			if(originPort.equals(myNode.getPort())){
				//if origin==myPort, all avds ave deleted. Ring complete
				Log.d(TAG,"DELETE: DELETE ALL COMPLETE");
			}
			else{
				//delete all and forward
				deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
				Message deleteForward = new Message(myNode.getPort(), selection, null, myNode.getPort(), "DELETE",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteForward.toString(), myNode.getSucc_port1());
			}
		}else if (!selection.contains(":") && !selection.contains("-")) {
			//delete a single key
			Log.d(TAG, "DELETE: DELETE SELECTION IS: " + selection);
			String port = checkWhereKeyBelongs(selection);
			if(myNode.getPort().equals(port)){
				//delete from yourself and send to replicas to delete
				deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
				Message deleteForward = new Message(myNode.getPort(), selection+":"+myNode.getPort()+"-1", null, myNode.getPort(), "DELETE",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteForward.toString(), myNode.getSucc_port1());

			}
			else{
				//send to the node to which key belongs
				Message deleteForward = new Message(myNode.getPort(), selection, null, myNode.getPort(), "DELETE",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteForward.toString(), port);

			}

		}
		else {
			//forwarded request for key
			//check if belongs to you - if so, delete key else forward the request to the successor
			Log.d(TAG, "DELETE: DELETE SELECTION IS: " + selection);
			String key = selection.split(":")[0];
			String[] selectArgs = {key};
			String replicaNum = selection.split(":")[1].split("-")[1];
			if(replicaNum.equals("1")){
				deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
				Message deleteForward = new Message(myNode.getPort(), selection.split("-")[0]+"-2", null, myNode.getPort(), "DELETE",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteForward.toString(), myNode.getSucc_port1());

			}
			else{
				deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
			}


		}
		return deletedRows;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		sqlDB=dbHelper.getWritableDatabase();
		//int originPort = Integer.parseInt(msgValues[0])/2;
		Log.v("insert", values.toString());
		long id = 0;

		String port = checkWhereKeyBelongs(String.valueOf(values.get("key")));
		Log.d(TAG,"INSERT: PORT TO INSERT IS: "+port);

		//check if it belongs in your space
		String keyVal = String.valueOf(values.get("key"));
		if(!keyVal.contains("-")) {
			if (myNode.getPort().equals(port)) {
				//YES
				Log.d(TAG, "INSERT: KEY BELONGS IN MY SPACE");
				//insert in mine and replicate in other AVDs
				insertAndReplicate(values);
			}
			else{
				//send the reques to the actual port for insert
				Message insertMessage = new Message(myNode.getPort(),values.get("key").toString(),values.get("value").toString(),myNode.getPort(),"INSERT",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insertMessage.toString(),port);
			}
		}
		else{
			Log.d(TAG,"INSERT: INSERT TO BE REPLICATED");
			ContentValues mContentValues = new ContentValues();
			mContentValues.put("key", values.get("key").toString().split(":")[0]);
			mContentValues.put("value", values.get("value").toString());

				id = insert(mContentValues);

			Log.d(TAG,"INSERT: IN REPLICATE DONE");
			Log.d(TAG, "SENDING RESPONSE PACK TO THE ORIGIN PORT");

			String []val = values.get("key").toString().split(":")[1].split("-");
			Log.d(TAG,"INSERT VALUES FOR SPLIT: "+val[0]+" and "+val[1]);

			String responseNum = val[1];
			Log.d(TAG,"INSERT: REPLICATE RESPONSE_NUM: "+responseNum);
			if(responseNum.equals("1")){
				Message insertResponse = new Message(val[0],null,null,myNode.getPort(),"INSERT_RESPONSE",responseNum);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insertResponse.toString(),val[0]);
				Log.d(TAG,"INSERT: REPLICATE 2");
				Message messageReplicate2=new Message(myNode.getPort(),mContentValues.get("key")+":"+val[0]+"-2".toString(),values.get("value").toString(),myNode.getPort(),"INSERT",null);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,messageReplicate2.toString(),myNode.getSucc_port1());
			}
			else{
				Message insertResponse = new Message(val[0],null,null,myNode.getPort(),"INSERT_RESPONSE",responseNum);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,insertResponse.toString(),val[0]);

			}

		}

		Uri newUri= ContentUris.withAppendedId(uri,id);
		return newUri;
	}

	public long insertAndReplicate(ContentValues values){
		long id = 0;

		id = insert(values);
		Log.d(TAG,"INSERT: INSERT DONE. NOW REPLICATING");
		//send request to replica1 and replica2 to insert in their content provider
		Log.d(TAG,"INSERT: REPLICATE 1");
		Message messageReplicate1=new Message(myNode.getPort(),values.get("key")+":"+myNode.getPort()+"-1".toString(),values.get("value").toString(),myNode.getPort(),"INSERT",null);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,messageReplicate1.toString(),myNode.getSucc_port1());



		return id;

	}
	public long insert(ContentValues values){
		long id = 0;
		try {
			id = sqlDB.insertWithOnConflict(DBHelper.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return id;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		//initializing all nodes
		initializeNodes();
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
		dbHelper=new DBHelper(getContext());

		// TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		//String myPortID=myPort;
		Log.d(TAG,"OnCreate: My Port ID: "+myPort);
		myNode=nodeHashMap.get(myPort);
		Log.d(TAG,"OnCreate: My Node: "+myNode);

		//starting server task
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.d(TAG, " in server local socket address is " + serverSocket.getLocalSocketAddress());
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		/*Log.d(TAG,"OnCreate: Initializing Database");
		sqlDB=dbHelper.getWritableDatabase();
		Log.d(TAG,"OnCreate: Initialized Database");*/

		return true;
	}

	private void initializeNodes() {
		try {
			nodeHashMap.put("11108",new Node("5554","11108",genHash("5554"),"11112","11116","11120",true));
			nodeHashMap.put("11112",new Node("5556","11112",genHash("5556"),"11124","11108","11116",true));
			nodeHashMap.put("11116",new Node("5558","11116",genHash("5558"),"11108","11120","11124",true));
			nodeHashMap.put("11120",new Node("5560","11120",genHash("5560"),"11116","11124","11112",true));
			nodeHashMap.put("11124",new Node("5562","11124",genHash("5562"),"11120","11112","11108",true));

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.v("query", selection);
		MatrixCursor matrixCursor=new MatrixCursor(colNames);
		sqlDB=dbHelper.getWritableDatabase();

		SQLiteQueryBuilder sqLiteQueryBuilder=new SQLiteQueryBuilder();
		sqLiteQueryBuilder.setTables(DBHelper.TABLE_NAME);
		String [] mSelectionArgs={selection};
		Cursor cursor = null;

		if(selection.equals("@")){
			cursor = sqLiteQueryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
			//Log.d(TAG, "QUERY: @ CURSOR IS: " + cursor.toString());
			return cursor;
		}
		else if(selection.equals("*")){
			Log.d(TAG, "QUERY: QUERY ALL: I AM THE ORIGIN");
			synchronized (syncList) {
				//query all from current AVD and forward query to successor
				cursor = sqLiteQueryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
				if (cursor != null) {
					while (cursor.moveToNext()) {
						Object[] columnValues = new Object[2];
						columnValues[0] = cursor.getString(cursor.getColumnIndex("key"));
						columnValues[1] = cursor.getString(cursor.getColumnIndex("value"));
						matrixCursor.addRow(columnValues);
					}
				}
				Message queryForward = new Message(myNode.getPort(),"*:"+myNode.getPort(),null,myNode.getPort(),"QUERY",null);
				//forward query all to successor
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port1());
				while (syncList.isEmpty() || !receivedAll) {
					try {
						syncList.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			//After receiving all the responses, add in the matrixCursor, clear the list and return
			for (String s : syncList) {
				Object[] objectValues = new Object[2];
				if(!s.equals(null) && s.contains("-")) {
					objectValues[0] = s.split("-")[0]; //key
					objectValues[1] = s.split("-")[1]; //value
					matrixCursor.addRow(objectValues);
				}
				else{
					Log.d(TAG,"RECEIVED NULL");
				}
			}
			syncList.clear();
			return matrixCursor;
		} else if (selection.contains("*")) {
			if (selection.split(":")[1].equals(myNode.getPort())) {
				//ring complete
				Log.d(TAG, "QUERY: QUERY ALL: RING COMPLETE");
				synchronized (syncList){
					receivedAll = true;
					syncList.notify();
				}
			} else {
				//request forwarded from some node
				Log.d(TAG, "QUERY: RECEIVED QUERY ALL");
				cursor = sqLiteQueryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
				String response = "";
				if (cursor != null) {
					while (cursor.moveToNext()) {
						response = response+cursor.getString(cursor.getColumnIndex("key")) + "-" + cursor.getString(cursor.getColumnIndex("value")) + ",";
					}
				}
				else{
					response="NO DATA";
				}
				String originPort = selection.split(":")[1];
				Message queryResponse = new Message(originPort, selection, response, myNode.getPort(), "QUERY_RESPONSE",null);
				//send to origin the cursor
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), originPort);
				Message queryForward = new Message(originPort,selection,null,myNode.getPort(),"QUERY",null);
				//send to successor the request
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port1());
			}
		}

		else if (!selection.contains(":") && !selection.contains("-")) {
			//has a key
			Log.d(TAG, "QUERY: QUERY SELECTION IS: " + selection);
			String port = checkWhereKeyBelongs(selection);
			Log.d(TAG,"QUERY: PORT TO QUERY IS: "+port);
			//check if the key belongs in my space- if so query the database and return the cursor

			if(myNode.getPort().equals(port)){
				//key belongs in my partition
				//send the request to the tail of the chain
				synchronized (syncList) {
					Log.d(TAG,"QUERY: KEY BELONGS TO ME");
					Log.d(TAG,"SENDING TO TAIL");
					Message queryToTail = new Message(myNode.getPort(), selection + ":" + myNode.getPort() + "-2", null, myNode.getPort(), "QUERY", null);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryToTail.toString(), myNode.getSucc_port2());
					while (syncList.isEmpty()){
						try {
							syncList.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				for (String s : syncList) {
					Log.d(TAG, "LIST ITEM: " + s);
					Object[] objectValues = new Object[2];
					if(s.contains("-")) {
						objectValues[0] = s.split("-")[0]; //key
						objectValues[1] = s.split("-")[1]; //value
						matrixCursor.addRow(objectValues);
					}
				}
				syncList.clear();
				return matrixCursor;
			}
			else{
				//key does not belong to me - send it to the actual node
				synchronized (syncList) {
					Message queryForward = new Message(myNode.getPort(), selection + ":" + myNode.getPort(), null, myNode.getPort(), "QUERY", null);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), port);
					while (syncList.isEmpty()) {
						try {
							syncList.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

				}

			for (String s : syncList) {
				Log.d(TAG, "LIST ITEM: " + s);
				Object[] objectValues = new Object[2];
				if(s.contains("-")) {
					objectValues[0] = s.split("-")[0]; //key
					objectValues[1] = s.split("-")[1]; //value
					matrixCursor.addRow(objectValues);
				}
			}
			syncList.clear();
			return matrixCursor;
			}
		}
		else if(selection.contains("-")) {
			//forwarded request from the request co-ordinator
			//Send the response to the origin port
			Log.d(TAG, "QUERY: QUERY SELECTION IS: " + selection);
			String key = selection.split(":")[0];
			String[] selectArgs = {key};
			cursor = sqLiteQueryBuilder.query(sqlDB, projection, "key = ?", selectArgs, null, null, sortOrder);
			String response = null;
			if (cursor != null && cursor.moveToFirst()) {
				response = cursor.getString(cursor.getColumnIndex("key")) + "-" + cursor.getString(cursor.getColumnIndex("value"));
			}
			String originPort = selection.split(":")[1].split("-")[0];
			Message queryResponse = new Message(originPort, selection, response, myNode.getPort(), "QUERY_RESPONSE",null);
			//send to origin the cursor
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), originPort);

		}
		else if(selection.contains(":")) {
			String originPort = selection.split(":")[1];
			//send this request to the tail of the chain
			Message queryToTail = new Message(originPort,selection+"-2",null,myNode.getPort(),"QUERY",null);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryToTail.toString(), myNode.getSucc_port2());
		}

		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	//from PA2A
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public boolean checkIfKeyBelongsToMe(String port,String originHash){
		//boolean result=false;
		//port 11108
		Node node = nodeHashMap.get(port);
		int predPort = Integer.parseInt(node.getPred_port())/2;
		int succPort = Integer.parseInt(node.getSucc_port1())/2;
		String predHash=null,succHash=null;
		try {
			predHash=genHash(String.valueOf(predPort));
			succHash=genHash(String.valueOf(succPort));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		 if(predHash.compareTo(originHash)<=0 && originHash.compareTo(node.getHash())<0){
			//my key space
			Log.d(TAG,"CHECK: MY KEY SPACE");
			return true;
		}else if(predHash.compareTo(node.getHash())>0 && originHash.compareTo(predHash)>0 ){
			//largest key
			Log.d(TAG,"CHECK: LARGEST KEY");
			return true;
		} else if(originHash.compareTo(predHash)<0 && originHash.compareTo(node.getHash())<0 && predHash.compareTo(node.getHash())>0){
			//smallest key
			Log.d(TAG,"CHECK: SMALLEST KEY");
			return true;
		}
		return false;
	}

	public String checkWhereKeyBelongs(String key){
		try {
			String keyHash=genHash(key);
			for (HashMap.Entry<String,Node> entry : nodeHashMap.entrySet()) {
				String port = entry.getKey();
				Node n=entry.getValue();
				if(checkIfKeyBelongsToMe(port,keyHash)){
					Log.d(TAG,"FOUND PORT");
					return port;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}



	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while (true) {
				Log.d(TAG,"Waiting for incoming connections");
				try {
					String msgReceived;
					//Accepting client connection using accept() method
					Socket client = serverSocket.accept();
					//System.out.println(client.isConnected()+" client connected and "+client.getRemoteSocketAddress());
					Log.d(TAG, "SERVER TASK: client connected" + client.getRemoteSocketAddress());
					//Reading the message received by reading InputStream using BufferedReader
					BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));

					msgReceived = br.readLine();
					//Log.d(TAG, "Server Side: Message Received is  " + msgReceived);
					if(msgReceived!=null){
						Log.d(TAG,"SERVER: MESSAGE RECEIVED NOT NULL");
						String[] msgValues=msgReceived.split("#");
						//String origin, String key, String value, String sender, String type, String response
						Message msg = new Message(msgValues[0],msgValues[1],msgValues[2],msgValues[3],msgValues[4], msgValues[5]);
						String msgType = msgValues[4];
						Log.d(TAG,"ServerTask: Message Type is: "+msgType);

						if(msgType.equals("QUERY")){
							Log.d(TAG,"ServerTask:QUERY");
							Cursor cursor=query( mUri, null, msgValues[1], null, null);
							if(cursor!=null)
								while(cursor.moveToNext()){
									Log.d(TAG, "QUERY: CURSOR: "+cursor.toString());
								}
						}
						else if(msgType.equals("QUERY_RESPONSE")){
							String msgResponse=msgValues[2];
							Log.d(TAG,"QUERYY_RESPONSE: Key-Value Pairs received");
							String key=msgValues[1];
							String [] pairs=msgResponse.split(",");
							Log.d(TAG,"QUERY_RESPONSE: Response is: "+pairs[0]);
							synchronized (syncList) {
								for (String s : pairs) {
									syncList.add(s);

								}
								Log.d(TAG, "QUERY_RESPONSE: SYNCHRONIZED LIST");
								//Log.d(TAG,syncList.get(0));
								syncList.notify();
							}
						}
						else if(msgType.equals("INSERT")){
							Log.d(TAG,"ServerTask:INSERT");
							ContentValues mContentValues = new ContentValues();
							mContentValues.put("key", msg.getKey());
							mContentValues.put("value", msg.getValue());
							insert(mUri,mContentValues);
							Log.d(TAG,"ServerTask:INSERT    inserted");
						}
						else if (msgType.equals("INSERT_RESPONSE")){
							Log.d(TAG,"ServerTask: INSERT_RESPONSE");
							if(msg.getResponse().equals("1")){
								insert1 =true;
							}
							if(msg.getResponse().equals("2")){
								insert2 = true;
							}
							/*synchronized (syncList2){
								syncList2.add(msg.getSender());
								Log.d(TAG,"INSERT_RESPOMSE: REPLICATION ACK RECEIVED FROM: "+msg.getSender());
								syncList2.notify();
							}*/


						}
						else if(msgType.equals("DELETE")){
							Log.d(TAG,"ServerTask:DELETE");
							int deletedRows=delete( mUri, msgValues[1],null);
						}
						else{
							Log.d(TAG,"ServerTask:else");
						}

					}
					//client.close();
				} catch (Exception e){
					e.printStackTrace();
				}
				// return null;
			} // end of while loop
		}  //end-doInBackground


	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String msgToSend=msgs[0];
			String portToSend=msgs[1];

			Socket socket = new Socket();
			try {
				socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(portToSend)));
				Log.d(TAG, "Client Side: "+socket.isConnected() + " and " + socket.getRemoteSocketAddress() + " and " + socket.getLocalSocketAddress());
				//Fetching the output stream of the socket.
				OutputStream os = socket.getOutputStream();
				PrintWriter pw = new PrintWriter(os, true);
				//Writing the message to be send to the other device on the socket's output stream.
				Log.d(TAG,"CLIENT TASK: Msg to Send: "+msgToSend);
				Log.d(TAG,"CLIENT TASK: Sending message to "+portToSend);
				pw.println(msgToSend);
				pw.flush();

			} catch (IOException e) {
				e.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}
}
