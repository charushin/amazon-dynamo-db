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
import java.util.concurrent.ExecutionException;

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
	boolean replicate1= false, replicate2 = false;
	boolean isInsertDone = false;
	boolean lockAvailable = true;
	List<Integer> readWriteLock = Collections.synchronizedList(new LinkedList<Integer>());
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
		}
		else{
			 deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
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

		String port = checkWhereKeyBelongs(values.get("key").toString());
		Message queryResponse = new Message(myNode.getPort(), values.get("key").toString(), values.get("value").toString(), myNode.getPort(), "INSERT",null);
		//send to origin the cursor
		String response = "";
		try {
			response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, queryResponse.toString(), port).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		Log.d(TAG,"INSERT**:  Response is : "+response);


		return uri;
	}

	public String insertAndReplicate(Message message){
		String response = "SUCCESS";
		long id = 0;
		ContentValues contentValues = new ContentValues();
		contentValues.put("key",message.getKey());
		contentValues.put("value",message.getValue());
		synchronized (readWriteLock){
			while(!lockAvailable){
				try {
					readWriteLock.wait();
					Log.d(TAG,"INSERT AND REPLICATE: Waiting for lock to be available");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			lockAvailable = true;
			Log.d(TAG,"INSERT: Inserting in self");
			try {
				id = sqlDB.insertWithOnConflict(DBHelper.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
			}
			catch(Exception e){
				e.printStackTrace();
			}
			if(message.getType().equals("INSERT")){
				//send to replica1
				message.setType("REPLICA1");
				String replicateResponse = sendMessageToServerSocket(message,myNode.getSucc_port1());
				Log.d(TAG,"INSERT AND REPLICATE: Response is "+replicateResponse);
				lockAvailable = true;
				readWriteLock.notify();
				return replicateResponse;
			}
			else if(message.getType().equals("REPLICA1")){
				//send to replica2
				message.setType("REPLICA2");
				String replicateResponse = sendMessageToServerSocket(message,myNode.getSucc_port1());
				Log.d(TAG,"INSERT AND REPLICATE: Response is "+replicateResponse);
				lockAvailable = true;
				readWriteLock.notify();
				return replicateResponse;
			}


			lockAvailable = true;
			readWriteLock.notify();
		}

		return response;
	}

	public String sendMessageToServerSocket(Message message, String portToSend){
		String fail = "FAIL";
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(portToSend)));
			Log.d(TAG, "Client Side: "+socket.isConnected() + " and " + socket.getRemoteSocketAddress() + " and " + socket.getLocalSocketAddress());
			//Fetching the output stream of the socket.
			OutputStream os = socket.getOutputStream();
			PrintWriter pw = new PrintWriter(os, true);
			//Writing the message to be send to the other device on the socket's output stream.
			Log.d(TAG,"Sending message to ServerSocket MSG: "+message);
			Log.d(TAG,"Sending message to ServerSocket PORT: "+portToSend);
			pw.println(message.toString());
			pw.flush();
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String msgReceived = br.readLine();
			return msgReceived;
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}
		return fail;
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
		Log.d(TAG,"OnCreate: Initializing Database");
		sqlDB=dbHelper.getWritableDatabase();
		Log.d(TAG,"OnCreate: Initialized Database");

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
		//sqlDB=dbHelper.getWritableDatabase();

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
				//syncList.notify();
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

		else if(!selection.contains(":")){
			String port = checkWhereKeyBelongs(selection);
			/*synchronized (syncList){*/
				Log.d(TAG,"QUERY FOR KEY");
				Node nodeToSend = nodeHashMap.get(port);
				Message queryForward = new Message(myNode.getPort(),selection,null,myNode.getPort(),"QUERY_KEY",null);
				//forward query all to successor
				String response = "";
				try {
					response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), nodeToSend.getSucc_port2()).get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
				String [] pair=response.split("-");
				matrixCursor.addRow(new String[]{pair[0],pair[1]});

			return matrixCursor;
			}

		return cursor;
	}

	public String queryOnMyContentProvider(Message message){
	Log.d(TAG, "QUERY: QUERY SELECTION IS: " + message.getKey());
			Log.d(TAG,"QUERY: Lock is: "+lockAvailable);
		SQLiteQueryBuilder sqLiteQueryBuilder=new SQLiteQueryBuilder();
		sqLiteQueryBuilder.setTables(DBHelper.TABLE_NAME);
		String [] mSelectionArgs={message.getKey()};
		Cursor cursor = null;

	synchronized (readWriteLock) {
		while (!lockAvailable) {
			try {
				readWriteLock.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Log.d(TAG, "QUERY: Waiting for query lock");

		}
		lockAvailable = false;

		Log.d(TAG, "QUERY: QUERY MY KEY SPACE");
		cursor = sqLiteQueryBuilder.query(sqlDB, null, "key = ?", mSelectionArgs, null, null, null);
		String response = null;
		if (cursor != null && cursor.moveToFirst()) {
			response = cursor.getString(cursor.getColumnIndex("key")) + "-" + cursor.getString(cursor.getColumnIndex("value"));
		}
		Log.d(TAG,"Query for single key: Response is "+response);


		lockAvailable = true;
		readWriteLock.notify();
		Log.d(TAG, "QUERY :Response is: " + response);
		return response;
	}

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
					OutputStream os = client.getOutputStream();
					PrintWriter pw = new PrintWriter(os, true);

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
						else if(msgType.equals("QUERY_KEY")){
							Log.d(TAG,"ServerTask: QUERY_KEY");
							String response = queryOnMyContentProvider(msg);
							Log.d(TAG,"ServerTask:QUERY_KEY    response: "+response);
							pw.println(response);
							pw.flush();
							Log.d(TAG,"QUERY_KEY: Written response on the socket");
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
							Log.d(TAG,"ServerTask:QUERY_KEY    response: ");
							pw.println("QUERY_RESPONSE ACK");
							pw.flush();
							Log.d(TAG,"QUERY_KEY: Written response on the socket");
						}
						else if(msgType.equals("INSERT") || msgType.equals("REPLICA1") || msgType.equals("REPLICA2")){
							/*Log.d(TAG,"ServerTask:INSERT");
							ContentValues mContentValues = new ContentValues();
							mContentValues.put("key", msg.getKey());
							mContentValues.put("value", msg.getValue());
							insert(mUri,mContentValues);*/
							String response = insertAndReplicate(msg);
							Log.d(TAG,"ServerTask:INSERT    response: "+response);
							pw.println(response);
							pw.flush();
							Log.d(TAG,"INSERT: Written response on the socket");
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



	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			String msgToSend=msgs[0];
			String [] messages= msgToSend.split("#");
			String portToSend=msgs[1];
			String fail = "FAIL";

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
				Log.d(TAG,"Message Type is: "+messages[4]);
				if(messages[4].equals("QUERY") || messages[4].equals("QUERY_RESPONSE")){

				}
				else {
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String msgReceived = br.readLine();
					return msgReceived;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
			return fail;
		}
	}
}
