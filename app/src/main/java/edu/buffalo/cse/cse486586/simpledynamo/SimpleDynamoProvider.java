    package edu.buffalo.cse.cse486586.simpledynamo;

    import java.io.BufferedReader;
    import java.io.File;
    import java.io.IOException;
    import java.io.InputStreamReader;
    import java.io.OutputStream;
    import java.io.PrintWriter;
    import java.net.InetAddress;
    import java.net.InetSocketAddress;
    import java.net.ServerSocket;
    import java.net.Socket;
    import java.net.SocketException;
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
    import android.content.ContentResolver;
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

    /* REFERENCES
        *https://developer.android.com/reference/android/database/MatrixCursor.html
        * https://docs.oracle.com/javase/tutorial/essential/concurrency/locksync.html
        * https://docs.oracle.com/javase/tutorial/essential/concurrency/guardmeth.html
        * From PA2B
        * https://docs.oracle.com/javase/7/docs/api/java/net/SocketTimeoutException.html
        * https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
        * https://developer.android.com/guide/topics/providers/content-provider-basics.html
        * https://developer.android.com/guide/topics/providers/content-provider-creating.html
        * https://developer.android.com/reference/android/os/AsyncTask.html
        * Used code from my PA2B and PA3
         */

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
        boolean dbExists;
        List<Integer> readWriteLock = Collections.synchronizedList(new LinkedList<Integer>());
        List<String> syncList2 = Collections.synchronizedList(new LinkedList<String>());

        @Override
        public int delete(Uri uri, String selection, String[] selectionArgs) {
            //sqlDB=dbHelper.getWritableDatabase();
            // TODO Auto-generated method stub
            //sqlDB=dbHelper.getWritableDatabase();
            Log.v("delete",selection);
            int deletedRows=0;
            //check if only one avd - then all delete will be from here
             if(selection.equals("@")){
                //If @, delete all from the current AVD
                deletedRows = sqlDB.delete(DBHelper.TABLE_NAME, null, null);
            }
            else{
                 //deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
                 String port = checkWhereKeyBelongs(selection);
                 Message deleteMessage = new Message(myNode.getPort(), selection,null, myNode.getPort(), "DELETE",null);
                 String response="",response1="",response2="";
                 try {
                     response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMessage.toString(), port).get();

                     if(response.contains("FAIL")){
                        Log.d(TAG,"Delete in port "+port+" failed");
                     }
                     deleteMessage.setType("DELETE_REPLICATE");
                     response1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMessage.toString(), nodeHashMap.get(port).getSucc_port1()).get();

                     if(response1.contains("FAIL")){
                         Log.d(TAG,"Delete in port "+port+" failed");
                     }

                     response2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMessage.toString(), nodeHashMap.get(port).getSucc_port2()).get();

                     if(response2.contains("FAIL")){
                         Log.d(TAG,"Delete in port "+port+" failed");
                     }
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 } catch (ExecutionException e) {
                     e.printStackTrace();
                 }
             }
            return deletedRows;
        }
        public int delete(String selection){
            //sqlDB=dbHelper.getWritableDatabase();
            int deletedRows = 0;
            deletedRows = sqlDB.delete(DBHelper.TABLE_NAME,"key=?",new String[]{selection.split(":")[0]});
            return deletedRows;
        }

        @Override
        public String getType(Uri uri) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Uri insert(Uri uri, ContentValues values) {
            Log.v("insert",values.get("key")+" and "+values.get("value"));

            String port = checkWhereKeyBelongs(values.get("key").toString());
            Log.e("insert","key: "+values.get("key")+" belongs to port: "+port);
            Message queryResponse = new Message(myNode.getPort(), values.get("key").toString(), values.get("value").toString(), myNode.getPort(), "INSERT",null);
            //send to origin the cursor
            String response = "",response1 = "",response2="";
            try {

                if(myNode.getPort().equals(port)){
                    //insert and then send to replicas
                    response=insertAndReplicate(queryResponse);
                }
                else {

                    //if(nodeHashMap.get(port).isStatus()) {
                    response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), port).get();
                    Log.d(TAG, "INSERT: Response from node is: " + response);
                }
                /*}
                else{
                    Log.d(TAG,"INSERT: Insertnot done on co-ordiantor. Status is failed");
                }*/
                //Adding failure handling part
                //if Co-ordinator fails, send NODE_FAIL message to everyone
                if(response.contains("FAIL") && !response.contains("-")) {
                    Message nodeFail = new Message(myNode.getPort(), null, null, myNode.getPort(), "NODE_FAIL", port);
                    sendNodeFailToEveryone(nodeFail);
                }

                    queryResponse.setType("REPLICA1");
                    Node succ1 = nodeHashMap.get(nodeHashMap.get(port).getSucc_port1());
                    //if (succ1.isStatus()) {
                        response1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), nodeHashMap.get(port).getSucc_port1()).get();
                        Log.d(TAG, "INSERT: Response from 1st replica is: " + response1);
                    /*} else {
                        Log.d(TAG, "INSERT: Insertnot done on REPLICA1. Status is failed");
                    }*/
                    if (response1.contains("FAIL") && !response1.contains("-")) {
                        Message nodeFail1 = new Message(myNode.getPort(), null, null, myNode.getPort(), "NODE_FAIL", nodeHashMap.get(port).getSucc_port1());
                        sendNodeFailToEveryone(nodeFail1);
                    }

                        queryResponse.setType("REPLICA2");
                        Node succ2 = nodeHashMap.get(nodeHashMap.get(port).getSucc_port2());
                        //if (succ2.isStatus()) {
                            response2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), nodeHashMap.get(port).getSucc_port2()).get();
                            Log.d(TAG, "INSERT: Response from 2nd replica is: " + response2);
                        /*} else {
                            Log.d(TAG, "INSERT: Insertnot done on REPLICA2. Status is failed");
                        }*/
                        if (response2.contains("FAIL")) {
                            Message nodeFail2 = new Message(myNode.getPort(), null, null, myNode.getPort(), "NODE_FAIL", nodeHashMap.get(port).getSucc_port2());
                            sendNodeFailToEveryone(nodeFail2);
                        }


                //new ClientTask()., executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryResponse.toString(), port);
            }  catch(Exception e){
                e.printStackTrace();
            }
            Log.d(TAG,"INSERT**:  Response is : "+response);
            return uri;
        }

        public void sendNodeFailToEveryone(Message message){
            for (HashMap.Entry<String, Node> entry : nodeHashMap.entrySet()) {
                String key = entry.getKey();
                Node value = entry.getValue();
                //send the node fail message on socket to everyone
                Socket socket = new Socket();
                try {
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(key)));
                    Log.d(TAG, "Client Side: "+socket.isConnected() + " and " + socket.getRemoteSocketAddress() + " and " + socket.getLocalSocketAddress());
                    //Fetching the output stream of the socket.
                    OutputStream os = socket.getOutputStream();
                    PrintWriter pw = new PrintWriter(os, true);
                    //Writing the message to be send to the other device on the socket's output stream.
                    Log.d(TAG,"Sending message to ServerSocket MSG: "+message);
                    Log.d(TAG,"Sending message to ServerSocket PORT: "+key);
                    pw.println(message.toString());
                    pw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
        }

        public String insertAndReplicate(Message message){
            String response = "SUCCESS";
            long id = 0;
            ContentValues contentValues = new ContentValues();
            contentValues.put("key",message.getKey());
            contentValues.put("value",message.getValue());
            //sqlDB=dbHelper.getWritableDatabase();
            synchronized (readWriteLock){
                while(!lockAvailable){
                    try {
                        readWriteLock.wait();
                        Log.d(TAG,"INSERT AND REPLICATE: Waiting for lock to be available");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                lockAvailable = false;
                id = insert(contentValues);
                lockAvailable = true;
                readWriteLock.notify();
            }

            return response;
        }
        public long insert(ContentValues contentValues){
            //sqlDB=dbHelper.getWritableDatabase();
            Log.d(TAG,"INSERT: Inserting in self");
            Log.d(TAG,"Inserting key: "+contentValues.get("key")+" and value: "+contentValues.get("value"));
            long id = 0;
            try {
                id = sqlDB.insertWithOnConflict(DBHelper.TABLE_NAME, null, contentValues, SQLiteDatabase.CONFLICT_REPLACE);
            }
            catch(Exception e){
                e.printStackTrace();
            }
            return id;
        }

        public String sendMessageToServerSocket(Message message, String portToSend){
            String fail = "FAIL";
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portToSend)));
                Log.d(TAG, "Client Side: " + socket.isConnected() + " and " + socket.getRemoteSocketAddress() + " and " + socket.getLocalSocketAddress());
                //Fetching the output stream of the socket.
                OutputStream os = socket.getOutputStream();
                PrintWriter pw = new PrintWriter(os, true);
                //Writing the message to be send to the other device on the socket's output stream.
                Log.d(TAG, "Sending message to ServerSocket MSG: " + message);
                Log.d(TAG, "Sending message to ServerSocket PORT: " + portToSend);
                pw.println(message.toString());
                pw.flush();
                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg = "";
                String msgReceived;
                while ((msgReceived = br.readLine()) != null) {
                    msg = msg + msgReceived;
                }
                return msg;
            } catch(SocketException e){
                return fail;
            } catch (IOException e) {
                e.printStackTrace();
            } catch(Exception e){
                e.printStackTrace();
                return fail;
            }
            return fail;
        }


        @Override
        public boolean onCreate() {
            // TODO Auto-generated method stub
            //initializing all nodes
            initializeNodes();
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            File dynamoDB = this.getContext().getDatabasePath(DBHelper.DB_NAME);

            dbExists = dynamoDB.exists();
            Log.d( TAG,"DB status: "+dynamoDB.exists());
            dbHelper=new DBHelper(getContext());


            // TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
            TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            //String myPortID=myPort;
            Log.d(TAG,"OnCreate: My Port ID: "+myPort);
            myNode=nodeHashMap.get(myPort);
            Log.d(TAG,"OnCreate: My Node: "+myNode);
            File dbFile2 = this.getContext().getDatabasePath(DBHelper.DB_NAME);
            Log.d( TAG,"DB status: "+dbFile2.exists());

            Log.d(TAG,"OnCreate: Initializing Database");
            sqlDB=dbHelper.getWritableDatabase();
            Log.d(TAG,"OnCreate: Initialized Database");

            //starting server task
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                Log.d(TAG, " in server local socket address is " + serverSocket.getLocalSocketAddress());
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
            }
            return true;
        }

        public void sendRecoveryMessageToEveryone(Message message){
            synchronized (readWriteLock) {
                while (!lockAvailable) {
                    try {
                        readWriteLock.wait();
                        Log.d(TAG, "INSERT AND REPLICATE: Waiting for lock to be available");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                lockAvailable = false;
                for (HashMap.Entry<String, Node> entry : nodeHashMap.entrySet()) {

                String key = entry.getKey();
                Node value = entry.getValue();
                if (!myNode.getPort().equals(key)) {
                        try {
                            //String response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, message.toString(), key).get();
                            Socket socket = new Socket();

                                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(key)));
                                Log.d(TAG, "Client Side: "+socket.isConnected() + " and " + socket.getRemoteSocketAddress() + " and " + socket.getLocalSocketAddress());
                                //Fetching the output stream of the socket.
                                OutputStream os = socket.getOutputStream();
                                PrintWriter pw = new PrintWriter(os, true);
                                //Writing the message to be send to the other device on the socket's output stream.
                                Log.d(TAG,"Sending message to ServerSocket MSG: "+message);
                                Log.d(TAG,"Sending message to ServerSocket PORT: "+key);
                                pw.println(message.toString());
                                pw.flush();
                                BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                String response="";
                                String msgReceived;
                                while((msgReceived=br.readLine())!=null){
                                    response =response+msgReceived;
                                }

                            //receiving key value pairs and inserting in my content provider
                            String[] keyVal = response.split(",");
                            for (String s : keyVal) {
                                if (s.contains("-")) {
                                    String k = s.split("-")[0];
                                    String v = s.split("-")[1];
                                    ContentValues cv = new ContentValues();
                                    cv.put("key", k);
                                    cv.put("value", v);
                                    //insert(cv);
                                    insert(cv);


                                }
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }

                }
                lockAvailable = true;
                readWriteLock.notify();

            }

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


            String qport = checkWhereKeyBelongs(selection);
            Log.e("query","key: "+selection+" belongs to port: "+qport);
            MatrixCursor matrixCursor=new MatrixCursor(colNames);
            //sqlDB=dbHelper.getWritableDatabase();

            SQLiteQueryBuilder sqLiteQueryBuilder=new SQLiteQueryBuilder();
            sqLiteQueryBuilder.setTables(DBHelper.TABLE_NAME);
            String [] mSelectionArgs={selection};
            Cursor cursor = null;

            if(selection.equals("@")){
                synchronized (readWriteLock){
                    while (!lockAvailable){
                        try {
                            readWriteLock.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    lockAvailable = false;
                    cursor = sqLiteQueryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
                    //Log.d(TAG, "QUERY: @ CURSOR IS: " + cursor.toString());
                    if (cursor != null) {
                        while (cursor.moveToNext()) {
                            Object[] columnValues = new Object[2];
                            columnValues[0] = cursor.getString(cursor.getColumnIndex("key"));
                            columnValues[1] = cursor.getString(cursor.getColumnIndex("value"));
                            matrixCursor.addRow(columnValues);
                        }
                    }

                    lockAvailable=true;
                    readWriteLock.notify();
                    return matrixCursor;

                }

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
                    //if my successor is available, send it, else send to my 2nd successor
                    if((nodeHashMap.get(myNode.getSucc_port1()).isStatus())) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port1());
                    }
                    else{
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port2());
                    }

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
                    if((nodeHashMap.get(myNode.getSucc_port1()).isStatus())) {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port1());
                    }
                    else{
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), myNode.getSucc_port2());
                    }
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
                    String succ2=nodeToSend.getSucc_port2();
                    String succ1=nodeToSend.getSucc_port1();
                    String node=nodeToSend.getPort();
                    if(succ2.equals(myNode.getPort())){
                        response=queryOnMyContentProvider(queryForward);
                    }
                    else {
                        //if(nodeHashMap.get(succ2).isStatus()) {
                        response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), nodeToSend.getSucc_port2()).get();
                        Log.d(TAG, "Query response from tail is : " + response);
                    }
                    //}
                    //if tail, fails send the tail failed message to everyone
                    if(response.contains("FAIL") || response.equals("") || (response.equals("null") || response.equals(null)) ) {
                        Message nodeFail = new Message(myNode.getPort(), null, null, myNode.getPort(), "NODE_FAIL", nodeHashMap.get(port).getSucc_port2());
                        sendNodeFailToEveryone(nodeFail);

                        //and get response from 1st successor
                        //if(nodeHashMap.get(succ1).isStatus()) {
                            response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), nodeToSend.getSucc_port1()).get();
                            Log.d(TAG, "Query response from 1st replica is : " + response);
                        //}
                        if (response.contains("FAIL") || response.equals("") ||(response.equals("null"))) {
                            Message nodeFail2 = new Message(myNode.getPort(), null, null, myNode.getPort(), "NODE_FAIL", nodeHashMap.get(port).getSucc_port1());
                            sendNodeFailToEveryone(nodeFail2);

                            //if 1st successor fails, get it from co-ordinator
                            //if (nodeHashMap.get(node).isStatus())
                                response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryForward.toString(), nodeToSend.getPort()).get();
                            Log.d(TAG, "Query response from head is : " + response);

                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                Log.v("query","Response is : "+response);
                String [] pair=response.split("-");
                matrixCursor.addRow(new String[]{pair[0],pair[1]});

                return matrixCursor;
            }

            return cursor;
        }

        public String queryRequest(Message message){
            Log.d(TAG,"QUERY: Query selection is : "+message.getKey());
            Log.d(TAG,"QUERY: Sending Query Request to the tail of the chain");


            //Message queryToTail = new Message(myNode.getPort(),selection,null,myNode.getPort(),"QUERY_KEY",null);
            //forward query all to successor
            String nodeToForward;
            String response = "";
            try {

            if(message.getType().equals("QUERY_KEY")) {

                message.setType("QUERY_TAIL");
                //response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message.toString(), myNode.getSucc_port2()).get();
                response = sendMessageToServerSocket(message, myNode.getSucc_port2());
                if (response.contains("FAIL") || response.contains("null") || response.contains("")) {
                    //tail is failed
                    Log.d(TAG, "Query from tail failed. Tail is failed or recovering");
                    //send to 1st replica
                    response = sendMessageToServerSocket(message, myNode.getSucc_port1());
                    if (response.contains("FAIL") || response.contains("null") || response.contains("")) {
                        //1st replica failed. query myself
                        Log.d(TAG, "Query from tail failed. 1st replica is failed or recovering");
                        response = queryOnMyContentProvider(message);
                    }
                }
            }
            else if(message.getType().equals("QUERY_KEY1")){
                message.setType("QUERY_TAIL");
                response = sendMessageToServerSocket(message, myNode.getSucc_port1());
                if (response.contains("FAIL") || response.contains("null") || response.contains("")) {
                    //1st replica failed. query myself
                    Log.d(TAG, "Query from tail failed. 1st replica is failed or recovering");
                    response = queryOnMyContentProvider(message);
                }
            }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Log.d(TAG, "QUERY :Response is: " + response);
            return response;
        }

        public String queryOnMyContentProvider(Message message){
        Log.d(TAG, "QUERY: QUERY SELECTION IS: " + message.getKey());
                Log.d(TAG,"QUERY: Lock is: "+lockAvailable);
            SQLiteQueryBuilder sqLiteQueryBuilder=new SQLiteQueryBuilder();
            sqLiteQueryBuilder.setTables(DBHelper.TABLE_NAME);
            String [] mSelectionArgs={message.getKey()};
            Cursor cursor = null;
            //sqlDB=dbHelper.getReadableDatabase();

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
                if(dbExists){
                    //send recovery message to everyone and receive response from everyone
                    Message recoveryMessage = new Message(myNode.getPort(),null,null,myNode.getPort(),"NODE_RECOVER",myNode.getPort());
                    int deletedRows = sqlDB.delete(DBHelper.TABLE_NAME, null, null);
                    /*try {
                        String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,recoveryMessage.toString(),myPort).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }*/
                    sendRecoveryMessageToEveryone(recoveryMessage);
                }

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
                            else if(msgType.equals("QUERY_KEY") || msgType.equals("QUERY_KEY1")){
                                Log.d(TAG,"ServerTask: QUERY_KEY");
                                //String response = queryRequest(msg);
                                String response = queryOnMyContentProvider(msg);
                                Log.d(TAG,"ServerTask:QUERY_KEY    response: "+response);
                                pw.println(response);
                                pw.flush();
                                Log.d(TAG,"QUERY_KEY: Written response on the socket"+msg.getOrigin());
                            }
                            else if(msgType.equals("QUERY_TAIL")){
                                Log.d(TAG,"ServerTask: QUERY_KEY");
                                String response = queryOnMyContentProvider(msg);
                                Log.d(TAG,"ServerTask:QUERY_KEY    response: "+response);
                                pw.println(response);
                                pw.flush();
                                Log.d(TAG,"QUERY_KEY: Written response on the socket"+msg.getOrigin());
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
                                Log.d(TAG,"QUERY_KEY: Written response on the socket"+msg.getOrigin());
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
                                Log.d(TAG,"INSERT: Written response on the socket"+msg.getOrigin());
                            }

                            else if(msgType.equals("DELETE") || msgType.equals("DELETE_REPLICATE")){
                                Log.d(TAG,"ServerTask:DELETE");
                                //int deletedRows=delete( mUri, msgValues[1],null);
                                int deletedRows = delete(msg.getKey());
                                pw.println(deletedRows);
                                pw.flush();
                                Log.d(TAG,"DELETE: Written response on the socket"+msg.getOrigin());

                            } else if(msgType.equals("NODE_FAIL")){
                                Log.d(TAG,"ServerTask: NODE_FAIL "+msg.getResponse());
                                String response=msg.getResponse();
                                Node failedNode=nodeHashMap.get(response);
                                failedNode.status = false;
                                nodeHashMap.put(response,failedNode);
                            } else if(msgType.equals("NODE_RECOVER")){
                                Log.d(TAG,"ServerTask: NODE_RECOVER");
                                String response=msg.getResponse();
                                Node recoveredNode=nodeHashMap.get(response);
                                recoveredNode.status = true;
                                nodeHashMap.put(response,recoveredNode);


                                //sending data to the recoveredNode
                                String responseVal = sendDataToRecoveredNode(recoveredNode);
                                Log.d(TAG,"ServerTask:RECOVER    response: "+responseVal);
                                pw.println(responseVal);
                                pw.flush();
                                Log.d(TAG,"RECOVER: Written response on the socket"+msg.getOrigin());

                            }
                            else{
                                Log.d(TAG,"ServerTask:else");
                            }

                        }
                        client.close();
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                    // return null;
                } // end of while loop
            }  //end-doInBackground

        }
        public String sendDataToRecoveredNode(Node node){
            String response ="";
            //checking what data to send

            if(node.getPort().equals(myNode.getSucc_port1()) || node.getPort().equals(myNode.getSucc_port2())){
                //if node is my successor, send my data to the node
                Log.d(TAG,"Sending my data to the recovered node");
                Cursor qCursor = query(mUri, null, "@",null,null);
                if(qCursor!=null){
                    while(qCursor.moveToNext()){
                        String key=qCursor.getString(qCursor.getColumnIndex("key"));
                        String value=qCursor.getString(qCursor.getColumnIndex("value"));
                        try {
                            /*if((genHash(key).compareTo(nodeHashMap.get(myNode.getPred_port()).getHash())>=0) && (genHash(key).compareTo(myNode.getHash())<0)){
                                response=response+key+"-"+value+",";
                            }*/
                            String port = checkWhereKeyBelongs(key);
                            if(port.equals(myNode.getPort())){
                                response=response+key+"-"+value+",";
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            else if(node.getPort().equals(myNode.getPred_port())){
                //if the node is my predecessor, send its own data to the node
                Log.d(TAG,"Sending its own data to the recovered node");
                Cursor qCursor = query(mUri, null, "@",null,null);
                if(qCursor!=null){
                    while(qCursor.moveToNext()){
                        String key=qCursor.getString(qCursor.getColumnIndex("key"));
                        String value=qCursor.getString(qCursor.getColumnIndex("value"));
                        try {
                            /*if((genHash(key).compareTo(node.getHash())<=0) && (genHash(key).compareTo(nodeHashMap.get(node.getPred_port()).getHash())>0)){
                                //sending data whose key hash is less than pred and greater than 2nd pred
                                response=response+key+"-"+value+",";
                            }*/
                            String port = checkWhereKeyBelongs(key);
                            if(port.equals(node.getPort())){
                                response=response+key+"-"+value+",";
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
            else{
                Log.d(TAG,"Node is not my predecesoor or 2 successor. Won't send anything");
                response = "NO DATA";
            }


            return response;
        }



        private class ClientTask extends AsyncTask<String, Void, String> {

            @Override
            protected String doInBackground(String... msgs) {
                String msgToSend=msgs[0];

                String [] messages= msgToSend.split("#");
                if(messages[4].equals("NODE_RECOVER")){
                    Message m = new Message(messages[0],messages[1],messages[2],messages[3],messages[4],messages[5]);
                    sendRecoveryMessageToEveryone(m);
                    return "NODE RECOVERED";
                }
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
                        String msg="";
                        String msgReceived=br.readLine();
                        /*while((msgReceived=br.readLine())!=null){
                            Log.d(TAG,"Client Task, Message received from server is: "+msgReceived);
                            msg = msg+msgReceived;
                        }
                        if(!msg.equals("")){
                            return msg;
                        }
                        else{
                            return fail;
                        }*/
                        if(msgReceived!=null){
                            return msgReceived;
                        }
                        return fail;
                    }
                } catch(SocketException e){
                    Log.d(TAG,"Client Task: Socket Exception Received. When connecting to port: " +portToSend);
                    return fail;
                } catch (IOException e) {
                    e.printStackTrace();
                    return fail;
                } catch(Exception e){
                    e.printStackTrace();
                    return fail;
                }
                return fail;
            }
        }
    }
