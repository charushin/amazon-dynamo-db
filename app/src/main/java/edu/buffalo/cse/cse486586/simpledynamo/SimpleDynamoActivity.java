package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.TAG;

public class SimpleDynamoActivity extends Activity {
	private ContentResolver mContentResolver;
	private Uri mUri;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());



		mContentResolver = getContentResolver();
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

		//The code below for server task and Telephony Manager from PA1
		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.d(TAG, myPort);


		/*findViewById(R.id.button3).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));*/
		findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				Cursor cursor=mContentResolver.query(mUri,null,"@",null,null);
				Log.d(TAG,"QUERY LOCAL FROM UI");
				if(cursor!=null){
					while (cursor.moveToNext()){
						String key=cursor.getString(cursor.getColumnIndex("key"));
						String value=cursor.getString(cursor.getColumnIndex("value"));
						Log.d("LOCAL",key+"-"+value);
						tv.append(key+"\t"+value+"\n");
					}
				}
			}
		});

		findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				tv.setText("");
				Cursor cursor=mContentResolver.query(mUri,null,"*",null,null);
				Log.d(TAG,"QUERY ALL FROM UI");
				if(cursor!=null){
					while (cursor.moveToNext()){
						String key=cursor.getString(cursor.getColumnIndex("key"));
						String value=cursor.getString(cursor.getColumnIndex("value"));
						Log.d("ALL",key+"-"+value);
						tv.append(key+"\t"+value+"\n");
					}
				}
			}
		});

		/*findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				tv.setText("");
				//Uri uri=mContentResolver.insert()
			}
		});*/
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
