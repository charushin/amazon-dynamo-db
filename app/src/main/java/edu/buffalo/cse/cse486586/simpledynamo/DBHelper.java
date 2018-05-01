package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by charushi on 4/4/18.
 */

/**
     * Referrence: https://developer.android.com/guide/topics/providers/content-provider-creating.html
     https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
     https://developer.android.com/training/data-storage/sqlite.html
     *
 * */
public class DBHelper extends SQLiteOpenHelper {
    public static final String DB_NAME="Simple_dynamo";
    public static final String TABLE_NAME="message_tbl";



        //public static String GM_CREATE_TABLE =
        //      "CREATE TABLE "+TABLE_NAME+" (key TEXT  PRIMARY KEY,value TEXT )";
    public static String GM_CREATE_TABLE = "CREATE TABLE message_tbl (" +
                "key           TEXT  PRIMARY KEY, \n" +
                "value      TEXT \n" +
                ")";

    public DBHelper(Context context) {
            super(context, DB_NAME, null, 1);
        }

    @Override
    public void onCreate(SQLiteDatabase db) {

            db.execSQL(GM_CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }

}
