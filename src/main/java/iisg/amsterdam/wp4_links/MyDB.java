package iisg.amsterdam.wp4_links;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import iisg.amsterdam.wp4_links.utilities.LoggingUtilities;

public class MyDB {

	public String directory;
	public RocksDB myDB;

	public static final Logger lg = LogManager.getLogger(MyDB.class);
	LoggingUtilities LOG = new LoggingUtilities(lg);


	public MyDB(String directory) {
		this.directory = directory;
	}


	public void openMyDB(Boolean deletePrevious) throws RocksDBException {
		// a static method that loads the RocksDB C++ library.
		RocksDB.loadLibrary();
		if(deletePrevious == true) {
			deleteMyDB();
		}
		try {
			FileUtils.forceMkdir(new File(directory));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try (@SuppressWarnings("resource")
		final Options options = new Options().setCreateIfMissing(true)) {
			// a factory method that returns a RocksDB instance	
			myDB = RocksDB.open(options, directory);
		} 		
	}


	public void deleteMyDB()
	{
		try {
			FileUtils.deleteDirectory(new File(directory));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public byte[] serialize(Object obj)  {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os;
		try {
			os = new ObjectOutputStream(out);
			os.writeObject(obj);
			return out.toByteArray();
		} catch (IOException e) {
			LOG.logError("serialize", "Error while serialising object");
			e.printStackTrace();
		}	
		return null;
	}

	public Object deserialize(byte[] data)  {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is;
		try {
			is = new ObjectInputStream(in);
			try {
				return is.readObject();
			} catch (ClassNotFoundException e) {
				LOG.logError("deserialize", "Error while reading input stream object");
				e.printStackTrace();
			}
		} catch (IOException e) {
			LOG.logError("deserialize", "Error while creating new input stream object");
			e.printStackTrace();
		}
		return null;
	}


	public void addSingleValueToDB(String key, String value)  {
		byte[] ser_key = serialize(key);
		try {
			final byte[] result = myDB.get(ser_key);
			if (result != null) {  // if key does not exist in db.
				LOG.logWarn("addSingleValueToDB", "Key: " + key + " already exists in DB (but it will be added anyway)");
			} 
			byte[] ser_value = serialize(value);
			myDB.put(ser_key, ser_value);
		} catch (RocksDBException e) {
			LOG.logError("addSingleValueToDB", "Error adding key: " + key + " with value: " + value + " to DB" );
			LOG.logError("addSingleValueToDB", e.getLocalizedMessage());
		}
	}
	
	public void addSingleValueToDB(String key, String value, Boolean forceAdd)  {
		try {
			byte[] ser_key = serialize(key);
			byte[] ser_value = serialize(value);
			myDB.put(ser_key, ser_value);
		} catch (RocksDBException e) {
			LOG.logError("addSingleValueToDB", "Error adding key: " + key + " with value: " + value + " to DB" );
			LOG.logError("addSingleValueToDB", e.getLocalizedMessage());
		}
	}
	
	public void removeEntryFromDB(String key)  {
		byte[] ser_key = serialize(key);
		try {
			myDB.delete(ser_key);
		} catch (RocksDBException e) {
			LOG.logError("removeEntryFromDB", "Error removing key: " + key + " from DB" );
			LOG.logError("removeEntryFromDB", e.getLocalizedMessage());
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public void addListValueToDB(String key, String value) {
		byte[] ser_key = serialize(key);
		ArrayList<String> myList;
		try {
			final byte[] result = myDB.get(ser_key);
			if (result == null) {  // if key does not exist in db.
				myList = new ArrayList<String>();
			} else {
				myList = (ArrayList<String>) deserialize(result);
			}
			myList.add(value);
			byte[] ser_value = serialize(myList);
			myDB.put(ser_key, ser_value);
		} catch (RocksDBException e) {
			LOG.logError("addListValueToDB", "Error adding key: " + key + " with value: " + value + " to DB" );
			LOG.logError("addListValueToDB", e.getLocalizedMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public void addSetValueToDB(String key, String value) {
		byte[] ser_key = serialize(key);
		HashSet<String> myList;
		try {
			final byte[] result = myDB.get(ser_key);
			if (result == null) {  // if key does not exist in db.
				myList = new HashSet<String>();
			} else {
				myList = (HashSet<String>) deserialize(result);
			}
			myList.add(value);
			byte[] ser_value = serialize(myList);
			myDB.put(ser_key, ser_value);
		} catch (RocksDBException e) {
			LOG.logError("addSetValueToDB", "Error adding key: " + key + " with value: " + value + " to DB" );
			LOG.logError("addSetValueToDB", e.getLocalizedMessage());
		}
	}
	
	public void addSetValueToDB(String key, HashSet<String> setValue) {
		byte[] ser_key = serialize(key);
		try {
			byte[] ser_value = serialize(setValue);
			myDB.put(ser_key, ser_value);
		} catch (RocksDBException e) {
			LOG.logError("addSetValueToDB", "Error adding key: " + key + " with set value: " + setValue + " to DB" );
			LOG.logError("addSetValueToDB", e.getLocalizedMessage());
		}
	}

	
	public String getSingleValueFromDB(String key)  {
		byte[] ser_key = serialize(key);
		try {
			final byte[] result = myDB.get(ser_key);
			if (result != null) {  // result == null if key does not exist in db.
				String myValue =  (String) deserialize(result);
				return myValue;
			}
		} catch (RocksDBException e) {
			LOG.logError("getSingleValueFromDB", "Error getting value of key: " + key + " from DB" );
			LOG.logError("getSingleValueFromDB", e.getLocalizedMessage());
		}
		return null;
	}


	@SuppressWarnings("unchecked")
	public ArrayList<String> getListFromDB(String key)  {
		byte[] ser_key = serialize(key);
		try {
			final byte[] result = myDB.get(ser_key);
			if (result != null) {  // result == null if key does not exist in db.
				ArrayList<String> myList = (ArrayList<String>) deserialize(result);
				return myList;
			}
		} catch (RocksDBException e) {
			LOG.logError("getListFromDB", "Error getting value of key: " + key + " from DB" );
			LOG.logError("getListFromDB", e.getLocalizedMessage());
		}
		return null;
	}
	
	
	@SuppressWarnings("unchecked")
	public HashSet<String> getSetFromDB(String key)  {
		byte[] ser_key = serialize(key);
		try {
			final byte[] result = myDB.get(ser_key);
			if (result != null) {  // result == null if key does not exist in db.
				HashSet<String> myList = (HashSet<String>) deserialize(result);
				return myList;
			}
		} catch (RocksDBException e) {
			LOG.logError("getSetFromDB", "Error getting value of key: " + key + " from DB" );
			LOG.logError("getSetFromDB", e.getLocalizedMessage());
		}
		return null;
	}
	
	
	

	

}