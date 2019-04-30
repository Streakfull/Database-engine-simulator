import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;

public class Page implements java.io.Serializable {
	private static final long serialVersionUID = 1L;
	private Vector<Tuple> tuples;
	private String sortingKey;
	private int pageID;
	private int capacity = 2;
	private String csvPath = "./src/data/metaData.csv";

	// defining classes
	private final String string = "class java.lang.String";
	private final String integer = "class java.lang.Integer";
	private final String doubled = "class java.lang.Double";
	private final String Boolean = "class java.lang.Boolean";
	private final String date = "class java.util.Date";

	public Page(int pageID, String sortingKey) {
		this.tuples = new Vector<Tuple>();
		this.sortingKey = sortingKey;
		this.pageID = pageID;
	}

	public void readPage() {
		tuples.forEach(tuple -> {
			tuple.readTuple();
			System.out.println("\n");

		});
	}

	public void insertTupleToPage(Tuple tuple) {
		tuples.add(tuple);
		sortPage();
	}

	public int getID() {
		return this.pageID;
	}

	public Boolean isFull() {
		return tuples.size() == capacity;
	}

	public int size() {
		return tuples.size();
	}

	public boolean contains(String strKey) {
		for (int i = 0; i < tuples.size(); i++) {
			String tupleValue = tuples.get(i).getKey(sortingKey) + "";
			if (tupleValue.equals(strKey))
				return true;
		}
		return false;
	}

	public int getRow(Tuple queryTuple) {
		for (int i = 1; i <= tuples.size(); i++) {
			String tupleValue = tuples.get(i - 1).getKey(sortingKey) + "";
			String queryValue = queryTuple.getKey(sortingKey) + "";
			if (tupleValue.equals(queryValue))
				return i;
		}
		return -1;
	}

	public void handleBitMapUpdate(Tuple tuple, int index, String name) throws IOException, ClassNotFoundException, DBAppException {
		ArrayList<String> keys = getIndexedKeys(name);
		for (int i = 0; i < keys.size(); i++) {
			String path = "./src/indexFiles/" + name + keys.get(i) + "INDEX.class";
			FileInputStream file = new FileInputStream(generateIndexPath(keys.get(i), name));
			ObjectInputStream input = new ObjectInputStream(file);
			BitMapIndex bitmap = (BitMapIndex) input.readObject();
			bitmap.update(pageID, index, tuple.getKey(keys.get(i)), capacity);
			writeBitMapToFile(keys.get(i), bitmap, name);
			input.close();
			file.close();
		}
	}

	public void updateTuple(Tuple tuple, String strKey, String tableName) throws ClassNotFoundException, IOException, DBAppException {
		for (int i = 0; i < tuples.size(); i++) {
			String tupleValue = tuples.get(i).getKey(sortingKey) + "";
			if (tupleValue.equals(strKey)) {
				for (String j : tuples.get(i).tupleData.keySet()) {
					if (tuple.tupleData.containsKey(j))
						tuples.get(i).tupleData.put(j, tuple.tupleData.get(j));
				}
				tuples.get(i).updateTouchDate();
				handleBitMapUpdate(tuples.get(i), i, tableName);
			}
		}
	}

	public void writeBitMapToFile(String column, BitMapIndex bitmap, String name) throws IOException {
		FileOutputStream file = new FileOutputStream(generateIndexPath(column, name));
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(bitmap);
		out.close();
		file.close();
	}

	private String generateIndexPath(String column, String name) {
		return "./src/indexFiles/" + name + column + "INDEX.class";
	}

	public void handleBitMapDelete(String name, int index) throws IOException, ClassNotFoundException {
		ArrayList<String> keys = getIndexedKeys(name);
		for (int i = 0; i < keys.size(); i++) {
			String path = "./src/indexFiles/" + name + keys.get(i) + "INDEX.class";
			FileInputStream file = new FileInputStream(generateIndexPath(keys.get(i), name));
			ObjectInputStream input = new ObjectInputStream(file);
			BitMapIndex bitmap = (BitMapIndex) input.readObject();
			bitmap.delete(pageID, index, false);
			writeBitMapToFile(keys.get(i), bitmap, name);
			input.close();
			file.close();
		}
	}

	public ArrayList<String> getIndexedKeys(String name) throws IOException {
		ArrayList<String> result = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(csvPath));
		String line;
		while ((line = br.readLine()) != null) {
			String[] lineArray = line.split(",");
			if (lineArray[0].equals(name) && lineArray[4].equals("TRUE")) {
				result.add(lineArray[1]);
			}
		}
		br.close();
		return result;
	}

	public void deleteTuple(Tuple tuple, String tableName) throws ClassNotFoundException, IOException {
		ArrayList<Integer> removedIndex = new ArrayList<Integer>();
		ArrayList<Tuple> removedObject = new ArrayList<Tuple>();
		for (int i = 0; i < tuples.size(); i++) {
			boolean misMatch = false;
			for (String j : tuple.tupleData.keySet()) {
				if (!(tuples.get(i).getKey(j) + "").equals(tuple.getKey(j) + "") && !j.equals("TouchDate"))
					misMatch = true;
			}
			if (!misMatch) {
				removedIndex.add(i);
				removedObject.add(tuples.get(i));
			}

		}
		for (int f = 0; f < removedIndex.size(); f++) {

			int ind = (int) removedIndex.get(f);
			Tuple removedTuple = removedObject.get(f);
			tuples.remove(removedTuple);
			handleBitMapDelete(tableName, ind);
		}

	}

	public Tuple getTupleAndRemove(Tuple tuple, String strKey) {
		Tuple result = null;
		for (int i = 0; i < tuples.size(); i++) {
			String tupleValue = tuples.get(i).getKey(sortingKey) + "";
			if (tupleValue.equals(strKey)) {
				for (String j : tuples.get(i).tupleData.keySet()) {
					if (tuple.tupleData.containsKey(j))
						tuples.get(i).tupleData.put(j, tuple.tupleData.get(j));
				}
				tuples.get(i).updateTouchDate();
				result = tuples.get(i);
				break;
			}
		}
		return result;
	}

	public void sortPage() {
		tuples.sort((Tuple a, Tuple b) -> {
			Object dataA = a.getKey(sortingKey);
			Object dataB = b.getKey(sortingKey);
			switch (dataA.getClass().toString()) {
			case string:
				return ((String) dataA).compareTo((String) dataB);
			case integer:
				return ((Integer) dataA).compareTo((Integer) dataB);
			case doubled:
				return ((Double) dataA).compareTo((Double) dataB);
			case Boolean:
				return ((Boolean) dataA).compareTo((Boolean) dataB);
			default:
				return ((Date) dataA).compareTo((Date) dataB);
			}
		});
	}

	public boolean duplicateCheck(Object tupleData) {
		for (int i = 0; i < tuples.size(); i++) {
			if (tuples.get(i).getKey(sortingKey).equals(tupleData))
				return true;
		}
		return false;
	}

	public boolean checkFirst(Tuple tuple) {
		if (tuples.size() == 0)
			return true;
		Object first = tuples.get(0).getKey(sortingKey);
		Object tupleData = tuple.getKey(sortingKey);
		int compareResult = 0;
		switch (tupleData.getClass().toString()) {
		case string:
			compareResult = ((String) tupleData).compareTo((String) first);
			break;
		case integer:
			compareResult = ((Integer) tupleData).compareTo((Integer) first);
			break;
		case doubled:
			compareResult = ((Double) tupleData).compareTo((Double) first);
			break;
		case Boolean:
			compareResult = ((Boolean) tupleData).compareTo((Boolean) first);
			break;
		default:
			compareResult = ((Date) tupleData).compareTo((Date) first);
			break;

		}
		if (compareResult < 0)
			return false;
		return true;
	}

	public ArrayList<String> getPositionIndex(String columnName, Object value, String operator) throws DBAppException {
		if (tuples.size() == 0)
			return null;
		ArrayList<String> result = new ArrayList<String>();
		for (int i = 0; i < tuples.size(); i++) {
			Object tuple = tuples.get(i).getKey(columnName);
			int compareResult = 0;
			try {
			switch (tuple.getClass().toString()) {
			case string:
				compareResult = ((String) tuple).compareTo((String) value);
				break;
			case integer:
				compareResult = ((Integer) tuple).compareTo((Integer) value);
				break;
			case doubled:
				compareResult = ((Double) tuple).compareTo((Double) value);
				break;
			case Boolean:
				compareResult = ((Boolean) tuple).compareTo((Boolean) value);
				break;
			default:
				compareResult = ((Date) tuple).compareTo((Date) value);
				break;
			}
			}
			catch(Exception e) {
				throw new DBAppException("Invalid Types");
			}
			// >, >=, <, <=, != or =
			switch (operator) {
			case ">":
				if (compareResult > 0)
					result.add("1");
				else
					result.add("0");
				break;
			case ">=":
				if (compareResult >= 0)
					result.add("1");
				else
					result.add("0");
				break;
			case "<":
				if (compareResult < 0)
					result.add("1");
				else
					result.add("0");
				break;
			case "<=":
				if (compareResult <= 0)
					result.add("1");
				else
					result.add("0");
				break;
			case "!=":
				if (compareResult != 0)
					result.add("1");
				else
					result.add("0");
				break;
			default:
				if (compareResult == 0)
					result.add("1");
				else
					result.add("0");
				break;

			}

		}
		return result;
	}

	public boolean doesTupleBelong(Tuple tuple) throws DBAppException {
		if (tuples.size() == 0)
			return true;
		int compareResult = 0;
		Object last = tuples.get(tuples.size() - 1).getKey(sortingKey);
		Object tupleData = tuple.getKey(sortingKey);
		if (duplicateCheck(tupleData))
			throw new DBAppException("Duplicate values are not allowed in the clustering column");
		try {
		switch (tupleData.getClass().toString()) {
		case string:
			compareResult = ((String) tupleData).compareTo((String) last);
			break;
		case integer:
			compareResult = ((Integer) tupleData).compareTo((Integer) last);
			break;
		case doubled:
			compareResult = ((Double) tupleData).compareTo((Double) last);
			break;
		case Boolean:
			compareResult = ((Boolean) tupleData).compareTo((Boolean) last);
			break;
		default:
			compareResult = ((Date) tupleData).compareTo((Date) last);
			break;

		}
		if (compareResult <= 0)
			return true;
		if (!this.isFull())
			return true;
		return false;
		}
		catch(Exception e) {
			throw new DBAppException("Invalid types");
		}
	}

	public Tuple pop() {
		Tuple tuple = tuples.get(tuples.size() - 1);
		tuples.remove(tuples.size() - 1);
		return tuple;
	}

	public void setID(int pageID) {
		this.pageID = pageID;
	}

	public int getCapacity() {
		return capacity;
	}

	public int getSize() {
		return tuples.size();
	}

	public Object getKey(String key, int index) {
		return tuples.get(index).getKey(key);
	}

	public int getIndex(Tuple tuple) {
		return tuples.indexOf(tuple);
	}
	public Tuple getTuple(int index) {
		if(index >= tuples.size()) {
			return null;
		}
		return tuples.get(index);
	}
}
