import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;

public class Table implements Serializable {
	private String name;
	private int pageCount;
	public int currentPage;
	private String key;
	private String csvPath = "./src/data/metaData.csv";

	public Table(String name, String key) {
		this.name = name;
		this.pageCount = 0;
		this.currentPage = -1;
		this.key = key;
	}

	public String generatePath(int pageNumber) {
		return "./src/pages/" + this.name + pageNumber + ".class";
	}

	private String generateIndexPath(String column) {
		return "./src/indexFiles/" + name + column + "INDEX.class";
	}

	public Page getPageFromFile(String path) throws ClassNotFoundException, IOException, DBAppException {
		if (pageCount == -1 || currentPage == -1) {
			throw new DBAppException("Table is empty");
		}
		FileInputStream file = new FileInputStream(path);
		ObjectInputStream input = new ObjectInputStream(file);
		Page page = (Page) input.readObject();
		input.close();
		file.close();
		return page;
	}

	public void writePageToFile(String path, Page page) throws IOException {
		FileOutputStream file = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(page);
		out.close();
		file.close();
	}

	public void writeBitMapToFile(String column, BitMapIndex bitmap) throws IOException {
		FileOutputStream file = new FileOutputStream(generateIndexPath(column));
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(bitmap);
		out.close();
		file.close();
	}

	public BitMapIndex getBitMap(String column) throws IOException, ClassNotFoundException {
		FileInputStream file = new FileInputStream(generateIndexPath(column));
		ObjectInputStream input = new ObjectInputStream(file);
		BitMapIndex bitmap = (BitMapIndex) input.readObject();
		input.close();
		file.close();
		return bitmap;
	}

	public ArrayList<String> getIndexedKeys() throws IOException {
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

	public ArrayList<String> getIndexedKeysType() throws IOException {
		ArrayList<String> result = new ArrayList<String>();
		BufferedReader br = new BufferedReader(new FileReader(csvPath));
		String line;
		while ((line = br.readLine()) != null) {
			String[] lineArray = line.split(",");
			if (lineArray[0].equals(name) && lineArray[4].equals("TRUE")) {
				result.add(lineArray[1]);
				result.add(lineArray[2]);
			}
		}
		br.close();
		return result;
	}

	public void handleBitMapInsert(Tuple tuple, int pageNumber, int pageIndex, int capacity)
			throws IOException, ClassNotFoundException, DBAppException {
		ArrayList<String> indexedColumns = getIndexedKeys();
		for (int i = 0; i < indexedColumns.size(); i++) {
			String column = indexedColumns.get(i);
			Object key = tuple.getKey(column);
			BitMapIndex bitmap = getBitMap(column);
			bitmap.insertToBitMap(key, pageNumber, pageIndex, capacity);
			writeBitMapToFile(column, bitmap);
		}
	}

	public void insertSorted(Tuple tuple) throws IOException, ClassNotFoundException, DBAppException {
		// No tuples yet in table
		if (currentPage == -1 || pageCount == -1) {
			currentPage++;
			if (pageCount == -1) {
				pageCount++;
			}
			Page newPage = new Page(pageCount, key);
			newPage.insertTupleToPage(tuple);
			handleBitMapInsert(tuple, pageCount, 0, newPage.getCapacity());
			String path = generatePath(pageCount);
			writePageToFile(path, newPage);
			return;
		}
		ArrayList<String> indexedKeys = getIndexedKeysType();
		int pageIndex = -1;
		if (indexedKeys.contains(key)) {
			Object tupleKey = tuple.getKey(key);
			BitMapIndex bitmap = getBitMap(key);
			int starting = 0;
			int ending = bitmap.getPageCount();
			pageIndex = this.binaryCompareSearch(tupleKey, key, bitmap, starting, ending, -5);

		}
		Boolean found = false;
		// determining to which page this tuple belongs;
		for (int i = pageIndex < 0 ? 0 : pageIndex; i <= pageCount; i++) {
			Page page = getPageFromFile(generatePath(i));
			Boolean belongs = page.doesTupleBelong(tuple);
			Boolean waitNext = false;
			if (belongs) {
				if (page.isFull()) {
					Tuple removedTuple = page.pop();
					insertNearest(removedTuple, i + 1);
				} else {
					if (i + 1 <= pageCount) {
						Page newPage = getPageFromFile(generatePath(i + 1));
						if (newPage.checkFirst(tuple))
							waitNext = true;

					}
				}
				if (!waitNext) {
					page.insertTupleToPage(tuple);
					handleBitMapInsert(tuple, i, page.getIndex(tuple), page.getCapacity());
					writePageToFile(generatePath(i), page);
					found = true;
					return;
				}
			}
		}
		// incase this is the biggest element
		if (!found) {
			pageCount++;
			Page newPage = new Page(pageCount, key);
			newPage.insertTupleToPage(tuple);
			handleBitMapInsert(tuple, pageCount, newPage.getIndex(tuple), newPage.getCapacity());
			writePageToFile(generatePath(pageCount), newPage);
		}
	}

	public void insertNearest(Tuple tuple, int index) throws ClassNotFoundException, IOException, DBAppException {
		if (index > pageCount) {
			pageCount++;
			Page newPage = new Page(index, key);
			newPage.insertTupleToPage(tuple);
			writePageToFile(generatePath(index), newPage);
			return;
		}
		Page page = getPageFromFile(generatePath(index));
		if (page.isFull()) {
			Tuple removedTuple = page.pop();
			insertNearest(removedTuple, index + 1);
		}
		page.insertTupleToPage(tuple);
		writePageToFile(generatePath(index), page);

	}

	public void updateTuple(Tuple tuple, String strKey) throws ClassNotFoundException, IOException, DBAppException {
		Boolean isKeyUpdated = tuple.tupleData.keySet().contains(this.key) ? true : false;
		ArrayList<String> indexedKeys = getIndexedKeysType();
		int pageIndex = -1;
		if (indexedKeys.contains(key)) {
			final String string = "java.lang.String";
			final String integer = "java.lang.Integer";
			final String doubled = "java.lang.Double";
			final String Boolean = "java.lang.Boolean";
			final String date = "java.util.Date";
			int typeIndex = indexedKeys.indexOf(key) + 1;
			String keyType = indexedKeys.get(typeIndex);
			ArrayList<ArrayList<String>> positions = null;
			switch (keyType) {
			case integer:
				int search = Integer.parseInt(strKey);
				positions = this.bitMapSeach(key, search, "=");
				break;
			case doubled:
				Double searchD = Double.parseDouble(strKey);
				positions = this.bitMapSeach(key, searchD, "=");
				break;
			}
			if (positions == null) {
				System.out.println("key not found");
				return;
			}
			for (int j = 0; j < positions.size(); j++) {
				if (positions.get(j).contains("1")) {
					pageIndex = j;
					break;
				}
			}
		}

		if (isKeyUpdated) {
			for (int i = pageIndex < 0 ? 0 : pageIndex; i <= pageCount; i++) {
				String path = generatePath(i);
				Page page = getPageFromFile(path);
				Tuple updated = page.getTupleAndRemove(tuple, strKey);
				if (updated != null) {
					writePageToFile(path, page);
					deleteTuple(updated);
					insertSorted(updated);
					return;

				}

			}
			return;
		}

		for (int i = pageIndex < 0 ? 0 : pageIndex; i <= pageCount; i++) {
			String path = generatePath(i);
			Page page = getPageFromFile(path);
			page.updateTuple(tuple, strKey, name);
			writePageToFile(path, page);
			if(pageIndex>=0) {
				break;
			}
		}
	}

	public void deleteTuple(Tuple tuple) throws ClassNotFoundException, IOException, DBAppException {
		ArrayList<Integer> pages = new ArrayList<Integer>();
		for (int i = 0; i <= pageCount; i++) {
			String path = generatePath(i);
			Page page = getPageFromFile(path);
			page.deleteTuple(tuple, name);
			if (page.size() != 0)
				writePageToFile(path, page);
			else {
				pages.add(i);
				for (int j = i; j < pageCount; j++) {
					File f1 = new File(generatePath(j + 1));
					FileInputStream file = new FileInputStream(f1);
					ObjectInputStream in = new ObjectInputStream(file);
					Page renamedPage = (Page) in.readObject();
					renamedPage.setID(renamedPage.getID() - 1);
					in.close();
					file.close();
					writePageToFile(generatePath(j + 1), renamedPage);

				}
			}
		}

		for (int i1 = 0; i1 < pages.size(); i1++) {
			File deleted = new File(generatePath(pages.get(i1)));
			deleted.delete();
			for (int j = pages.get(i1); j < pageCount; j++) {
				File f1 = new File(generatePath(j + 1));
				FileInputStream file = new FileInputStream(f1);
				ObjectInputStream in = new ObjectInputStream(file);
				Page renamedPage = (Page) in.readObject();
				writePageToFile(generatePath(j), renamedPage);
				in.close();
				file.close();
				f1.delete();

			}
			for (int k = i1 + 1; k < pages.size(); k++) {
				pages.set(k, pages.get(k) - 1);
			}
			pageCount--;

		}

	}

	public void readTable() throws ClassNotFoundException, IOException, DBAppException {
		if (currentPage == -1) {
			System.out.println("Empty Table");
			return;
		}
		for (int i = 0; i <= pageCount; i++) {
			System.out.println("Page: " + i);
			Page page = getPageFromFile(generatePath(i));
			page.readPage();
		}
	}

	public int getPageCount() {
		return pageCount;
	}

	public ArrayList<Tuple> handleSqlQueries(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws IOException, ClassNotFoundException, DBAppException {
		ArrayList<ArrayList<ArrayList<String>>> query = new ArrayList<ArrayList<ArrayList<String>>>();
		ArrayList<String> keys = getIndexedKeys();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			SQLTerm term = arrSQLTerms[i];
			String columnName = arrSQLTerms[i]._strColumnName;
			ArrayList<ArrayList<String>> page_positions = new ArrayList<ArrayList<String>>();
			if (keys.contains(columnName))
				page_positions = bitMapSeach(columnName, term._objValue, term._strOperator);
			else
				page_positions = linearSeach(columnName, term._objValue, term._strOperator);
			query.add(page_positions);
			System.out.println(page_positions.toString() + "PAGE POSITIONS");

		}
		if (query.size() != strarrOperators.length + 1) {
			System.out.println("Wrong format, operators must be equal to the size of sql terms -1");
			return null;
		}

		ArrayList<String> operators = new ArrayList<String>(Arrays.asList(strarrOperators));
		System.out.println(operators.toString());
		// doing all AND operations first
		int removedCount = 0;
		for (int i = 0; i < operators.size(); i++) {
			if (operators.get(i).equals("AND")) {
				int index = i - removedCount;
				ArrayList<ArrayList<String>> temp = handleAND(query.get(index), query.get(index + 1));
				query.set(index, temp);
				query.remove(index + 1);
				removedCount++;

			}
		}

		// removing and
		for (int r = 0; r < operators.size(); r++) {
			if (operators.get(r).equals("AND"))
				operators.remove(r);
		}
		// XOR OPERATIONS
		removedCount = 0;
		for (int a = 0; a < operators.size(); a++) {
			int index = a - removedCount;
			if (operators.get(a).equals("XOR")) {
				ArrayList<ArrayList<String>> temp = handleXOR(query.get(index), query.get(index + 1));
				query.set(index, temp);
				query.remove(index + 1);
				removedCount++;
			}

		}
		// removing XOR
		for (int r1 = 0; r1 < operators.size(); r1++) {
			if (operators.get(r1).equals("XOR"))
				operators.remove(r1);
		}

		// OR OPERATIONS
		removedCount = 0;
		for (int a2 = 0; a2 < operators.size(); a2++) {
			int index = a2 - removedCount;
			if (operators.get(a2).equals("OR")) {
				ArrayList<ArrayList<String>> temp = handleOR(query.get(index), query.get(index + 1));
				query.set(index, temp);
				query.remove(index + 1);
				removedCount++;
				// operators.remove(a2);
			}
		}
		// getting the results
		System.out.println(query.toString() + "FINAL");
		ArrayList<ArrayList<String>> finalPositions = query.get(0);
		ArrayList<Tuple> queryResults = new ArrayList<Tuple>();
		// getting the tuples from pages
		for (int rc = 0; rc < finalPositions.size(); rc++) {
			ArrayList<String> inside = finalPositions.get(rc);
			if (inside.contains("1")) {
				/*
				 * if(rc>pageCount) break;
				 */
				// only loading a page when it has a row needed
				Page page = getPageFromFile(generatePath(rc));
				for (int ax = 0; ax < inside.size(); ax++) {
					if (inside.get(ax).equals("1"))
						queryResults.add(page.getTuple(ax));
				}

			}
		}

		System.out.println(queryResults.toString());
		return queryResults;
	}

	public ArrayList<ArrayList<String>> handleAND(ArrayList<ArrayList<String>> arr1,
			ArrayList<ArrayList<String>> arr2) {
		int minSize = arr1.size() > arr2.size() ? arr2.size() : arr1.size();
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < minSize; i++) {
			ArrayList<String> arr1Inside = arr1.get(i);
			ArrayList<String> arr2Inside = arr2.get(i);
			int minSizeInside = arr1Inside.size() > arr2Inside.size() ? arr2Inside.size() : arr1Inside.size();
			ArrayList<String> arr3Inside = new ArrayList<String>();
			for (int j = 0; j < minSizeInside; j++) {
				if (arr1Inside.get(j).equals("1") && arr2Inside.get(j).equals("1"))
					arr3Inside.add("1");
				else {
					arr3Inside.add("0");
				}
			}
			result.add(arr3Inside);

		}
		return result;
	}

	public ArrayList<ArrayList<String>> handleXOR(ArrayList<ArrayList<String>> arr1,
			ArrayList<ArrayList<String>> arr2) {
		int maxSize = arr1.size() > arr2.size() ? arr1.size() : arr2.size();
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < maxSize; i++) {
			ArrayList<String> arr1Inside = new ArrayList<String>();
			ArrayList<String> arr2Inside = new ArrayList<String>();
			if (i < arr1.size())
				arr1Inside = arr1.get(i);
			if (i < arr2.size())
				arr2Inside = arr2.get(i);
			int maxSizeInside = arr1Inside.size() > arr2Inside.size() ? arr1Inside.size() : arr2Inside.size();
			ArrayList<String> arr3Inside = new ArrayList<String>();
			for (int j = 0; j < maxSizeInside; j++) {
				int oneCount = 0;
				if (j < arr1Inside.size()) {
					if (arr1Inside.get(j).equals("1"))
						oneCount++;
				}
				if (j < arr2Inside.size()) {
					if (arr2Inside.get(j).equals("1"))
						oneCount++;
				}

				if (oneCount == 1)
					arr3Inside.add("1");
				else
					arr3Inside.add("0");

			}

			result.add(arr3Inside);

		}
		return result;
	}

	public ArrayList<ArrayList<String>> handleOR(ArrayList<ArrayList<String>> arr1, ArrayList<ArrayList<String>> arr2) {
		int maxSize = arr1.size() > arr2.size() ? arr1.size() : arr2.size();
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < maxSize; i++) {
			ArrayList<String> arr1Inside = new ArrayList<String>();
			ArrayList<String> arr2Inside = new ArrayList<String>();
			if (i < arr1.size())
				arr1Inside = arr1.get(i);
			if (i < arr2.size())
				arr2Inside = arr2.get(i);
			int maxSizeInside = arr1Inside.size() > arr2Inside.size() ? arr1Inside.size() : arr2Inside.size();
			ArrayList<String> arr3Inside = new ArrayList<String>();
			for (int j = 0; j < maxSizeInside; j++) {
				boolean oneFound = false;
				if (j < arr1Inside.size()) {
					if (arr1Inside.get(j).equals("1"))
						oneFound = true;
				}
				if (j < arr2Inside.size()) {
					if (arr2Inside.get(j).equals("1"))
						oneFound = true;
				}
				if (oneFound)
					arr3Inside.add("1");
				else
					arr3Inside.add("0");
			}

			result.add(arr3Inside);
		}
		return result;

	}

	public ArrayList<ArrayList<String>> linearSeach(String columnName, Object value, String operator)
			throws ClassNotFoundException, IOException, DBAppException {
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i <= pageCount; i++) {
			Page page = getPageFromFile(generatePath(i));
			ArrayList<String> pageResult = page.getPositionIndex(columnName, value, operator);
			result.add(pageResult);
		}
		return result;
	}

	public ArrayList<ArrayList<String>> bitMapSeach(String columnName, Object value, String operator)
			throws ClassNotFoundException, IOException, DBAppException {
		BitMapIndex bitmap = getBitMap(columnName);
		int starting = 0;
		int ending = bitmap.getPageCount();
		int pageIndex = -1;
		if (operator.equals("=")) {
			pageIndex = binarySearch(value, columnName, bitmap, starting, ending, -5, true);
		} else {
			if (operator.equals("!="))
				pageIndex = binarySearch(value, columnName, bitmap, starting, ending, -5, false);
			else {
				pageIndex = binaryCompareSearch(value, columnName, bitmap, starting, ending, -5);
			}
		}

		ArrayList<ArrayList<String>> result;
		switch (operator) {
		case ">":
			result = bitmap.getAllAfter(pageIndex, value, false);
			break;
		case ">=":
			result = bitmap.getAllAfter(pageIndex, value, true);
			break;
		case "<":
			result = bitmap.getAllBefore(pageIndex, value, false);
			break;
		case "<=":
			result = bitmap.getAllBefore(pageIndex, value, true);
			break;
		case "!=":
			result = bitmap.getExact(pageIndex, value, true);
			break;
		default:
			result = bitmap.getExact(pageIndex, value, false);
			break;
		}
		return result;
	}

	public int binarySearch(Object key, String columnName, BitMapIndex bitmap, int starting, int ending, int midStore,
			boolean isEqual) throws ClassNotFoundException, IOException, DBAppException {
		if (ending >= starting) {
			if (starting == ending)
				return starting;
			int mid = (starting + (ending - 1)) / 2;
			if (mid == midStore) {
				if (isEqual)
					return -1;
				return -2;
			}
			if (bitmap.contains(mid, key))
				return mid;

			if (bitmap.lastIsBigger(mid, key)) {
				return binarySearch(key, columnName, bitmap, starting, mid + 1, mid, isEqual);

			}

			return binarySearch(key, columnName, bitmap, mid + 1, ending, mid, isEqual);

		}

		if (isEqual)
			return -1;
		return -2;
	}

	public int binaryCompareSearch(Object key, String columnName, BitMapIndex bitmap, int starting, int ending,
			int midStore) throws ClassNotFoundException, IOException, DBAppException {
		if (ending >= starting) {
			int mid = (starting + (ending - 1)) / 2;
			if (mid == midStore)
				return mid;
			if (starting == ending)
				return starting;
			if (bitmap.liesWithinPage(mid, key))
				return mid;
			if (bitmap.lastIsBigger(mid, key)) {
				return binaryCompareSearch(key, columnName, bitmap, starting, mid + 1, mid);
			}
			return binaryCompareSearch(key, columnName, bitmap, mid + 1, ending, mid);

		}
		return -1;

	}

}
