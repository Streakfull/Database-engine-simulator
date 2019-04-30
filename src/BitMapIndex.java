import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.TreeMap;

public class BitMapIndex implements Serializable {
	private String tableName;
	private int indexPageCount;
	private String columnName;
	private int indexRowCount;
	private boolean found = false;
	private int rowCount = 0;
	private final String string = "class java.lang.String";
	private final String integer = "class java.lang.Integer";
	private final String doubled = "class java.lang.Double";
	private final String Boolean = "class java.lang.Boolean";
	private final String date = "class java.util.Date";

	private String generatePath(int indexNumber) {
		return "./src/indexFiles/" + this.tableName + this.columnName + "INDEX" + indexNumber + ".class";
	}

	public BitMapIndex(String columnName, String tableName) throws IOException {
		this.indexPageCount = -1;
		this.columnName = columnName;
		this.tableName = tableName;
	}

	public void writeBitMapToFile(String path, BitMapPage index) throws IOException {
		FileOutputStream file = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(file);
		out.writeObject(index);
		out.close();
		file.close();
	}

	public BitMapPage getPageFromFile(String path) throws ClassNotFoundException, IOException {
		FileInputStream file = new FileInputStream(path);
		ObjectInputStream input = new ObjectInputStream(file);
		BitMapPage index = (BitMapPage) input.readObject();
		input.close();
		file.close();
		return index;
	}

	public void insertSorted(Object key, ArrayList<ArrayList<String>> bitSequence)
			throws IOException, ClassNotFoundException, DBAppException {
		if (indexPageCount == -1) {
			indexPageCount++;
			BitMapPage newPage = new BitMapPage();
			newPage.insertNewPage(key, bitSequence);
			writeBitMapToFile(generatePath(indexPageCount), newPage);
			return;
		}
		boolean found = false;
		boolean waitNext = false;
		for (int i = 0; i <= indexPageCount; i++) {
			BitMapPage page = getPageFromFile(generatePath(i));
			boolean belongs = page.doesKeyBelong(key);
			if (belongs) {
				if (page.isFull()) {
					Entry<Object, ArrayList<ArrayList<String>>> entry = page.pop();
					insertNearest(entry, i + 1);

				} else {
					if (i + 1 <= indexPageCount) {
						BitMapPage newPage2 = getPageFromFile(generatePath(i + 1));
						if (newPage2.checkFirst(key))
							waitNext = true;
					}
				}
				if (!waitNext) {
					page.insertNewPage(key, bitSequence);
					writeBitMapToFile(generatePath(i), page);
					found = true;
					return;

				}
			}
		}
		if (!found) {
			indexPageCount++;
			BitMapPage page = new BitMapPage();
			page.insertNewPage(key, bitSequence);
			writeBitMapToFile(generatePath(indexPageCount), page);

		}

	}

	public void insertNearest(Entry<Object, ArrayList<ArrayList<String>>> entry, int index)
			throws IOException, ClassNotFoundException {
		if (index > indexPageCount) {
			indexPageCount++;
			BitMapPage newPage = new BitMapPage();
			newPage.insertNewPage(entry.getKey(), entry.getValue());
			writeBitMapToFile(generatePath(indexPageCount), newPage);
			return;
		}
		BitMapPage page = getPageFromFile(generatePath(index));
		if (page.isFull()) {
			Entry<Object, ArrayList<ArrayList<String>>> removedEntry = page.pop();
			insertNearest(removedEntry, index + 1);
		}
		page.insertNewPage(entry.getKey(), entry.getValue());
		writeBitMapToFile(generatePath(index), page);
	}

	public void readBitMap() throws ClassNotFoundException, IOException {
		for (int i = 0; i <= indexPageCount; i++) {
			BitMapPage page = getPageFromFile(generatePath(i));
			if(!page.getBitMap().isEmpty()) {
				System.out.println("BitMap on " +columnName +" Page:" +i );
					page.read();
			}
		}

	}

	public void insertToBitMap(Object key, int pageNumber, int pageIndex, int maxPageCapacity)
			throws ClassNotFoundException, IOException, DBAppException {
		if (indexPageCount < 0) {
			ArrayList<ArrayList<String>> temp = new ArrayList<ArrayList<String>>();
			ArrayList<String> inside = new ArrayList<String>();
			inside.add("1");
			temp.add(inside);
			insertSorted(key, temp);
			return;
		}
		boolean found = false;
		ArrayList<ArrayList<String>> bitsTemp = null;
		for (int i = 0; i <= indexPageCount; i++) {
			BitMapPage page = getPageFromFile(generatePath(i));
			TreeMap<Object, ArrayList<ArrayList<String>>> pageMap = page.getBitMap();
			List<Object> mainList = new ArrayList<Object>();
			Set<Object> keyset = pageMap.keySet();
			mainList.addAll(keyset);
			for (int j = 0; j < mainList.size(); j++) {
				Object uniqueKey = mainList.get(j);
				ArrayList<ArrayList<String>> bits = pageMap.get(uniqueKey);
				bitsTemp = bits;
				int lastIndex = bits.size() - 1;
				if (key.equals(uniqueKey)) {
					found = true;
					if (pageNumber > lastIndex) {
						ArrayList<String> newArrayList = new ArrayList<String>();
						newArrayList.add("1");
						bits.add(newArrayList);
					} else {
						ArrayList<String> temp = bits.get(pageNumber);
						if (temp.size() < maxPageCapacity) {
							if (pageIndex < temp.size())
								temp.add(pageIndex, "1");
							else {
								temp.add("1");
							}
						} else {
							String overFlow = temp.get(temp.size() - 1);
							temp.add(pageIndex, "1");
							temp.remove(temp.size() - 1);
							bits = insertNearest2(bits, pageNumber + 1, overFlow, maxPageCapacity);
						}

					}

				} else {
					if (pageNumber > lastIndex) {
						ArrayList<String> newArrayList = new ArrayList<String>();
						newArrayList.add("0");
						bits.add(newArrayList);
					} else {
						ArrayList<String> temp = bits.get(pageNumber);
						if (temp.size() < maxPageCapacity) {
							if (pageIndex < temp.size())
								temp.add(pageIndex, "0");
							else {
								temp.add("0");
							}
						} else {
							String overFlow = temp.get(temp.size() - 1);
							temp.add(pageIndex, "0");
							temp.remove(temp.size() - 1);
							bits = insertNearest2(bits, pageNumber + 1, overFlow, maxPageCapacity);
						}

					}
				}
				page.insertNewPage(uniqueKey, bits);
				try {
					writeBitMapToFile(generatePath(i), page);
					// lastIndex = i;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
		if (!found) {
			try {
			for (int i1 = 0; i1 < bitsTemp.size(); i1++) {
				for (int j = 0; j < bitsTemp.get(i1).size(); j++) {
					bitsTemp.get(i1).set(j, "0");
				}
			}
			
				bitsTemp.get(pageNumber).set(pageIndex, "1");
				insertSorted(key, bitsTemp);
			} catch (Exception e) {
				ArrayList<ArrayList<String>> first = new ArrayList<ArrayList<String>>();
				ArrayList<String> inside = new ArrayList<String>();
				inside.add("1");
				first.add(inside);
				insertSorted(key, first);
			}
		}

	}

	public ArrayList<ArrayList<String>> insertNearest2(ArrayList<ArrayList<String>> arr, int index, String value,
			int maxCapacity) {
		if (index > arr.size() - 1) {
			ArrayList<String> newArray = new ArrayList<String>();
			newArray.add(value);
			arr.add(newArray);
			return arr;
		}
		if (arr.get(index).size() < maxCapacity) {
			arr.get(index).add(0, value);
			return arr;
		}
		String overFlow = arr.get(index).get(maxCapacity - 1);
		arr.get(index).add(0, value);
		arr.get(index).remove(maxCapacity - 1);
		return insertNearest2(arr, index + 1, overFlow, maxCapacity);
	}

	public void delete(int pageNumber, int indexInPage, boolean flag) throws ClassNotFoundException, IOException {
		for (int i = 0; i <= indexPageCount; i++) {
			BitMapPage page = getPageFromFile(generatePath(i));
			page.delete(pageNumber, indexInPage, flag);
			writeBitMapToFile(generatePath(i), page);
		}

	}

	public void update(int pageNumber, int indexInPage, Object key, int max)
			throws ClassNotFoundException, IOException, DBAppException {
		delete(pageNumber, indexInPage, true);
		readBitMap();
		insertToBitMap(key, pageNumber, indexInPage, max);

	}
	// public ArrayList<ArrayList<String>> OR (ArrayList<ArrayList<String>>)

	public int getPageCount() {
		return indexPageCount;
	}

	public boolean contains(int pageNumber, Object key) throws ClassNotFoundException, IOException, DBAppException {
		if (pageNumber > indexPageCount)
			return false;
		BitMapPage page = getPageFromFile(generatePath(pageNumber));
		if (page.contains(key))
			return true;
		return false;
	}


	public boolean lastIsBigger(int pageNumber, Object key) throws ClassNotFoundException, IOException, DBAppException {
		BitMapPage page = getPageFromFile(generatePath(pageNumber));
		TreeMap<Object,ArrayList<ArrayList<String>>> map = page.getBitMap();
		if(map.isEmpty())
			return false;
		System.out.println(pageNumber);
		Object last = map.lastKey();
		int compareResult = 0;
		try {
		switch (last.getClass().toString()) {
		case string:
			compareResult = ((String) last).compareTo((String) key);
			break;
		case integer:
			compareResult = ((Integer) last).compareTo((Integer) key);
			break;
		case doubled:
			compareResult = ((Double) last).compareTo((Double) key);
			break;
		case Boolean:
			compareResult = ((Boolean) last).compareTo((Boolean) key);
			break;
		default:
			compareResult = ((Date) last).compareTo((Date) key);
			break;
		}
		
		if (compareResult > 0) {
			return true;
		}
		return false;
	} catch(Exception e) {
		throw new DBAppException("invalid input");
	}
	}
	
	public boolean liesWithinPage(int pageNumber,Object key) throws ClassNotFoundException, IOException, DBAppException {
		BitMapPage page = getPageFromFile(generatePath(pageNumber));
		TreeMap<Object,ArrayList<ArrayList<String>>> map = page.getBitMap();
		if(map.isEmpty())
			return false;
		Object last = map.lastKey();
		Object first = map.firstKey();
		int compareFirst = 0;
		int compareLast = 0;
		try {
		switch (last.getClass().toString()) {
		case string:
			compareLast = ((String) last).compareTo((String) key);
			compareFirst = ((String) first).compareTo((String) key);
			break;
		case integer:
			compareLast = ((Integer) last).compareTo((Integer) key);
			compareFirst = ((Integer) first).compareTo((Integer) key);
			break;
		case doubled:
			compareLast = ((Double) last).compareTo((Double) key);
			compareFirst = ((Double) last).compareTo((Double) key);
			break;
		case Boolean:
			compareLast = ((Boolean) last).compareTo((Boolean) key);
			compareFirst = ((Boolean) last).compareTo((Boolean) key);
			break;
		default:
			compareLast = ((Date) last).compareTo((Date) key);
			compareFirst = ((Date) last).compareTo((Date) key);
			break;
		}
		}
		catch(Exception e) {
			throw new DBAppException("Invalid Types");
		}
		if(compareFirst<=0&&compareLast>=0) {
			return true;
		}
		return false;
	}

	// ArrayList operations
	public ArrayList<ArrayList<String>> negate(ArrayList<ArrayList<String>> arr) {
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < arr.size(); i++) {
			ArrayList<String> inside = arr.get(i);
			ArrayList<String> newInside = new ArrayList<String>();
			for (int j = 0; j < inside.size(); j++) {
				if (inside.get(j).equals("1"))
					newInside.add("0");
				else
					newInside.add("1");
			}
			result.add(newInside);

		}
		return result;
	}

	public ArrayList<ArrayList<String>> OR(ArrayList<ArrayList<String>> arr1, ArrayList<ArrayList<String>> arr2) {
		if (arr1.isEmpty()) {
			return arr2;
		}
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		for (int i = 0; i < arr1.size(); i++) {
			ArrayList<String> arr1Inside = arr1.get(i);
			ArrayList<String> arr2Inside = arr2.get(i);
			ArrayList<String> arr3Inside = new ArrayList<String>();
			for (int j = 0; j < arr1Inside.size(); j++) {
				if (arr1Inside.get(j).equals("1") || arr2Inside.get(j).equals("1"))
					arr3Inside.add("1");
				else
					arr3Inside.add("0");
			}
			result.add(arr3Inside);
		}
		return result;
	}
	public ArrayList<ArrayList<String>> AllOnesOrZeros(Entry<Object,ArrayList<ArrayList<String>>> entry,String value){
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> source = entry.getValue();
		for(int i =0;i<source.size();i++) {
			ArrayList<String> insideSource = source.get(i);
			ArrayList<String> insideResult = new ArrayList<String>();
		for(int j=0;j<insideSource.size();j++) {
			insideResult.add(value);
		}
		result.add(insideResult);
		}
		return result;
	}

	public ArrayList<ArrayList<String>> getExact(int pageNumber, Object value, boolean negated)
			throws ClassNotFoundException, IOException {
		//flag that != found no keys
		if(pageNumber == -2) {
			BitMapPage page =  getPageFromFile(generatePath(0));
			return AllOnesOrZeros(page.getBitMap().firstEntry(),"1");
		}
		if(pageNumber == -1) {
			BitMapPage page =  getPageFromFile(generatePath(0));
			return AllOnesOrZeros(page.getBitMap().firstEntry(),"0");
		}
		BitMapPage page = getPageFromFile(generatePath(pageNumber));
		ArrayList<ArrayList<String>> result =  page.getBitMap().get(value);
		if (negated)
			result = negate(page.getBitMap().get(value));
		if(result==null)
			return new  ArrayList<ArrayList<String>>();
		return result;
	}

	public ArrayList<ArrayList<String>> getAllAfter(int pageNumber, Object value, boolean included)
			throws ClassNotFoundException, IOException, DBAppException {
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		if(pageNumber==-1)
			return result;
		for (int i = pageNumber; i <= indexPageCount; i++) {
			BitMapPage page = getPageFromFile(generatePath(i));
			TreeMap<Object, ArrayList<ArrayList<String>>> map = page.getBitMap();
			int compareResult = 0;
			for (Object j : map.keySet()) {
				// to handle the case of getting from inside the page
				try {
				switch (j.getClass().toString()) {
				case string:
					compareResult = ((String) j).compareTo((String) value);
					break;
				case integer:
					compareResult = ((Integer) j).compareTo((Integer) value);
					break;
				case doubled:
					compareResult = ((Double) j).compareTo((Double)value);
					break;
				case Boolean:
					compareResult = ((Boolean) j).compareTo((Boolean) value);
					break;
				default:
					compareResult = ((Date) value).compareTo((Date) j);
					break;

				}
				} catch(Exception e) {
					throw new DBAppException("Invalid Types");
				}
				if (compareResult == 0 && included) {
					result = OR(result, map.get(j));
					
				}
				if(compareResult > 0) {
					result = OR(result, map.get(j));
				}
			}
		}
		return result;
	}
	public ArrayList<ArrayList<String>> getAllBefore(int pageNumber, Object value, boolean included)
			throws ClassNotFoundException, IOException, DBAppException {
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		if(pageNumber==-1)
			return result;
		for (int i = pageNumber; i >=0; i--) {
			BitMapPage page = getPageFromFile(generatePath(i));
			TreeMap<Object, ArrayList<ArrayList<String>>> map = page.getBitMap();
			int compareResult = 0;
			for (Object j : map.keySet()) {
				// to handle the case of getting from inside the page
				try {
				switch (j.getClass().toString()) {
				case string:
					compareResult = ((String) j).compareTo((String) value);
					break;
				case integer:
					compareResult = ((Integer) j).compareTo((Integer) value);
					break;
				case doubled:
					compareResult = ((Double) j).compareTo((Double) value);
					break;
				case Boolean:
					compareResult = ((Boolean) j).compareTo((Boolean) value);
					break;
				default:
					compareResult = ((Date) j).compareTo((Date) value);
					break;

				}
				}catch(Exception e) {
					throw new DBAppException("Invalid Types");
				}
				if (compareResult == 0 && included)
					result = OR(result, map.get(j));
				if(compareResult < 0)
					result = OR(result, map.get(j));
					
			}
		}
		return result;
	}

}
