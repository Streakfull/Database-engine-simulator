import java.io.Serializable;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

public class BitMapPage implements Serializable {
	private TreeMap<Object, ArrayList<ArrayList<String>>> bitmap;
	private final String string = "class java.lang.String";
	private final String integer = "class java.lang.Integer";
	private final String doubled = "class java.lang.Double";
	private final String Boolean = "class java.lang.Boolean";
	private final String date = "class java.util.Date";

	public BitMapPage() {
		this.bitmap = new TreeMap<Object, ArrayList<ArrayList<String>>>();

	}
	public String compress(String str){
		String acc="";
		int count=0;
		for(int i=0;i<str.length();i++){
			if(str.charAt(i)=='0'){
				count++;
			}
			else{
				if(count>0){
					acc=acc+count;
				}
				acc=acc+".";
				count=0;
			}
		}
		if(count>0){
		String bitmap=acc+count;
		return acc+count;
		}
		else{
		String bitmap=acc;
		return acc;}
	}
	public String decompress(){
		String acc="";
		String count="";
		String str="";
		boolean found=false;
		for(int i=0;i<str.length();i++){
			if(str.charAt(i)=='.'){
				acc=acc+"1";
			}
			while(i<str.length()&& !(str.charAt(i)=='.')){
				count=count+str.charAt(i);
				i++;
				found=true;
			}
			if(found){
			for(int c=0;c<Integer.parseInt(count);c++){
				acc=acc+"0";
				
			}
			i--;
			}
			count="";
			found=false;
		}
		return acc;
	}

	public TreeMap<Object, ArrayList<ArrayList<String>>> getBitMap() {
		return (bitmap);
	}

	public boolean isFull() {
		return bitmap.size() == 4;
	}

	public void insertNewPage(Object key, ArrayList<ArrayList<String>> value) {
		if (checkEmpty(value))
			bitmap.remove(key);
		else
			bitmap.put(key, value);

	}

	public boolean checkEmpty(ArrayList<ArrayList<String>> value) {
		for (int i = 0; i < value.size(); i++) {
			ArrayList<String> inside = value.get(i);
			if (inside.contains("1"))
				return false;
		}
		return true;
	}

	public void insertValueToPage(Object key, String value, int pageIndex) {
		ArrayList<ArrayList<String>> newArrayList = bitmap.get(key);
		newArrayList.get(pageIndex).add(value);
		bitmap.put(key, newArrayList);
	}

	public boolean doesKeyBelong(Object key) throws DBAppException {
		if (bitmap.size() == 0)
			return true;
		int compareResult = 0;
		Object last = bitmap.lastKey();
		try {
			switch (key.getClass().toString()) {
			case string:
				compareResult = ((String) key).compareTo((String) last);
				break;
			case integer:
				compareResult = ((Integer) key).compareTo((Integer) last);
				break;
			case doubled:
				compareResult = ((Double) key).compareTo((Double) last);
				break;
			case Boolean:
				compareResult = ((Boolean) key).compareTo((Boolean) last);
				break;
			default:
				compareResult = ((Date) key).compareTo((Date) last);
				break;

			}
		} catch (Exception e) {
			throw new DBAppException("Invalid Types");
		}
		if (compareResult <= 0)
			return true;
		if (!this.isFull())
			return true;
		return false;

	}

	public boolean contains(Object key) throws DBAppException {
		for (Object i : bitmap.keySet()) {
			try {
				int compareResult = 0;
				switch (i.getClass().toString()) {
				case string:
					compareResult = ((String) key).compareTo((String) i);
					break;
				case integer:
					compareResult = ((Integer) key).compareTo((Integer) i);
					break;
				case doubled:
					compareResult = ((Double) key).compareTo((Double) i);
					break;
				case Boolean:
					compareResult = ((Boolean) key).compareTo((Boolean) i);
					break;
				default:
					compareResult = ((Date) key).compareTo((Date) i);
					break;
				}
				if (compareResult == 0)
					return true;

			} catch (Exception e) {
				throw new DBAppException("Invalid Types");
			}

		}
		return false;
	}

	public boolean checkFirst(Object key) {
		int compareResult = 0;
		if (bitmap.isEmpty())
			return false;
		Object last = bitmap.firstKey();
		switch (key.getClass().toString()) {
		case string:
			compareResult = ((String) key).compareTo((String) last);
			break;
		case integer:
			compareResult = ((Integer) key).compareTo((Integer) last);
			break;
		case doubled:
			compareResult = ((Double) key).compareTo((Double) last);
			break;
		case Boolean:
			compareResult = ((Boolean) key).compareTo((Boolean) last);
			break;
		default:
			compareResult = ((Date) key).compareTo((Date) last);
			break;

		}
		if (compareResult <= 0)
			return false;
		return true;
	}

	public int getRows() {
		Entry<Object, ArrayList<ArrayList<String>>> newArrayList = bitmap.firstEntry();
		ArrayList<ArrayList<String>> array = newArrayList.getValue();
		int result = 0;
		for (int i = 0; i < array.size(); i++) {
			for (int j = 0; j < array.get(i).size(); j++) {
				result++;
			}
		}
		return result;
	}

	public ArrayList<ArrayList<String>> getArray() {
		return bitmap.firstEntry().getValue();
	}

	public Entry<Object, ArrayList<ArrayList<String>>> pop() {
		Entry<Object, ArrayList<ArrayList<String>>> lastElement = bitmap.lastEntry();
		bitmap.remove(bitmap.lastKey());
		return lastElement;
	}

	public void read() {
		for (Object i : bitmap.keySet()) {
			ArrayList<ArrayList<String>> currentList = bitmap.get(i);
			System.out.println(i + currentList.toString());
		}

	}

	public void delete(int pageNumber, int Index, boolean keepInPlace) {
		ArrayList<Object> deletedValues = new ArrayList<Object>();
		for (Object i : bitmap.keySet()) {
			ArrayList<String> inside = bitmap.get(i).get(pageNumber);
			if (inside.size() == 1 && !keepInPlace) {
				bitmap.get(i).remove(pageNumber);
			} else {
				inside.remove(Index);
			}
			if (checkEmpty(bitmap.get(i)))
				deletedValues.add(i);
		}
		deletedValues.forEach(value -> bitmap.remove(value));

	}

}
