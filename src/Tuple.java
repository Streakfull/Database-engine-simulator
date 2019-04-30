import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class Tuple implements Serializable {
	private static final long serialVersionUID = 1L;
	Map<String, Object> tupleData;

	public Tuple(Hashtable<String, Object> tupleData) {
		this.tupleData = tupleData;
		tupleData.put("TouchDate", new Date());
	}
	
	public void updateTouchDate() {
		tupleData.replace("TouchDate", new Date());
	}
	public void readTuple() {
		System.out.print("{ ");
		for (String i : tupleData.keySet()) {
			System.out.print(i + ": " + tupleData.get(i)+", ");
		}
		System.out.print("}");
	}
 public Object getKey(String key) {
	 return tupleData.get(key);
 }
 public String toString() {
	 return tupleData.toString();
 }
 public Hashtable<String,Object> getTupleData() {
	 return (Hashtable<String, Object>) tupleData;
 }
}
