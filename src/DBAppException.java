
public class DBAppException extends Exception{
	
	private String message;
	
	public DBAppException(String message) {
		super();
		this.message = message;
	}
	public String getMessage() {
		return message;
	}
}
