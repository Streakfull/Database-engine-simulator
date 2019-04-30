import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBApp {
	private String csvPath = "./src/data/metaData.csv";

	private String generatePath(String tableName) {
		return "./src/tables/" + tableName + ".class";
	}

	private String generateIndexPath(String name, String column) {
		return "./src/indexFiles/" + name + column + "INDEX.class";
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		try {
			setCsv(htblColNameType, strClusteringKeyColumn, strTableName);
			FileOutputStream file = new FileOutputStream(generatePath(strTableName));
			ObjectOutputStream output = new ObjectOutputStream(file);
			Table table = new Table(strTableName, strClusteringKeyColumn);
			output.writeObject(table);
			output.close();
			file.close();
		} catch (Exception e) {
			throw new DBAppException("Error in creating tale");
		}

	}

	public void ValidateData(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (htblColNameValue.isEmpty())
			throw new DBAppException("Data types are not valid");
		BufferedReader br;
		Hashtable<String, Object> dataTypes = new Hashtable<String, Object>();
		String line;
		try {
			br = new BufferedReader(new FileReader(csvPath));
			while ((line = br.readLine()) != null) {
				if (line.split(",")[0].equals(strTableName)) {
					// System.out.println(line.split(",")[2]);
					dataTypes.put(line.split(",")[1], line.split(",")[2]);
				}
			}
			br.close();
		} catch (IOException e) {
			System.out.println("meta file not found");
		}
		if (dataTypes.isEmpty()) {
			throw new DBAppException("Table not found");
		}

		for (String i : htblColNameValue.keySet()) {
			if (!i.equals("TouchDate")) {
				if (!(htblColNameValue.get(i).getClass().toString().substring(6).equals(dataTypes.get(i) + ""))) {
					throw new DBAppException("Data types are not valid");

				}
			}
		}
	}

	public void setCsv(Hashtable<String, String> htblColNameType, String key, String name) throws IOException {
		FileWriter csvWriter = new FileWriter(csvPath, true);
		for (String i : htblColNameType.keySet()) {
			String isKey = i.equals(key) ? "true" : "false";
			String value = htblColNameType.get(i).toString();
			csvWriter.append(name);
			csvWriter.append(",");
			csvWriter.append(i);
			csvWriter.append(",");
			csvWriter.append(value);
			csvWriter.append(",");
			csvWriter.append(isKey);
			csvWriter.append(",");
			csvWriter.append("false");
			csvWriter.append("\n");
		}
		csvWriter.flush();
		csvWriter.close();
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		ValidateData(strTableName, htblColNameValue);
		String path = generatePath(strTableName);
		try {
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream input = new ObjectInputStream(file);
			Table table = (Table) input.readObject();
			table.insertSorted(new Tuple(htblColNameValue));
			input.close();
			file.close();
			// updating the table variables back into the file.
			FileOutputStream fileO = new FileOutputStream(path);
			ObjectOutputStream output = new ObjectOutputStream(fileO);
			output.writeObject(table);
			output.close();
			fileO.close();
		} catch (IOException | ClassNotFoundException e) {
			throw new DBAppException("Table not found");
		}
	}

	/*
	 * public void insertIntoTable(String strTableName, Hashtable<String, Object>
	 * htblColNameValue) throws IOException, ClassNotFoundException, DBAppException
	 * { //ValidateData(strTableName, htblColNameValue); String path =
	 * generatePath(strTableName);
	 * 
	 * FileInputStream file = new FileInputStream(path); ObjectInputStream input =
	 * new ObjectInputStream(file); Table table = (Table) input.readObject();
	 * table.insertSorted(new Tuple(htblColNameValue)); input.close(); file.close();
	 * // updating the table variables back into the file. FileOutputStream fileO =
	 * new FileOutputStream(path); ObjectOutputStream output = new
	 * ObjectOutputStream(fileO); output.writeObject(table); output.close();
	 * fileO.close(); }
	 */

	public Table readTableFromFile(String strTableName) throws IOException, ClassNotFoundException {
		String path = generatePath(strTableName);
		FileInputStream file = new FileInputStream(path);
		ObjectInputStream input = new ObjectInputStream(file);
		Table table = (Table) input.readObject();
		input.close();
		file.close();
		return table;
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		ValidateData(strTableName, htblColNameValue);
		try {
			String path = generatePath(strTableName);
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream input = new ObjectInputStream(file);
			Table table = (Table) input.readObject();
			table.updateTuple(new Tuple(htblColNameValue), strKey);
			input.close();
			file.close();
			// updating the table variables back into the file.
			FileOutputStream fileO = new FileOutputStream(path);
			ObjectOutputStream output = new ObjectOutputStream(fileO);
			output.writeObject(table);
			output.close();
			fileO.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		try {
			String path = generatePath(strTableName);
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream input = new ObjectInputStream(file);
			Table table = (Table) input.readObject();
			table.deleteTuple(new Tuple(htblColNameValue));
			input.close();
			file.close();
			// updating the table variables back into the file.
			FileOutputStream fileO = new FileOutputStream(path);
			ObjectOutputStream output = new ObjectOutputStream(fileO);
			output.writeObject(table);
			output.close();
			fileO.close();
		} catch (Exception e) {
			throw new DBAppException("talbe not found");
		}
	}

	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		String path = "./src/indexFiles/" + strTableName + strColName + "INDEX.class";
		try {
			FileOutputStream file = new FileOutputStream(path);
			BitMapIndex newIndex = new BitMapIndex(strColName, strTableName);
			ObjectOutputStream output = new ObjectOutputStream(file);
			Table table = readTableFromFile(strTableName);
			tableRowsToBitMap(table, newIndex, strColName);
			updateCsvFile(strTableName, strColName);
			output.writeObject(newIndex);
			output.close();
			file.close();
		} catch (Exception e) {
			throw new DBAppException("fine not found");
		}

	}

	public void updateCsvFile(String tableName, String Column) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvPath));
		String line;
		ArrayList<String[]> file = new ArrayList<String[]>();
		while ((line = br.readLine()) != null) {
			String[] lineArray = line.split(",");
			if (lineArray[0].equals(tableName) && lineArray[1].equals(Column)) {
				lineArray[4] = "TRUE";
			}
			file.add(lineArray);

		}
		FileWriter csvWriter = new FileWriter(csvPath);
		file.forEach(arr -> {
			for (int i = 0; i < arr.length; i++) {
				try {
					csvWriter.append(arr[i]);
					csvWriter.append(",");
					if (i == arr.length - 1)
						csvWriter.append("\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		br.close();
		csvWriter.close();
	}

	public void tableRowsToBitMap(Table table, BitMapIndex index, String key)
			throws ClassNotFoundException, IOException, DBAppException {
		if (table.currentPage == -1)
			return;
		int rowsSoFar = 0;
		for (int i = 0; i <= table.getPageCount(); i++) {
			Page page = table.getPageFromFile(table.generatePath(i));
			int j;
			Object keyUpdated = null;
			for (j = 0; j < page.getSize(); j++) {
				keyUpdated = page.getKey(key, j);
				index.insertToBitMap(keyUpdated, i, j, page.getCapacity());

			}

		}
	}

	public void readBitMapFromFile(String tableName, String column) throws IOException, ClassNotFoundException {
		FileInputStream file = new FileInputStream(generateIndexPath(tableName, column));
		ObjectInputStream in = new ObjectInputStream(file);
		BitMapIndex index = (BitMapIndex) in.readObject();
		index.readBitMap();
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			return null;
		if (arrSQLTerms.length != strarrOperators.length + 1) {
			System.out.println("Wrong format, operators length do not match the sql terms");
			return null;
		}
		try {
			Table table = readTableFromFile(arrSQLTerms[0]._strTableName);
			return table.handleSqlQueries(arrSQLTerms, strarrOperators).iterator();
		} catch (ClassNotFoundException e) {
			throw new DBAppException("class not found");
		} catch (IOException e) {
			// throw new DBAppException("Table not found");
			e.printStackTrace();
			return null;
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
			return null;
		}

	}

	public void init() {
	}

	public static void main(String[] args) {
		// Overwriting a table is not supported
		// defining a table of 8 rows [A table page holds 2 rows and a bitmap page holds
		// 4 rows]
		String column1 = "Name";
		String column2 = "ID";
		String column3 = "DPS";
		String column4 = "Weak";
		String column5 = "Birth Date";

		Hashtable<String, Object> test1 = new Hashtable<String, Object>();
		test1.put(column1, "Ashe");
		test1.put(column2, (Integer) 1);
		test1.put(column3, (Double) 1.6);
		test1.put(column4, (Boolean) true);
		test1.put(column5, new Date());
		Hashtable<String, String> tableData = new Hashtable<String, String>();
		tableData.put("Name", "java.lang.String");
		tableData.put("ID", "java.lang.Integer");
		tableData.put("DPS", "java.lang.Double");
		tableData.put("Weak", "java.lang.Boolean");
		tableData.put("Birth Date", "java.util.Date");

		Hashtable<String, Object> row2 = new Hashtable<String, Object>();
		row2.put(column1, "Irelia");
		row2.put(column2, (Integer) 2);
		row2.put(column3, (Double) 15.8);
		row2.put(column4, (Boolean) false);
		row2.put(column5, new Date());
		Hashtable<String, Object> row3 = new Hashtable<String, Object>();
		row3.put(column1, "Gnar");
		row3.put(column2, (Integer) 3);
		row3.put(column3, (Double) 0.7);
		row3.put(column4, (Boolean) true);
		row3.put(column5, new Date());

		Hashtable<String, Object> row4 = new Hashtable<String, Object>();
		row4.put(column1, "Syndra");
		row4.put(column2, (Integer) 4);
		row4.put(column3, (Double) 1.8);
		row4.put(column4, (Boolean) false);
		row4.put(column5, new Date());
		Hashtable<String, Object> row5 = new Hashtable<String, Object>();
		row5.put(column1, "Braum");
		row5.put(column2, (Integer) 5);
		row5.put(column3, (Double) 1.8);
		row5.put(column4, (Boolean) true);
		row5.put(column5, new Date());
		Hashtable<String, Object> row6 = new Hashtable<String, Object>();
		row6.put(column1, "Jayce");
		row6.put(column2, (Integer) 6);
		row6.put(column3, (Double) 86.2);
		row6.put(column4, (Boolean) true);
		row6.put(column5, new Date());
		Hashtable<String, Object> row7 = new Hashtable<String, Object>();
		row7.put(column1, "Tristana");
		row7.put(column2, (Integer) 10);
		row7.put(column3, (Double) 70.5);
		row7.put(column4, (Boolean) false);
		row7.put(column5, new Date());
		DBApp engine = new DBApp();

		try {
			String tableName = "FirstTable";
			engine.createTable(tableName, "ID", tableData);
			engine.createBitmapIndex(tableName, "ID");
			engine.createBitmapIndex(tableName, "DPS");
			engine.insertIntoTable(tableName, row7);
			engine.insertIntoTable(tableName, test1);
			engine.insertIntoTable(tableName, row2);
			engine.insertIntoTable(tableName, row3);
			engine.insertIntoTable(tableName, row5);
			engine.insertIntoTable(tableName, row4);
			engine.insertIntoTable(tableName, row6);
			// printing table
			//engine.readTableFromFile(tableName).readTable();
			///engine.deleteFromTable(tableName,updateTest);
			//engine.updateTable(tableName, "3", updateTest);
			//engine.insertIntoTable(tableName, row7);
			engine.readTableFromFile(tableName).readTable();
			// printing bit map
			engine.readBitMapFromFile(tableName, "ID");
			engine.readBitMapFromFile(tableName, "DPS");
			/////////////////////////////////////////////
			// Sql queries
			//engine.createBitmapIndex(tableName, "DPS");
					// select from Champions where (DPS <=0.7 OR(ID = 7 AND DPS>60) OR NAME =
			// SYNDRA)
			SQLTerm x = new SQLTerm();
			SQLTerm y = new SQLTerm();
			SQLTerm z = new SQLTerm();
			SQLTerm e = new SQLTerm();
			x._strTableName = tableName;
			x._strColumnName = "DPS";
			x._strOperator = "<=";
			x._objValue = (Double) 0.7;
			///////////
			y._strTableName = tableName;
			y._objValue = (Integer) 7;
			y._strColumnName = "ID";
			y._strOperator = "=";
			/////////////
			z._strTableName = tableName;
			z._strColumnName = "DPS";
			z._strOperator = ">";
			z._objValue = (Double) 60.5;
			////////////
			e._strTableName = tableName;
			e._strColumnName = "Name";
			e._strOperator = "=";
			e._objValue = "Syndra";

			SQLTerm[] arrSQLTerms = { x, y, z, e };
			String[] strarrOperators = { "OR", "AND", "OR" };
			engine.selectFromTable(arrSQLTerms, strarrOperators);
			
			
			
		} catch (ClassNotFoundException | IOException | DBAppException e) {
			System.out.println(e.getMessage());
		}

	}

}
