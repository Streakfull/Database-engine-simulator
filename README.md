# Database Engine Simulator

A Java-based simulator that demonstrates the core internals of a relational database engine. The project implements page-based storage, sorted clustering, bitmap indexing with run-length encoding compression, and a simple SQL query executor — all persisted to disk via Java serialization.

## Features

- **Table management** — create tables with typed schemas (String, Integer, Double, Boolean, Date)
- **Page-based storage** — tuples are stored across fixed-capacity pages, sorted by clustering key
- **CRUD operations** — insert, update, and delete with automatic page overflow handling
- **Bitmap indexing** — create bitmap indexes on any column for fast selective queries
- **RLE compression** — bitmap pages use run-length encoding to minimize storage
- **SQL queries** — `SELECT` with multi-condition `WHERE` clauses using `AND`/`OR` operators and comparison operators (`=`, `<`, `>`, `<=`, `>=`)
- **Schema metadata** — table schemas tracked in a CSV catalog (`metadata.csv`)

## Architecture

```
src/
├── DBApp.java            Main API — create, insert, update, delete, select, index
├── Table.java            Table logic — paging, sorted insert, query execution
├── Page.java             Fixed-capacity tuple container, binary search
├── Tuple.java            Row wrapper (HashMap + auto-generated timestamp)
├── BitMapIndex.java      Bitmap index spanning multiple pages
├── BitMapPage.java       Single bitmap page with RLE compress/decompress
├── SQLTerm.java          Query condition (column, operator, value)
└── DBAppException.java   Custom exception
```

**Storage layout at runtime:**

| Path | Contents |
| ---- | -------- |
| `src/tables/` | Serialized `Table` objects |
| `src/pages/` | Serialized `Page` objects (`{table}{pageNum}.class`) |
| `src/indexFiles/` | Serialized bitmap indexes (`{table}{column}INDEX.class`) |
| `src/data/metaData.csv` | Schema catalog |

## Getting Started

### Prerequisites

- Java SE 8+

### Running

The project is an Eclipse IDE project. Import it directly or compile from the command line:

```bash
# Compile
javac -d bin src/*.java

# Run
java -cp bin DBApp
```

### Configuration

Edit `config/DBApp.properties` to tune storage parameters:

```properties
MaximumRowsCountinPage = 2    # tuples per page
BitmapSize = 4                # entries per bitmap page
```

## Usage Example

```java
DBApp engine = new DBApp();

// Define schema
Hashtable<String, String> columns = new Hashtable<>();
columns.put("ID", "java.lang.Integer");
columns.put("Name", "java.lang.String");
columns.put("Score", "java.lang.Double");

// Create table (ID is the clustering key)
engine.createTable("Players", "ID", columns);

// Create bitmap index
engine.createBitmapIndex("Players", "Score");

// Insert a row
Hashtable<String, Object> row = new Hashtable<>();
row.put("ID", 1);
row.put("Name", "Alice");
row.put("Score", 95.5);
engine.insertIntoTable("Players", row);

// Query
SQLTerm condition = new SQLTerm();
condition._strTableName = "Players";
condition._strColumnName = "Score";
condition._strOperator = ">";
condition._objValue = 90.0;
engine.selectFromTable(new SQLTerm[]{ condition }, new String[]{});
```
