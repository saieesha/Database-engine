import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class Commands {
	
	/* This method determines what type of command the userCommand is and
	 * calls the appropriate method to parse the userCommand String. 
	 */
	public static void parseUserCommand (String PUserCommand) {
		
		/* Clean up command string so that each token is separated by a single space */
		PUserCommand = PUserCommand.replaceAll("\n", " ");    // Remove newlines
		PUserCommand = PUserCommand.replaceAll("\r", " ");    // Remove carriage returns
		PUserCommand = PUserCommand.replaceAll(",", " , ");   // Tokenize commas
		PUserCommand = PUserCommand.replaceAll("\\(", " ( "); // Tokenize left parentheses
		PUserCommand = PUserCommand.replaceAll("\\)", " ) "); // Tokenize right parentheses
		PUserCommand = PUserCommand.replaceAll("( )+", " ");  // Reduce multiple spaces to a single space

		/* commandTokens is an array of Strings that contains one lexical token per array
		 * element. The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement 
		 */
		ArrayList<String> parseCommandTokens = new ArrayList<String>(Arrays.asList(PUserCommand.split(" ")));

		/*
		*  This switch handles a very small list of hard-coded commands from SQL syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (parseCommandTokens.get(0).toLowerCase()) {

		    case "delete":
			    System.out.println("Case: DELETE");
			    parseDelete(parseCommandTokens);
			    break;
			case "select":
				System.out.println("Case: SELECT");
				parseQuery(parseCommandTokens);
				break;
			case "show":
				System.out.println("Case: SHOW");
				show(parseCommandTokens);
				break;
			case "create":
				switch(parseCommandTokens.get(1)){
					case "table":
						System.out.println("Case: CREATE TABLE");
						parseCreateTable(PUserCommand);
						break;
					case "index":
						System.out.println("Case: CREATE INDEX");
						parseCreateIndex(PUserCommand);
						break;
					default:
						System.out.println("I do not understand this command!");
						break;
				}
				break;
			case "insert":
				System.out.println("Case: INSERT");
				parseInsert(parseCommandTokens);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "update":
				System.out.println("Case: UPDATE");
				parseUpdate(parseCommandTokens);
				break;
			case "quit":
				Settings.setExit(true);
				break;
			case "drop":
				System.out.println("Case: DROP");
				dropTable(parseCommandTokens);
				break;
			case "exit":
				Settings.setExit(true);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + PUserCommand + "\"");
				break;
		}
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + Settings.getVersion());
		System.out.println(Settings.getCopyright());
	}

	// to check if table already exists
	public static boolean tableExists(String tabTableName){
		tabTableName = tabTableName+".tbl";
		
		try {	
			File tabDataDir = new File(Constants.userDataDir);
			if (tabTableName.equalsIgnoreCase(Constants.TABLE_CATALOG+Constants.FILE_TYPE) || tabTableName.equalsIgnoreCase(Constants.COLUMN_CATALOG+Constants.FILE_TYPE))
				tabDataDir = new File(Constants.catalogDir) ;
			
			String[] tabOldTables = tabDataDir.list();
			for (int i=0; i<tabOldTables.length; i++) {
				if(tabOldTables[i].equals(tabTableName))
					return true;
			}
		}
		catch (Exception e) {
			System.out.println("Unable to create directory");
			System.out.println(e);
		}

		return false;
	}

	// WORKING!!!
	public static void parseCreateTable(String parseCommand) {
		/* TODO: Before attempting to create new table file, check if the table already exists */
		
		System.out.println("Stub: parseCreateTable method");
		System.out.println("Command: " + parseCommand);
		ArrayList<String> parseCommandTokens = commandStringToTokenList(parseCommand);

		/* Extract the table name from the command string token list */
		String tableFileName = parseCommandTokens.get(2) + ".tbl";

		System.out.println("Parsing the string:\"" + parseCommand + "\"");
		
		String tableName = parseCommand.split(" ")[2];
		String parseCols = parseCommand.split(tableName)[1].trim();
		String[] create_cols = parseCols.substring(1, parseCols.length()-1).split(",");
		
		int i = 0;
		while(i < create_cols.length) {
			create_cols[i] = create_cols[i].trim();
			i++;
		}
		
		if(tableExists(tableName)){
			System.out.println("Table "+tableName+" already exists.");
		}
		else
		{
			Table.createTable(tableName, create_cols);		
		}


		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 *  Note that this doesn't create the table file in the correct directory structure
			 */

			/* Create a new table file whose initial size is one page (i.e. page size number of bytes) */
			RandomAccessFile parseTableFile = new RandomAccessFile("data/user_data/" + tableName, "rw");
			parseTableFile.setLength(Settings.getPageSize());

			/* Write page header with initial configuration */
			parseTableFile.seek(0);
			parseTableFile.writeInt(0x0D);       // Page type
			parseTableFile.seek(0x02);
			parseTableFile.writeShort(0x01FF);   // Offset beginning of cell content area
			parseTableFile.seek(0x06);
			parseTableFile.writeInt(0xFFFFFFFF); // Sibling page to the right
			parseTableFile.seek(0x0A);
			parseTableFile.writeInt(0xFFFFFFFF); // Parent page 
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
		/*  Code to insert an entry in the TABLES meta-data for this new table.
		 *  i.e. New row in davisbase_tables if you're using that mechanism for meta-data.
		 */
		
		/*  Code to insert entries in the COLUMNS meta data for each column in the new table.
		 *  i.e. New rows in davisbase_columns if you're using that mechanism for meta-data.
		 */
	}

	// WORKING!!!
	public static void show(ArrayList<String> showCommandTokens) {
		System.out.println("Command: " + tokensToCommandString(showCommandTokens));
		System.out.println("Stub: This is the show method");
		/* TODO: Your code goes here */

		System.out.println("Parsing the string:\"show tables\"");
		

		String showTable = Constants.TABLE_CATALOG;
		String[] showCols = {Constants.HEADER_TABLE_NAME};
		String[] showCondition = new String[0];
		Table.select(showTable, showCols, showCondition,true);
	}

	/*
	 *  Stub method for inserting a new record into a table.
	 */
	public static void parseInsert (ArrayList<String> parseInsertCommandTokens) {
		System.out.println("Command: " + tokensToCommandString(parseInsertCommandTokens));
		System.out.println("Stub: This is the insertRecord method");
		/* TODO: Your code goes here */

		String PIcommand = tokensToCommandString(parseInsertCommandTokens);

		try{
			System.out.println("Parsing the string:\"" + PIcommand + "\"");
			
			String table = PIcommand.split(" ")[2];
			String rawCols = PIcommand.split("values")[1].trim();
			String[] insertValsInit = rawCols.substring(1, rawCols.length()-1).split(",");
			String[] insertVals = new String[insertValsInit.length + 1];
			for(int i = 1; i <= insertValsInit.length; i++)
				insertVals[i] = insertValsInit[i-1].trim();
		
			if(tableExists(table)){
				Table.insertInto(table, insertVals,Constants.userDataDir+"/");
			}
			else
			{
				System.out.println("Table "+table+" does not exist.");
			}
			}
			catch(Exception e)
			{
				System.out.println(e+e.toString());
			}
	}
	
	// DELETE - WORKING!!!
	public static void parseDelete(ArrayList<String> parseDeleteCommandTokens) {
		System.out.println("Command: " + tokensToCommandString(parseDeleteCommandTokens));
		System.out.println("Stub: This is the deleteRecord method");
		/* TODO: Your code goes here */

		String PIcommand = tokensToCommandString(parseDeleteCommandTokens);

		System.out.println("Parsing the string:\"" + PIcommand + "\"");
		
		String table = PIcommand.split(" ")[2];
		String[] PIrawConditionArray = PIcommand.split("where");
		String PIrawCondition = PIrawConditionArray.length>1?PIrawConditionArray[1]:"";
		String[] PIparsedCondition = PIrawConditionArray.length>1?parseCondition(PIrawCondition) : new String[0];
		if(tableExists(table)){
			Table.delete(table, PIparsedCondition, Constants.userDataDir);
		}
		else
		{
			System.out.println("Table "+table+" does not exist.");
		}

	}
	

	/**
	 *  Stub method for dropping tables
	 */
	// DROP TABLE - WORKING!!!
	public static void dropTable(ArrayList<String> dropTableCommandTokens) {
		System.out.println("Command: " + tokensToCommandString(dropTableCommandTokens));
		System.out.println("Stub: This is the dropTable method.");

		String dropTableCommand = tokensToCommandString(dropTableCommandTokens);

		System.out.println("Parsing the string:\"" + dropTableCommand + "\"");
		
		String[] dropTableTokens=dropTableCommand.split(" ");
		String dropTableName = dropTableTokens[2];
		if(tableExists(dropTableName)){
			Table.drop(dropTableName);
		}
		else{
			System.out.println("Table "+dropTableName+" does not exist.");
		}
	}

	/**
	 *  Stub method for executing queries
	 */
	// SELECT
	public static void parseQuery(ArrayList<String> parseQueryCommandTokens) {
		System.out.println("Command: " + tokensToCommandString(parseQueryCommandTokens));
		System.out.println("Stub: This is the parseQuery method");

		String parseQueryCommand = tokensToCommandString(parseQueryCommandTokens);

		System.out.println("Parsing the string:\"" + parseQueryCommand + "\"");
		
		String[] parsedCondition;
		String[] parsedQueryColumns;
		String[] cols_condition = parseQueryCommand.split("where");
		if(cols_condition.length > 1){
			parsedCondition = parseCondition(cols_condition[1].trim());
		}
		else{
			parsedCondition = new String[0];
		}
		String[] select = cols_condition[0].split("from");
		String tableName = select[1].trim();
		String parseQueryCols = select[0].replace("select", "").trim();
		if(parseQueryCols.contains("*")){
			parsedQueryColumns = new String[1];
			parsedQueryColumns[0] = "*";
		}
		else{
			parsedQueryColumns = parseQueryCols.split(",");
			for(int i = 0; i < parsedQueryColumns.length; i++)
				parsedQueryColumns[i] = parsedQueryColumns[i].trim();
		}
		
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}
		else
		{
		    Table.select(tableName, parsedQueryColumns, parsedCondition,true);
		}
	}

	public static String[] parseCondition(String PCondition){
		String parCondition[] = new String[3];
		String parseTemp[] = new String[2];
		if(PCondition.contains(Constants.EQUALS_SIGN)) {
			parseTemp = PCondition.split(Constants.EQUALS_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.EQUALS_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}
		
		if(PCondition.contains(Constants.LESS_THAN_SIGN)) {
			parseTemp = PCondition.split(Constants.LESS_THAN_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.LESS_THAN_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}
		
		if(PCondition.contains(Constants.GREATER_THAN_SIGN)) {
			parseTemp = PCondition.split(Constants.GREATER_THAN_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.GREATER_THAN_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}
		
		if(PCondition.contains(Constants.LESS_THAN_EQUAL_SIGN)) {
			parseTemp = PCondition.split(Constants.LESS_THAN_EQUAL_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.LESS_THAN_EQUAL_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}

		if(PCondition.contains(Constants.GREATER_THAN_EQUAL_SIGN)) {
			parseTemp = PCondition.split(Constants.GREATER_THAN_EQUAL_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.GREATER_THAN_EQUAL_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}
		
		if(PCondition.contains(Constants.NOT_EQUAL_SIGN)) {
			parseTemp = PCondition.split(Constants.NOT_EQUAL_SIGN);
			parCondition[0] = parseTemp[0].trim();
			parCondition[1] = Constants.NOT_EQUAL_SIGN;
			parCondition[2] = parseTemp[1].trim();
		}

		return parCondition;
	}

	// UPDATE
	public static void parseUpdate(ArrayList<String> pUpdatecommandTokens) {
		System.out.println("Command: " + tokensToCommandString(pUpdatecommandTokens));
		System.out.println("Stub: This is the parseUpdate method");

		String parseUpdateCommand = tokensToCommandString(pUpdatecommandTokens);

		System.out.println("Parsing the string:\"" + parseUpdateCommand + "\"");
		
		String pUpdateTable = parseUpdateCommand.split(" ")[1];
		String pUpdateWhereCondition = parseUpdateCommand.split("set")[1].split("where")[1];
		String pUpdatesetCondition = parseUpdateCommand.split("set")[1].split("where")[0];
		String[] parsedCondition = parseCondition(pUpdateWhereCondition);
		String[] parsedSetCondition = parseCondition(pUpdatesetCondition);
		if(!tableExists(pUpdateTable)){
			System.out.println("Table "+pUpdateTable+" does not exist.");
		}
		else
		{
			Table.update(pUpdateTable, parsedCondition, parsedSetCondition, Constants.userDataDir);
		}
	}

	public static void parseCreateIndex(String parseCreateCommand)
	{
		// String command = tokensToCommandString(commandTokens);

		System.out.println("Parsing the string:\"" + parseCreateCommand + "\"");
		
		String[] parseCreateTokens=parseCreateCommand.split(" ");
		String parseCreateTableName = parseCreateTokens[3];
		String[] parseCreateTemp = parseCreateCommand.split(parseCreateTableName);
		String parseCreateCols = parseCreateTemp[1].trim();
		String[] create_cols = parseCreateCols.substring(1, parseCreateCols.length()-1).split(",");
		
		int i = 0;
		while(i < create_cols.length) {
			create_cols[i] = create_cols[i].trim();
			i++;
		}
		
		
		Table.createIndex(parseCreateTableName, create_cols);	
	}

	public static String tokensToCommandString (ArrayList<String> stringCommandTokens) {
		String commandString = "";
		for(String TCtoken : stringCommandTokens)
			commandString = commandString + TCtoken + " ";
		return commandString;
	}
	
	public static ArrayList<String> commandStringToTokenList (String stringCommand) {
		stringCommand.replace("\n", " ");
		stringCommand.replace("\r", " ");
		stringCommand.replace(",", " , ");
		stringCommand.replace("\\(", " ( ");
		stringCommand.replace("\\)", " ) ");
		ArrayList<String> tokenizedCommand = new ArrayList<String>(Arrays.asList(stringCommand.split(" ")));
		return tokenizedCommand;
	}

	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		out.println(Utils.printSeparator("*",80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("CREATE TABLE table_name (<column_name <datatype> <NOT NULL/UNIQUE>)");
		out.println("\tCreate a new table in the database");
		out.println("SELECT ⟨column_list⟩ FROM table_name [WHERE condition];\n");
		out.println("\tDisplay table records whose optional condition");
		out.println("\tis <column_name> = <value>.\n");
		out.println("INSERT INTO table_name VALUES (value1, value2, ...);\n");
		out.println("\tInsert new record into the table.");
		out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("DROP TABLE table_name;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(Utils.printSeparator("*",80));
	}
	
}
