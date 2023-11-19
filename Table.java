import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class Table {

    public static int numRecords;

    public static int getPageCount(RandomAccessFile file) {
        int pagesCount = 0;
        try {
            pagesCount = (int) (file.length() / ((long) (Constants.pageSize)));
        } catch (Exception e) {
            System.out.println(e);
        }
        return pagesCount;
    }

    public static void createTable(String table, String[] columns) {
        try {
            String[] newColumns = new String[columns.length + 1];
            newColumns[0] = "rowid INT UNIQUE";
            for (int i = 0; i < columns.length; i++) {
                newColumns[i + 1] = columns[i];
            }

            RandomAccessFile file = new RandomAccessFile(Constants.userDataDir + table + Constants.FILE_TYPE, "rw");
            file.setLength(Constants.pageSize);
            file.seek(0);
            file.writeByte(Constants.recordsPage);
            file.close();

            String[] insertValues = { "0", table, String.valueOf(0) };
            insertInto(Constants.TABLE_CATALOG, insertValues, Constants.catalogDir);

            for (int i = 0; i < newColumns.length; i++) {
                String[] attributes = newColumns[i].split(" ");
                String nullSetting;
                String isUnique = "NO";

                if (attributes.length > 2) {
                    nullSetting = "NO";
                    if (attributes[2].toUpperCase().trim().equals("UNIQUE"))
                        isUnique = "YES";
                    else
                        isUnique = "NO";
                } else
                    nullSetting = "YES";

                String[] values = { "0", table, attributes[0], attributes[1].toUpperCase(), String.valueOf(i + 1),
                        nullSetting, isUnique };
                insertInto("davisbase_columns", values, Constants.catalogDir);
            }
        } catch (Exception e) {
            System.out.println(e);

        }
    }

    public static void insertInto(String table, String[] values, String dir_s) {
        try {
            RandomAccessFile file = new RandomAccessFile(dir_s + table + Constants.FILE_TYPE, "rw");
            insertInto(file, table, values);
            file.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void insertInto(RandomAccessFile file, String table, String[] values) {
        String[] dtype = getDataType(table);
        String[] isNullAllowed = getNullable(table);
        String[] isUnique = getUnique(table);
        int rowId = 0;
        if (Constants.TABLE_CATALOG.equals(table) || Constants.COLUMN_CATALOG.equals(table)) {
            // iterate through the file to get latest rowid
            int pageCount = getPageCount(file);
            int currentPage = 1;
            for (int i = 1; i <= pageCount; i++) {
                int rightMostFile = BPlusTree.getRightMost(file, i);
                if (rightMostFile == 0)
                    currentPage = i;
            }
            int[] keyArray = BPlusTree.getKeyArray(file, currentPage);
            for (int i = 0; i < keyArray.length; i++)
                if (keyArray[i] > rowId)
                    rowId = keyArray[i];
        } else {
            // do a select to get the latest rowid
            Records rowIdRecords = select(Constants.TABLE_CATALOG, new String[] { "cur_row_id" },
                    new String[] { "table_name", "=", table }, false);
            rowId = Integer.parseInt(rowIdRecords.content.entrySet().iterator().next().getValue()[2]);
        }

        values[0] = String.valueOf(rowId + 1);

        // check for null values
        for (int i = 0; i < isNullAllowed.length; i++)
            if (values[i].equals("null") && isNullAllowed[i].equals(Constants.FALSE)) {
                System.out.println("NULL-value constraint violation");
                System.out.println();
                return;
            }

        // check for unique constraints
        for (int i = 0; i < isUnique.length; i++)
            if (isUnique[i].equals("YES")) {
                System.out.println("Checking for unique constraint violation");
                System.out.println();

                try {
                    String[] columnName = getColName(table);

                    String[] cmp = { columnName[i], "=", values[i] };
                    Records records = select(table, new String[] { "*" }, cmp, false);

                    if (records.num_row > 0) {
                        System.out.println("Duplicate key found for " + columnName[i].toString());
                        System.out.println();
                        return;
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }

            }

        // check for the uniqueness of new row id
        int newRowId = Integer.parseInt(values[0]);
        int page = searchKeyPage(file, newRowId);
        if (page != 0)
            if (BPlusTree.hasKey(file, page, newRowId)) {
                System.out.println("Uniqueness constraint violation");
                System.out.println("for");
                for (int k = 0; k < values.length; k++)
                    System.out.println(values[k]);

                return;
            }

        if (page == 0)
            page = 1;

        byte[] typeCode = new byte[dtype.length - 1];
        short payloadSize = (short) calPayloadSize(table, values, typeCode);
        int cellSize = payloadSize + 6;
        int offset = BPlusTree.checkLeafSpace(file, page, cellSize);

        if (offset != -1) {
            BPlusTree.insertLeafCell(file, page, offset, payloadSize, newRowId, typeCode, values);

        } else {
            BPlusTree.splitLeaf(file, page);
            insertInto(file, table, values);
        }

        if (!Constants.TABLE_CATALOG.equals(table) && !Constants.COLUMN_CATALOG.equals(table)) {
            update(Constants.TABLE_CATALOG,
                    new String[] { "table_name", "=", table },
                    new String[] { "cur_row_id", "=", String.valueOf(values[0]) },
                    Constants.catalogDir);
        }
    }

    public static int searchKeyPage(RandomAccessFile file, int key) {
        try {
            // get the number of pages
            int pageCount = getPageCount(file);

            // iterate over all the pages
            for (int page = 1; page <= pageCount; page++) {
                // get the page type
                file.seek((page - 1) * Constants.pageSize);
                byte pageCategory = file.readByte();

                if (pageCategory == Constants.recordsPage) {
                    // get all the keys on current page
                    int[] keyArray = BPlusTree.getKeyArray(file, page);

                    if (keyArray.length == 0)
                        return 0;

                    int rightMostFile = BPlusTree.getRightMost(file, page);

                    // if key in current page return current page number
                    if (keyArray[0] <= key && key <= keyArray[keyArray.length - 1]) {
                        return page;
                    }
                    // if last page and key less than last key on this page, return current page
                    else if (rightMostFile == 0 && keyArray[keyArray.length - 1] < key) {
                        return page;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return 1;
    }

    public static void createIndex(String table, String[] column_array) {
        try {
            String path = Constants.userDataDir;

            RandomAccessFile file = new RandomAccessFile(path + table + Constants.FILE_TYPE, "rw");
            String[] columnName = getColName(table);

            String column_name = "";
            for (int i = 0; i < column_array.length; i++) {
                if (i == column_array.length - 1) {
                    column_name += column_array[i];
                } else {
                    column_name += column_array[i] + ".";
                }

            }
            BTree b_tree = new BTree(
                    new RandomAccessFile(path + table + "." + column_name + Constants.INDEX_FILE_TYPE, "rw"));
            int control = 0;
            for (int j = 0; j < column_array.length; j++)
                for (int i = 0; i < columnName.length; i++)
                    if (column_array[j].equals(columnName[i]))
                        control = i;

            try {

                int numOfPages = getPageCount(file);
                for (int page = 1; page <= numOfPages; page++) {

                    file.seek((page - 1) * Constants.pageSize);
                    byte pageType = file.readByte();
                    if (pageType == Constants.recordsPage) {
                        byte numOfCells = BPlusTree.getCellNumber(file, page);

                        for (int i = 0; i < numOfCells; i++) {
                            long loc = BPlusTree.getCellLoc(file, page, i);
                            String[] vals = retrieveValues(file, loc);
                            int rowid = Integer.parseInt(vals[0]);

                            b_tree.add(String.valueOf(vals[control]), String.format("%04x", loc));
                        }
                    } else
                        continue;
                }

            } catch (Exception e) {
                System.out.println("Error at indexing");
                e.printStackTrace();
            }
            file.close();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String[] retrieveValues(RandomAccessFile Acessfile, long LOC) {

        String[] data = null;
        try {

            SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.datePattern);

            Acessfile.seek(LOC + 2);
            int row = Acessfile.readInt();
            int numCols = Acessfile.readByte();

            byte[] typeCode = new byte[numCols];
            Acessfile.read(typeCode);

            data = new String[numCols + 1];

            data[0] = Integer.toString(row);

            for (int i = 1; i <= numCols; i++) {
                switch (typeCode[i - 1]) {
                    case Constants.NULL:
                        Acessfile.readByte();
                        data[i] = "null";
                        break;

                    case Constants.TINYINT:
                        data[i] = Integer.toString(Acessfile.readByte());
                        break;

                    case Constants.INT:
                        data[i] = Integer.toString(Acessfile.readInt());
                        break;

                    case Constants.LONG:
                        data[i] = Long.toString(Acessfile.readLong());
                        break;

                    case Constants.FLOAT:
                        data[i] = String.valueOf(Acessfile.readFloat());
                        break;

                    case Constants.DOUBLE:
                        data[i] = String.valueOf(Acessfile.readDouble());
                        break;

                    case Constants.DATETIME:
                        Long temp = Acessfile.readLong();
                        Date dateTime = new Date(temp);
                        data[i] = dateFormat.format(dateTime);
                        break;

                    case Constants.DATE:
                        temp = Acessfile.readLong();
                        Date date = new Date(temp);
                        data[i] = dateFormat.format(date).substring(0, 10);
                        break;

                    default:
                        int len = typeCode[i - 1] - 0x0C;
                        byte[] bytes = new byte[len];
                        Acessfile.read(bytes);
                        data[i] = new String(bytes);
                        break;
                }
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        return data;
    }

    public static Records select(String table, String[] cols, String[] cmp, boolean display){
		try{
			//get the path from where to pick the file
			String path = Constants.userDataDir;
			if (table.equalsIgnoreCase(Constants.TABLE_CATALOG) || table.equalsIgnoreCase(Constants.COLUMN_CATALOG))
				path = Constants.catalogDir;
			
			
			RandomAccessFile file = new RandomAccessFile(path+table+Constants.FILE_TYPE, "rw");
			
			//get column names and data types
			String[] columnName = getColName(table);
			String[] dataType = getDataType(table);
			
			Records records = new Records();
			
			//handle null values in comparision
			if (cmp.length > 0 && cmp[1].equals("=") && cmp[2].equalsIgnoreCase("null")) 
			{
				System.out.println("Empty Set");
				file.close();
				return null;
			}
			if (cmp.length > 0 && cmp[1].equals("!=") && cmp[2].equalsIgnoreCase("null")) 
			{
				cmp = new String[0];
			}
			
			
			
			filter(file, cmp, columnName, dataType, records);
			
			if(display) records.display(cols); 
			
			file.close();
			
			return records;
		}catch(Exception e){
			System.out.println(e);
			return null;
		}
	}

    public static void drop(String table) {
        try {
            // delete table record from davisbase_tables and davisbase_columns from
            // /data/catalog dir
            delete(Constants.COLUMN_CATALOG, new String[] { "table_name", "=", table }, Constants.catalogDir);
            delete(Constants.TABLE_CATALOG, new String[] { "table_name", "=", table }, Constants.catalogDir);

            File dropOldFile = new File(Constants.userDataDir, table + Constants.FILE_TYPE);
            dropOldFile.delete();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void delete(String deleteTable, String[] deleteCmp, String deleteDir) {
        try {
            ArrayList<Integer> deleteRowIds = new ArrayList<Integer>();

            if (!"rowid".equals(deleteCmp[0]) || deleteCmp.length == 0) {
                // get the rowids to be updated
                Records delRecords = select(deleteTable, new String[] { "*" }, deleteCmp, false);
                deleteRowIds.addAll(delRecords.content.keySet());
            } else
                // we already have a rowid, just add it to the list
                deleteRowIds.add(Integer.parseInt(deleteCmp[2]));

            for (int delRowId : deleteRowIds) {
                // open the file for table
                RandomAccessFile delFile = new RandomAccessFile(deleteDir + deleteTable + Constants.FILE_TYPE, "rw");
                int delNumPages = getPageCount(delFile);
                int delPage = 0;

                // find the page where data is located
                for (int currPage = 1; currPage <= delNumPages; currPage++)
                    if (BPlusTree.hasKey(delFile, currPage, delRowId)
                            && BPlusTree.getPageType(delFile, currPage) == Constants.recordsPage) {
                        delPage = currPage;
                        break;
                    }

                // if not found return error
                if (delPage == 0) {
                    System.out.println("Oops! Data not found in table.");
                    return;
                }

                // get all the cells on that page
                short[] delCells = BPlusTree.getCellArray(delFile, delPage);
                int k = 0;

                // iterate over all the cells
                int delCellNum = 0;
                while (delCellNum < delCells.length) {
                    // for(int cellNum = 0; cellNum < cells.length; cellNum++)
                    // {
                    // get location for current cell
                    long delCurrLoc = BPlusTree.getCellLoc(delFile, delPage, delCellNum);

                    // retrieve all the values
                    String[] delValues = retrieveValues(delFile, delCurrLoc);

                    // get the current row id
                    int delCurrRowId = Integer.parseInt(delValues[0]);

                    // if not current row id, move the cell
                    if (delCurrRowId != delRowId) {
                        BPlusTree.setCellOffset(delFile, delPage, k, delCells[delCellNum]);
                        k++;
                    }
                    delCellNum++;
                }

                // change cell number
                BPlusTree.setCellNumber(delFile, delPage, (byte) k);
            }

        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type,
            Records records) {
        try {
            int pageCount = getPageCount(file);
            for (int page = 1; page <= pageCount; page++) {
                file.seek((page - 1) * Constants.pageSize);
                byte pageCategory = file.readByte();

                if (pageCategory == Constants.recordsPage) {
                    byte cellCount = BPlusTree.getCellNumber(file, page);

                    // iterate over all the cells
                    for (int cell = 0; cell < cellCount; cell++) {
                        // fetch data in the current cell
                        long loc = BPlusTree.getCellLoc(file, page, cell);
                        String[] values = retrieveValues(file, loc);
                        int rowid = Integer.parseInt(values[0]);

                        // date handling
                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME"))
                                values[j] = "'" + values[j] + "'";

                        // check if the value satisfies the condition
                        boolean compareCheck = cmpCheck(values, rowid, cmp, columnName);

                        // date handling
                        for (int j = 0; j < type.length; j++)
                            if (type[j].equals("DATE") || type[j].equals("DATETIME"))
                                values[j] = values[j].substring(1, values[j].length() - 1);

                        // if condition satisfied, add to response
                        if (compareCheck)
                            records.add(rowid, values);

                    }
                } else
                    continue;
            }

            records.columnName = columnName;
            records.format = new int[columnName.length];

        } catch (Exception e) {
            System.out.println("Error at filter");
            e.printStackTrace();
        }

    }

    public static boolean cmpCheck(String[] values, int rowid, String[] cmp, String[] columnName) {
        boolean flag = false;

        // nothing to compare
        if (cmp.length == 0) {
            flag = true;
        } else {
            // get the column position
            int columnPosition = 1;
            for (int i = 0; i < columnName.length; i++) {
                if (columnName[i].equals(cmp[0])) {
                    columnPosition = i + 1;
                    break;
                }
            }

            if (columnPosition == 1) {
                // if comparision on rowid
                int value = Integer.parseInt(cmp[2]);
                String operator = cmp[1];

                // check different condition
                switch (operator) {
                    case Constants.EQUALS_SIGN:
                        return rowid == value;
                    case Constants.GREATER_THAN_SIGN:
                        return rowid > value;
                    case Constants.GREATER_THAN_EQUAL_SIGN:
                        return rowid >= value;
                    case Constants.LESS_THAN_SIGN:
                        return rowid < value;
                    case Constants.LESS_THAN_EQUAL_SIGN:
                        return rowid <= value;
                    case Constants.NOT_EQUAL_SIGN:
                        return rowid != value;
                }
            } else
                return cmp[2].equals(values[columnPosition - 1]);
        }

        return flag;
    }

    // Update function to validate the update in the database
    // table is called as parameter, along with compare, assign and directory
    public static void update(String row_col, String[] compare, String[] assign, String folder) {
        try {
            ArrayList<Integer> value_r = new ArrayList<Integer>();

            // get the rowids to be updated
            if (compare.length == 0 || !"rowid".equals(compare[0])) {

                Records save = select(row_col, new String[] { "*" }, compare, false);
                value_r.addAll(save.content.keySet());
            } else
                value_r.add(Integer.parseInt(compare[2]));

            for (int key : value_r) {
                RandomAccessFile sheet = new RandomAccessFile(folder + row_col + Constants.FILE_TYPE, "rw");
                int page_count = getPageCount(sheet);

                // iterate over all the pages to check which page contains our key
                int list = 0;
                for (int Page_cursor = 1; Page_cursor <= page_count; Page_cursor++) {
                    if (BPlusTree.hasKey(sheet, Page_cursor, key)
                            && BPlusTree.getPageType(sheet, Page_cursor) == Constants.recordsPage) {
                        list = Page_cursor;
                    }
                }

                // if not found return error
                if (list == 0) {
                    System.out.println("The given key value does not exist");
                    return;
                }

                // get all the keys on the current page
                int[] tokenn = BPlusTree.getKeyArray(sheet, list);
                int cell_id = 0;

                // search for our key
                for (int i = 0; i < tokenn.length; i++)
                    if (tokenn[i] == key)
                        cell_id = i;

                // get the location of our key
                int set_change = BPlusTree.getCellOffset(sheet, list, cell_id);
                long location = BPlusTree.getCellLoc(sheet, list, cell_id);

                // get all columns, saved values and data types for current key
                String[] Val_col = getColName(row_col);
                String[] retrieved_value = retrieveValues(sheet, location);
                String[] data_type = getDataType(row_col);

                // handle date data type
                for (int a = 0; a < data_type.length; a++)
                    if (data_type[a].equals("DATE") || data_type[a].equals("DATETIME"))
                        retrieved_value[a] = "'" + retrieved_value[a] + "'";

                // search for our column
                int search = 0;
                for (int a = 0; a < Val_col.length; a++)
                    if (Val_col[a].equals(assign[0])) {
                        search = a;
                        break;
                    }

                // update column value
                retrieved_value[search] = assign[2];

                // check for null constraint
                String[] const_null = getNullable(row_col);
                for (int i = 0; i < const_null.length; i++) {
                    if (retrieved_value[i].equals("null") && const_null[i].equals("NO")) {
                        System.out.println("NULL-value constraint violation");
                        return;
                    }
                }

                // update the value in file
                byte[] update = new byte[Val_col.length - 1];
                int count_pl = calPayloadSize(row_col, retrieved_value, update);
                BPlusTree.updateLeafCell(sheet, list, set_change, count_pl, key, update, retrieved_value);

                sheet.close();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static int calPayloadSize(String row_col, String[] assign, byte[] ct) {
        String[] data = getDataType(row_col);
        int space = data.length;
        for (int a = 1; a < data.length; a++) {
            ct[a - 1] = getTypeCode(assign[a], data[a]);
            space = space + fieldLength(ct[a - 1]);
        }
        return space;
    }

    public static byte getTypeCode(String value, String type) {
        if (value.equals("null")) {
            switch (type) {
                case "TINYINT":
                    return Constants.NULL;
                // case "SMALLINT": return Constants.SHORTNULL;
                // case "INT": return Constants.INTNULL;
                // case "BIGINT": return Constants.LONGNULL;
                // case "REAL": return Constants.INTNULL;
                // case "DOUBLE": return Constants.LONGNULL;
                // case "DATETIME": return Constants.LONGNULL;
                // case "DATE": return Constants.LONGNULL;
                // case "TEXT": return Constants.LONGNULL;
                default:
                    return Constants.NULL;
            }
        } else {
            switch (type) {
                case "TINYINT":
                    return Constants.TINYINT;
                // case "SMALLINT": return Constants.SHORTINT;
                case "INT":
                    return Constants.INT;
                case "BIGINT":
                    return Constants.LONG;
                case "REAL":
                    return Constants.FLOAT;
                case "DOUBLE":
                    return Constants.DOUBLE;
                case "DATETIME":
                    return Constants.DATETIME;
                case "DATE":
                    return Constants.DATE;
                case "TEXT":
                    return (byte) (value.length() + Constants.TEXT);
                default:
                    return Constants.NULL;
            }
        }
    }

    public static short fieldLength(byte code) {
        switch (code) {
            case Constants.NULL:
                return 1;
            // case Constants.SHORTNULL: return 2;
            // case Constants.INTNULL: return 4;
            // case Constants.LONGNULL: return 8;
            case Constants.TINYINT:
                return 1;
            // case Constants.SHORTINT: return 2;
            case Constants.INT:
                return 4;
            case Constants.LONG:
                return 8;
            case Constants.FLOAT:
                return 4;
            case Constants.DOUBLE:
                return 8;
            case Constants.DATETIME:
                return 8;
            case Constants.DATE:
                return 8;
            default:
                return (short) (code - Constants.TEXT);
        }
    }

    public static String[] getDataType(String table) {
        return getDavisbaseColumnsColumn(3, table);
    }

    public static String[] getColName(String table) {
        return getDavisbaseColumnsColumn(2, table);
    }

    public static String[] getNullable(String table) {
        return getDavisbaseColumnsColumn(5, table);
    }

    public static String[] getUnique(String table) {
        return getDavisbaseColumnsColumn(6, table);
    }

    public static String[] getDavisbaseColumnsColumn(int i, String table) {
        try {
            // fetch the data from davisbase_columns
            RandomAccessFile file = new RandomAccessFile(Constants.catalogDir + "davisbase_columns.tbl", "rw");
            Records records = new Records();
            String[] columnName = { "rowid", "table_name", "column_name", "data_type", "ordinal_position",
                    "is_nullable", "is_unique" };
            String[] cmp = { "table_name", "=", table };
            filter(file, cmp, columnName, new String[] {}, records);

            // save the result
            HashMap<Integer, String[]> content = records.content;

            // add all to the result array
            ArrayList<String> array = new ArrayList<String>();
            for (String[] x : content.values()) {
                array.add(x[i]);
            }

            return array.toArray(new String[array.size()]);

        } catch (Exception e) {
            System.out.println(e);
        }

        return new String[0];
    }

}
