import java.io.File;
import java.io.RandomAccessFile;
import java.util.Scanner;

/**
 *  @author Team Chicago
 *  @version 1.0
 *  <b>
 *  <p>We're just trying our best :)</p>
 *  </b>
 *
 */
public class DavisBase {

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	/** ***********************************************************************
	 *  Main method
	 */
    public static void main(String[] args) {
    	
		init();
		/* Display the welcome screen */
		Utils.splashScreen();

		/* Variable to hold user input from the prompt */
		String userCommand = ""; 

		while(!Settings.isExit()) {
			System.out.print(Settings.getPrompt());
			/* Strip newlines and carriage returns */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim();
			Commands.parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}

	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}

	public static void init(){
		try {
			File directory = new File("data");
			if(!directory.exists()){
                directory.mkdir();
				System.out.println("Initializing...");
				initialize();
			}
			else {
				directory = new File(Constants.catalogDir);
                int oldDefaultTable = 0;
				int oldDefaultColumn = 0;
				String[] previousData = directory.list();
				for (int i=0; i<previousData.length; i++) {
					if(previousData[i].equals(Constants.TABLE_CATALOG+Constants.FILE_TYPE))
                        oldDefaultTable = 1;
					if(previousData[i].equals(Constants.COLUMN_CATALOG+Constants.FILE_TYPE))
                        oldDefaultColumn = 1;
				}
				
				if(oldDefaultTable == 0 || oldDefaultColumn == 0)
					initialize();
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void initialize() 
	{

		try {
			File directory = new File(Constants.userDataDir);
			directory.mkdir();
			directory = new File(Constants.catalogDir);
			directory.mkdir();
			String[] previousData;
			previousData = directory.list();
			for (int i=0; i<previousData.length; i++) {
				File file = new File(directory, previousData[i]); 
				file.delete();
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile tableCatalog = new RandomAccessFile(Constants.catalogDir+"/davisbase_tables.tbl", "rw");
			tableCatalog.setLength(Constants.PAGE_SIZE);
			tableCatalog.seek(0);
			tableCatalog.write(Constants.recordsPage); //records page
			tableCatalog.writeByte(0x02); //intnull (size 4, can also use int here)
									
			//creating davisbase_tables
			tableCatalog.writeShort(Constants.COLUMN_OFFSET);
			tableCatalog.writeInt(0);
			tableCatalog.writeInt(0);
			tableCatalog.writeShort(Constants.TABLE_OFFSET);
			tableCatalog.writeShort(Constants.COLUMN_OFFSET);
			
			tableCatalog.seek(Constants.TABLE_OFFSET);
			tableCatalog.writeShort(20);
			// tableCatalog.writeShort(0);
			tableCatalog.writeInt(1); 
			tableCatalog.writeByte(1);
			tableCatalog.writeByte(28);
			// tableCatalog.writeByte(0);
			tableCatalog.writeBytes(Constants.TABLE_CATALOG);
			
			tableCatalog.seek(Constants.COLUMN_OFFSET);
			tableCatalog.writeShort(21);
			// tableCatalog.writeShort(0);
			tableCatalog.writeInt(2); 
			tableCatalog.writeByte(1);
			tableCatalog.writeByte(29);
			// tableCatalog.writeByte(0);
			tableCatalog.writeBytes(Constants.COLUMN_CATALOG);
			
			tableCatalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
		
		try {
			RandomAccessFile columnCatalog = new RandomAccessFile(Constants.catalogDir+"/davisbase_columns.tbl", "rw");
			columnCatalog.setLength(Constants.PAGE_SIZE);
			columnCatalog.seek(0);       
			columnCatalog.writeByte(Constants.recordsPage); 
			columnCatalog.writeByte(0x09); //no of records

			
			int[] offset=new int[9];
			offset[0]=Constants.PAGE_SIZE-45;
			offset[1]=offset[0]-49;
			offset[2]=offset[1]-46;
			offset[3]=offset[2]-50;
			offset[4]=offset[3]-51;
			offset[5]=offset[4]-49;
			offset[6]=offset[5]-59;
			offset[7]=offset[6]-51;
			offset[8]=offset[7]-49;
			
			columnCatalog.writeShort(offset[8]); 
			columnCatalog.writeInt(0); 
			columnCatalog.writeInt(0); 
			
			for(int i=0;i<offset.length;i++)
				columnCatalog.writeShort(offset[i]);

			
			//creating davisbase_columns
			columnCatalog.seek(offset[0]);
			columnCatalog.writeShort(36);
			columnCatalog.writeInt(1); //key
			columnCatalog.writeByte(6); //no of columns
			columnCatalog.writeByte(28); //16+12next file lines indicate the code for datatype/length of the 5 columns
			columnCatalog.writeByte(17); //5+12
			columnCatalog.writeByte(15); //3+12
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.TABLE_CATALOG); 
			columnCatalog.writeBytes(Constants.HEADER_ROWID); 
			columnCatalog.writeBytes("INT"); 
			columnCatalog.writeByte(1); 
			columnCatalog.writeBytes(Constants.FALSE); 
			columnCatalog.writeBytes(Constants.FALSE); 
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[1]);
			columnCatalog.writeShort(42); 
			columnCatalog.writeInt(2); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(28);
			columnCatalog.writeByte(22);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.TABLE_CATALOG); 
			columnCatalog.writeBytes(Constants.HEADER_TABLE_NAME); 
			columnCatalog.writeBytes(Constants.HEADER_TEXT); 
			columnCatalog.writeByte(2);
			columnCatalog.writeBytes(Constants.FALSE); 
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[2]);
			columnCatalog.writeShort(37); 
			columnCatalog.writeInt(3); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(17);
			columnCatalog.writeByte(15);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes(Constants.HEADER_ROWID);
			columnCatalog.writeBytes("INT");
			columnCatalog.writeByte(1);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[3]);
			columnCatalog.writeShort(43);
			columnCatalog.writeInt(4); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(22);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes(Constants.HEADER_TABLE_NAME);
			columnCatalog.writeBytes(Constants.HEADER_TEXT);
			columnCatalog.writeByte(2);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[4]);
			columnCatalog.writeShort(44);
			columnCatalog.writeInt(5); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(23);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes("column_name");
			columnCatalog.writeBytes(Constants.HEADER_TEXT);
			columnCatalog.writeByte(3);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[5]);
			columnCatalog.writeShort(42);
			columnCatalog.writeInt(6); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(21);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes("data_type");
			columnCatalog.writeBytes(Constants.HEADER_TEXT);
			columnCatalog.writeByte(4);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[6]);
			columnCatalog.writeShort(52); 
			columnCatalog.writeInt(7); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(28);
			columnCatalog.writeByte(19);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes("ordinal_position");
			columnCatalog.writeBytes("TINYINT");
			columnCatalog.writeByte(5);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.seek(offset[7]);
			columnCatalog.writeShort(44); 
			columnCatalog.writeInt(8); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(23);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes(Constants.HEADER_IS_NULLABLE);
			columnCatalog.writeBytes(Constants.HEADER_TEXT);
			columnCatalog.writeByte(6);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
		

			columnCatalog.seek(offset[8]);
			columnCatalog.writeShort(42); 
			columnCatalog.writeInt(9); 
			columnCatalog.writeByte(6);
			columnCatalog.writeByte(29);
			columnCatalog.writeByte(21);
			columnCatalog.writeByte(16);
			columnCatalog.writeByte(4);
			columnCatalog.writeByte(14);
			columnCatalog.writeByte(14);
			columnCatalog.writeBytes(Constants.COLUMN_CATALOG);
			columnCatalog.writeBytes(Constants.HEADER_IS_UNIQUE);
			columnCatalog.writeBytes(Constants.HEADER_TEXT);
			columnCatalog.writeByte(7);
			columnCatalog.writeBytes(Constants.FALSE);
			columnCatalog.writeBytes(Constants.FALSE);
			
			columnCatalog.close();
			
			String[] new_row = {"10", Constants.TABLE_CATALOG,"cur_row_id","INT","3",Constants.FALSE,Constants.FALSE};		
			Table.insertInto(Constants.COLUMN_CATALOG,new_row,Constants.catalogDir);			//add current row_id column to davisbase_columns
		}
		catch (Exception e) 
		{
			System.out.println(e);
		}
	}
}