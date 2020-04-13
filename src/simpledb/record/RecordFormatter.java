package simpledb.record;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;
import static simpledb.record.RecordPage.EMPTY;
import simpledb.file.Page;
import simpledb.buffer.PageFormatter;

/**
 * An object that can format a page to look like a block of 
 * empty records.
 * @author Edward Sciore
 */
class RecordFormatter implements PageFormatter {
	public static final int HEADERSIZE = INT_SIZE*4;
	private TableInfo ti;
   
   /**
    * Creates a formatter for a new page of a table.
    * @param ti the table's metadata
    */
   public RecordFormatter(TableInfo ti) {
      this.ti = ti;
   }
   
   /** 
    * Formats the page by allocating as many record slots
    * as possible, given the record length.
    * Each record slot is assigned a flag of EMPTY.
    * Each integer field is given a value of 0, and
    * each string field is given a value of "".
    * @see simpledb.buffer.PageFormatter#format(simpledb.file.Page)
    */
   public void format(Page page) {
	      //int recsize = ti.recordLength() + INT_SIZE;
	   //setting header
	   page.setInt(0, HEADERSIZE);
	   page.setInt(INT_SIZE, HEADERSIZE);
	   page.setInt(2*INT_SIZE, HEADERSIZE);
	   page.setInt(3*INT_SIZE, 0);
	   makeDefaultRecord(page, HEADERSIZE);
      //for (int pos=0; pos+recsize<=BLOCK_SIZE; pos += recsize) {
      //   page.setInt(pos, EMPTY);
      //   makeDefaultRecord(page, pos);
      //}
	   //Posible solucion bug
	   //llenar page con ""
	   int auxsize = STR_SIZE("".length());
	   for(int i = 100; i < 390; i = i + auxsize)
	   {
		   page.setString(i, "");
	   }
   }
   
   private void makeDefaultRecord(Page page, int pos) {
	   //recordlength ahora nos dira field count
	   int recordmetadatasize = 2*INT_SIZE;
	   int recordheadersize = ti.recordLength()* INT_SIZE + recordmetadatasize;
	   //int recsize = recordheadersize + "empty record fields size";
	   int[] types = getTypes();
	   //Segundo for para llenar el record con 0 y ""
	   int desplazamiento = 0;
	   for(int j = 0; j < types.length; j++)
	   {
		   page.setInt(pos + recordmetadatasize + j*INT_SIZE, pos + recordheadersize + desplazamiento);
		   if(types[j] == 0){
			   page.setInt(pos + recordheadersize + desplazamiento, 0);
			   desplazamiento += INT_SIZE;
		   }else{
			   page.setString(pos + recordheadersize + desplazamiento, "");
			   desplazamiento += STR_SIZE("".length());
		   }
	   }   
	 
	   //Metadata del record
	   if(pos == HEADERSIZE){
		   page.setInt(pos, -1);
	   }else{
		   page.setInt(pos, page.getInt(INT_SIZE));
	   }
	   page.setInt(pos+INT_SIZE, pos + recordheadersize + desplazamiento);
	   page.setInt(page.getInt(pos+INT_SIZE), -1);

	   
		//	  for (String fldname : ti.schema().fields()) {
		//         int offset = ti.offset(fldname);
		//         if (ti.schema().type(fldname) == INTEGER)
		//            page.setInt(pos + INT_SIZE + offset, 0);
		//         else
		//            page.setString(pos + INT_SIZE + offset, "");
		//      }
   }

   private int[] getTypes(){
	   int[] types = new int[ti.recordLength()];
	   //Primer for para ver como se deben estructurar los records
	   for (String fldname : ti.schema().fields()) 
	   {
	         int offset = ti.offset(fldname);
	         int type = ti.schema().type(fldname);
	         if(type == INTEGER)
	         {
	        	 types[offset] = 0;
	         }else{
	        	 types[offset] = 1;
	         }
	   }
	   return types;
   }

}
