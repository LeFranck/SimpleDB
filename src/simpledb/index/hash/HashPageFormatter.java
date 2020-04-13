package simpledb.index.hash;

import static simpledb.file.Page.*;
import static simpledb.record.RecordPage.EMPTY;
import static simpledb.record.RecordPage.INUSE;
import static java.sql.Types.INTEGER;
import simpledb.file.Page;
import simpledb.buffer.PageFormatter;
import simpledb.record.TableInfo;

/**
 * An object that can format a page to look like an
 * empty B-tree block.
 * @author Edward Sciore
 */
public class HashPageFormatter implements PageFormatter {
   private TableInfo ti;
   private int pisosGlobal;
   private int bucketsUsados;
   
   /**
    * Creates a formatter for a new page of the
    * specified B-tree index.
    * @param ti the index's metadata
    * @param firstdir_inpage the page's initial firstdir_inpage value
    */
   public HashPageFormatter(TableInfo ti, int pisosGlobal, int bucketsUsados) {
      this.ti = ti;
      this.pisosGlobal = pisosGlobal;
      this.bucketsUsados = bucketsUsados;
   }
   
   /** 
    * Formats the page by initializing as many index-record slots
    * as possible to have default values.
    * Each integer field is given a value of 0, and
    * each string field is given a value of "".
    * The location that indicates the number of records
    * in the page is also set to 0.
    * @see simpledb.buffer.PageFormatter#format(simpledb.file.Page)
    */
   //HEADER |pisos global | buckets usados|
   public void format(Page page) {
      //page.setInt(0, pisosGlobal);
      //page.setInt(INT_SIZE, bucketsUsados);  // #records = 0
      int recsize = ti.recordLength() + INT_SIZE;
      //Primer record se usara como header
      //ATENTO CON EL ORDEN
      for (int pos=0; pos+recsize<=BLOCK_SIZE; pos += recsize)
      {
          page.setInt(pos, EMPTY);
          makeDefaultRecord(page, pos);
      }
      
      page.setInt(0, INUSE);
      page.setString(ti.offset("identificador") + INT_SIZE, "D");
      page.setInt(ti.offset("numbucket") + INT_SIZE, pisosGlobal);
      page.setInt(ti.offset("profundidad") + INT_SIZE, bucketsUsados);

   }
   
   private void makeDefaultRecord(Page page, int pos) {
      for (String fldname : ti.schema().fields()) {
         int offset = ti.offset(fldname);
         if (ti.schema().type(fldname) == INTEGER)
            page.setInt(pos + INT_SIZE + offset, 0);
         else
            page.setString(pos + INT_SIZE + offset, "");
      }
   }
   
   
}
