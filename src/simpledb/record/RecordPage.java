package simpledb.record;

import static java.sql.Types.INTEGER;
import static simpledb.file.Page.*;

import javax.swing.text.MaskFormatter;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.tx.Transaction;

/**
 * Manages the placement and access of records in a block.
 * @author Edward Sciore
 */
public class RecordPage {
   public static final int EMPTY = 0, INUSE = 1;

   
   private Block blk;
   private TableInfo ti;
   private Transaction tx;
   private int slotsize;
   private int currentslot = 4;
   
   /** Creates the record manager for the specified block.
     * The current record is set to be prior to the first one.
     * @param blk a reference to the disk block
     * @param ti the table's metadata
     * @param tx the transaction performing the operations
     */
   public RecordPage(Block blk, TableInfo ti, Transaction tx) {
      this.blk = blk;
      this.ti = ti;
      this.tx = tx;
      slotsize = ti.recordLength() + INT_SIZE;
      tx.pin(blk);
  }
   
   /**
    * Closes the manager, by unpinning the block.
    */
   public void close() {
      if (blk != null) {
    	  tx.unpin(blk);
    	  blk = null;
      }
   }
   
   /**
    * Moves to the next record in the block.
    * @return false if there is no next record.
    */
   public boolean next() {
      return searchFor(INUSE);
   }
   
   /**
    * Returns the integer value stored for the
    * specified field of the current record.
    * @param fldname the name of the field.
    * @return the integer stored in that field
    */
   public int getInt(String fldname) {
      int position = fieldpos(fldname);
      return tx.getInt(blk, position);
   }
   
   /**
    * Returns the string value stored for the
    * specified field of the current record.
    * @param fldname the name of the field.
    * @return the string stored in that field
    */
   public String getString(String fldname) {
      int position = fieldpos(fldname);
      return tx.getString(blk, position);
   }
   
   /**
    * Stores an integer at the specified field
    * of the current record.
    * @param fldname the name of the field
    * @param val the integer value stored in that field
    */
   public void setInt(String fldname, int val) {
      int position = fieldpos(fldname);
      tx.setInt(blk, position, val);
      if(tx.getInt(blk,currentpos()) == -1){
    	  tx.setInt(blk, currentpos(), currentpos());
      }
	   int aux3 = currentpos()+INT_SIZE;
	   int nr = tx.getInt(blk, aux3);
	   tx.setInt(blk, nr, -1);
   }
   
   /**
    * Stores a string at the specified field
    * of the current record.
    * @param fldname the name of the field
    * @param val the string value stored in that field
    */
   public void setString(String fldname, String val) {
	   int place = ti.offset(fldname);
	   if(place != (ti.recordLength()-1)){
		   int[] types = getTypes();
		   int intcont = 0;
		   int strcont = 0;
		   for(int k = place + 1; k < types.length; k++){
			   if(types[k]==0){
				   intcont++;
			   }else{
				   strcont++;
			   }
		   }
		   int[] ints = new int[intcont];
		   String[] strs = new String[strcont];
		   intcont = 0;
		   strcont = 0;
		   int startofieldsplaces = currentpos() + 2*INT_SIZE;
		   //Busco guardar los datos de mas adelante 
		   for(int i = place + 1; i < ti.recordLength(); i++ )
		   {
			   if(types[i] == 0){
				   ints[intcont] = tx.getInt(blk,tx.getInt(blk, startofieldsplaces + i*INT_SIZE)); 
				   intcont++;
			   }else{
				   strs[strcont] = tx.getString(blk, tx.getInt(blk,startofieldsplaces + i*INT_SIZE)); 
				   strcont++;
			   }
		   }
		   
		   int position = fieldpos(fldname);
		   tx.setString(blk, position, val);
		   int desplazamiento = STR_SIZE(val.length());
		   intcont = 0;
		   strcont = 0;
		   for(int i = place + 1; i < ti.recordLength(); i++ )
		   {
			   if(types[i] == 0){
				   int aux1 = tx.getInt(blk, startofieldsplaces + i*INT_SIZE);
				   tx.setInt(blk, aux1, tx.getInt(blk, startofieldsplaces + i*INT_SIZE)+desplazamiento);
				   tx.setInt(blk, tx.getInt(blk, startofieldsplaces + i*INT_SIZE), ints[intcont]);
				   intcont++;
			   }else{
				   int aux1 = tx.getInt(blk, startofieldsplaces + i*INT_SIZE);
				   tx.setInt(blk, aux1, tx.getInt(blk, startofieldsplaces + i*INT_SIZE)+desplazamiento);
				   int auxaux = tx.getInt(blk, startofieldsplaces + i*INT_SIZE);
				   
				   tx.setString(blk, auxaux, strs[strcont]);
				   strcont++;
			   }
		   }
	   }else{
		      int position = fieldpos(fldname);
		      tx.setString(blk, position, val);
	   }
	   
	   if(tx.getInt(blk,currentpos()) == -1){
		   tx.setInt(blk, currentpos(), currentpos());
	   }
	   int aux3 = currentpos()+INT_SIZE;
	   int nr = tx.getInt(blk, aux3);
	   tx.setInt(blk, nr, -1);
	   //tx.setInt(blk, currentpos()+INT_SIZE, val);
	   //int end =  endofcurrentrecord();
	   //tx.setInt(blk, currentpos()+INT_SIZE, val);
   }
   
   
   /**
    * Deletes the current record.
    * Deletion is performed by just marking the record
    * as "deleted"; the current record does not change. 
    * To get to the next record, call next().
    */
   public void delete() {
      int position = currentpos();
      tx.setInt(blk, position, EMPTY);
   }
   
   /**
    * Inserts a new, blank record somewhere in the page.
    * Return false if there were no available slots.
    * @return false if the insertion was not possible
    */
   public boolean insert() {
      //currentslot = -1;
      int fr = tx.getInt(blk, 0);
      tx.setInt(blk, INT_SIZE, fr);
      int nextrecord = tx.getInt(blk, currentpos());
      if(nextrecord == -1){
    	  return true;
      }
      boolean found = searchFor(EMPTY);
      if (found) {
         int position = currentpos();
         tx.setInt(blk, INT_SIZE, tx.getInt(blk, position + INT_SIZE));
    	  
      }
      return found;
   }
   
   /**
    * Sets the current record to be the record having the
    * specified ID.
    * @param id the ID of the record within the page.
    */
   public void moveToId(int id) {
      currentslot = id;
   }
   
   /**
    * Returns the ID of the current record.
    * @return the ID of the current record
    */
   public int currentId() {
      return currentslot;
   }
   
   private int currentpos() {
	   return tx.getInt(blk, INT_SIZE);
      //return currentslot * slotsize;
   }
   
   private int fieldpos(String fldname) {
	   int offset = ti.offset(fldname) * INT_SIZE;
	   int recordmetadatasize = 2*INT_SIZE;
	   int pos_apreguntar = currentpos() + recordmetadatasize + offset;
	   return tx.getInt(blk, pos_apreguntar);
	   //int offset = INT_SIZE + ti.offset(fldname);
	   //return currentpos() + offset;
   }
   
   private boolean isValidSlot() {
      return tx.getInt(blk,currentpos() + INT_SIZE) <= 330;
   }
   
   private boolean searchFor(int flag) {
	   if(flag == 0){
		   int startnext = tx.getInt(blk, currentpos()+INT_SIZE);
		   int stateofnext = tx.getInt(blk, startnext);
		   while (isValidSlot()) {
			   if (stateofnext == -1){
				   makeDefaultRecord(blk, startnext);
				   return true;
			   }else{
				   tx.setInt(blk, INT_SIZE, startnext);
				   startnext = tx.getInt(blk, currentpos()+INT_SIZE);
				   stateofnext = tx.getInt(blk, startnext);
				   }
			   }
		   return false;
	   }else{
		   while(isValidSlot())
		   {
			   int hulu = tx.getInt(blk, currentpos()); 
			   if(hulu != -1){
				   int buggg = tx.getInt(blk, currentpos()+INT_SIZE);
				   tx.setInt(blk, INT_SIZE, buggg);
				   return true;
			   }else{
				   return false;
			   }
		   }
		   return false;
	   }
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
 
   private void makeDefaultRecord(Block blk, int pos) {
	   //recordlength ahora nos dira field count
	   int recordmetadatasize = 2*INT_SIZE;
	   int recordheadersize = ti.recordLength()* INT_SIZE + recordmetadatasize;
	   //int recsize = recordheadersize + "empty record fields size";
	   int[] types = getTypes();
	   //Segundo for para llenar el record con 0 y ""
	   int desplazamiento = 0;
	   for(int j = 0; j < types.length; j++)
	   {
		   tx.setInt(blk, pos + recordmetadatasize + j*INT_SIZE, pos + recordheadersize + desplazamiento);
		   if(types[j] == 0){
			   tx.setInt(blk,pos + recordheadersize + desplazamiento, 0);
			   desplazamiento += INT_SIZE;
		   }else{
			   tx.setString(blk, pos + recordheadersize + desplazamiento, "");
			   desplazamiento += STR_SIZE("".length());
		   }
	   }   
	 
	   //Metadata del record
	   if(pos == INT_SIZE*4){
		   tx.setInt(blk,pos, -1);
	   }else{
		   tx.setInt(blk,pos, tx.getInt(blk, INT_SIZE));
	   }
	   tx.setInt(blk,pos+INT_SIZE, pos + recordheadersize + desplazamiento);
	   tx.setInt(blk,tx.getInt(blk,pos+INT_SIZE), -1);
	   tx.setInt(blk, INT_SIZE*2, pos);

	   
		//	  for (String fldname : ti.schema().fields()) {
		//         int offset = ti.offset(fldname);
		//         if (ti.schema().type(fldname) == INTEGER)
		//            page.setInt(pos + INT_SIZE + offset, 0);
		//         else
		//            page.setString(pos + INT_SIZE + offset, "");
		//      }
   }
   
   public int getFreeSpace(){
	   int end = endofcurrentrecord();
	   return 400 - end;
   }
   
   private int endofcurrentrecord(){
	   int lr = tx.getInt(blk, INT_SIZE*2);
	   int lastfield = (ti.recordLength() + 1)*INT_SIZE;
	   int[] types = getTypes();
	   int end = lr;
	   if(types[ti.recordLength()-1] == 0){
		   end = tx.getInt(blk, lr+lastfield) + INT_SIZE;
	   }else{
		   String laststring = tx.getString(blk, tx.getInt(blk, lr + lastfield));
		   end = tx.getInt(blk, lr+lastfield) + STR_SIZE(laststring.length());
	   }
	   return end;
   }
   
}
