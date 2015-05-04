import org.pentaho.di.core.database.*;
import org.pentaho.di.core.variables.*;
import org.pentaho.di.trans.*;
import java.sql.*;
import java.util.*;
import java.io.*;
 
private Database db = null;
private Queue sqlScripts = new LinkedList();
private Queue sqlFiles = new LinkedList();
private int readCount = 0;
private int failCount = 0;
private ResultSet resultSet = null;
private int maxRows = 0;
private int fetchSize = 0;

public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
	Object[] row = null;
  if (first) {
    // Get the list of files to read for sql statements
    FieldHelper fileField = get(Fields.Info, "filename");
    RowSet infoStream = findInfoRowSet("sqlFiles");

    // Loop through each row coming from the info step
    Object[] infoRow = null;
    while((infoRow = getRowFrom(infoStream)) != null) {
      String sqlFile = fileField.getString(infoRow);
      // Open the file and read the SQL statement			
      try {
        File f = new File(sqlFile);
        String sql = new Scanner(f).useDelimiter("\\Z").next(); // Read a single "token"
        // Store the statement and filename
        sqlFiles.add(sqlFile);
        sqlScripts.add(sql);
        logDetailed("Read SQL from " + sqlFile);
        logDebug(sql);
      } catch (IOException e) {
              logError("Failed to open input file " + sqlFile + " [" + e.getMessage() + "]");
      }
    }
    first = false; // Done with initialization
  }
  
  RowMetaInterface rowMeta = null;
  DatabaseMeta dbMeta = null;

	// If we don't have a resultset, try to get one
	while (resultSet == null && sqlScripts.size() > 0) {
		// Execute the next script
		try {
			logDetailed("Executing SQL from " + sqlFiles.element());
			String sql = (String) sqlScripts.remove(); // Pop from queue
			resultSet = db.openQuery(sql); // Execute SQL
      
      dbMeta = db.getDatabaseMeta();
			rowMeta = db.getQueryFields(sql, false); // Database row metadata
			
			logDetailed("Fetched " + rowMeta.size() + " fields");
			
		} catch (KettleDatabaseException e){
      logError("Error executing script in " + sqlFiles.element() + " " + e.getMessage());
      setErrors(1);
		} catch (SQLException ex){
      logError("Error fetching resultset meta data for " + sqlFiles.element() + " " + ex.getMessage());
      setErrors(1);
		}
		sqlFiles.remove(0); // Remove filename from result
	}

	// If we still don't have a resultset, we're done...
	if (resultSet == null) {
		logDetailed("No more files to process");
		setOutputDone();
		return false;
	}

	// At this point, we have a valid db connection and a valid resultset
	try {
		// Fetch the next row
		for (row = db.getRow(resultSet); (row != null) && ((readCount < maxRows) || maxRows == 0); row = db.getRow(resultSet)) {
			readCount++;
			// Now we have a row, so do something with it...
			
      // Allocate an output row array
      Object[] outputRow = RowDataUtil.allocateRowData(rowMeta.size());
			
      // Set output values
      for (int f = 0; f < rowMeta.size(); f++) {
        ValueMetaInterface fromMeta = rowMeta.getValueMeta(f);
        ValueMetaInterface toMeta = data.outputRowMeta.getValueMeta(f);
        Object val = row[f];

        // Convert to output type
        if (val != null) {
          if (val instanceof String) // Trim any string values before converting
            val = ((String)val).trim();
          outputRow[f] = toMeta.convertData(fromMeta, val);
        }
        else
          outputRow[f] = null;
      }

      // Write the row to the log
      logRowlevel("Read row: " + data.outputRowMeta.getString(outputRow));

      // Pass the row to the next step
      putRow(data.outputRowMeta, outputRow);
		}

		// All done with this resultset
		logDetailed("Finished with result set");
		db.closeQuery(resultSet);
		resultSet = null;
	} catch (KettleDatabaseException e){
    logError("Error fetching next row " + e.getMessage());
    setErrors(1);
		failCount++;
		// Give up on this resultset
		db.closeQuery(resultSet);
		resultSet = null;
	}

	if (readCount >= maxRows && maxRows > 0) {
		logDetailed("Maximum rows reached [" + maxRows + "]");
		setOutputDone();
		return false;
	}

  return true;
}
 
public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
{
    if (parent.initImpl(stepMetaInterface, stepDataInterface)){
		String maxRowsParam = getParameter("maxRows");
		if (maxRowsParam != null && maxRowsParam.trim().length() > 0) {
			maxRows = Integer.parseInt(maxRowsParam);
			logDetailed("Max rows = " + maxRows);
		}
		String fetchSizeParam = getParameter("fetchSize");
		if (fetchSizeParam != null && fetchSizeParam.trim().length() > 0) {
			fetchSize = Integer.parseInt(fetchSizeParam);
			logDetailed("Fetch size = " + fetchSize);
		}
	    String connectionName = getParameter("connectionName");
        try{
		 	// Connect to the source database
            db = new Database(this.parent, getTransMeta().findDatabase(connectionName));
            db.shareVariablesWith(this.parent);
            db.connect();
			logDetailed("Connected to database [" + connectionName + "]");
            return true;
        }
        catch(KettleDatabaseException e){
            logError("Error connecting to " + connectionName + " " + e.getMessage());
            setErrors(1);
            stopAll();
        }
    }
    return false;
     
}
 
public void dispose(StepMetaInterface smi, StepDataInterface sdi)
{
    if (db != null) {
        db.disconnect();
    }
    parent.disposeImpl(smi, sdi);
}