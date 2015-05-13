import org.pentaho.di.core.database.*;
import org.pentaho.di.core.variables.*;
import org.pentaho.di.trans.*;
import java.sql.*;
import java.io.*;
import java.util.*;
import oracle.jdbc.driver.*;

private final int MAX_PARAMS = 20;
private int[] paramIndexes = null;
private int[] outputIndexes = null;
private RowMetaInterface infoMeta = null;
private Database db = null;
private int maxRows = 0;
private int rowCount = 0;
private int fetchSize = 0;
private String procName = null;
private List procParams = null; 
private int cursorParamIndex = 0;
private RowSet infoStream = null;
private boolean stopFlag = false;

// Overrides
public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
{
  Object[] infoRow = null;

  if (first) {
    if (procParams.size() > 0) {
      // Locate parameter value source
      infoStream = findInfoRowSet("PARAMETERS");
      
      if (infoStream == null) {
        // Can't continue without parameter values
        logError("Unable to find PARAMETERS InfoStream");
        setErrors(1);
        setOutputDone();
        return false;
      }
    }

    // Done with initialization
    first = false;
  }
  else {
    // We only run once for procedures with no parameters
    if (procParams.size() == 0) {
      setOutputDone();
      return false;    
    }
  }
   
  // If this procedure requires parameters, fetch values for them
  if (procParams.size() > 0) {
    // Get next set of parameter values (from InfoStep)
    logDebug("Fetching " + procParams.size() + " parameter values from PARAMETERS");
    infoRow = getRowFrom(infoStream);
    // Quit if none are available
    if (infoRow == null) {
      logDetailed("No more parameter values available from PARAMETERS");
      setOutputDone();
      return false;
    }
    
    if (infoMeta == null) {
      // Fetch parameter stream meta information
      infoMeta = infoStream.getRowMeta();
      if (infoMeta == null) {
        setErrors(1);
        logError("Unable to retrieve meta information from PARAMETERS");
        setOutputDone();
        return false;
      }
      
      // For each procedure parameter, identify the index in the info stream
      paramIndexes = new int[procParams.size()];
      logDebug("Mapping " + procParams.size() + " parameters");
      for (int p = 0; p < paramIndexes.length; p++) {
        // Use each parameter's name to look up its index in the info stream
        String paramName = (String) procParams.get(p);
        logDebug("    Mapping " + paramName);
        paramIndexes[p] = infoMeta.indexOfValue(paramName);
      }
    }
  }
  else {
    infoRow = new Object[0]; // Empty array for procedures with no parameters
  }

  CallableStatement stmt=null;
  ResultSet rs=null;
  rowCount = 0;

  try
  {
    // Create a callable statement
    stmt = createStatement();
  
    // Execute the statement and fetch the result set
    rs = getResultSet(stmt, infoRow);
    rs.setFetchSize(fetchSize);
    rs.setFetchDirection(ResultSet.FETCH_FORWARD);
    logDetailed("Query completed. Fetching results...");

    RowMetaInterface rowMeta = null;
    DatabaseMeta dbMeta = null;
    
    // Fetch the first row from the result set
    while (rs.next() && ((rowCount < maxRows) || maxRows == 0) && (stopFlag == false)) {
     
      // Fetch the database row meta information from the first row
      if (rowMeta == null) {
        logDebug("Retrieving row meta information");
        dbMeta = db.getDatabaseMeta();
        if (dbMeta == null)
          logError("Unable to fetch database meta");
        rowMeta = db.getMetaFromRow(null, rs.getMetaData()); // getMetaFromRow does not use the provided row (as of PDI 5.1)
        if (rowMeta == null)
          logError("Unable to fetch database row meta");

        // For each result column, identify the index in the output stream
        outputIndexes = new int[rowMeta.size()];
        logDebug("Mapping " + rowMeta.size() + " columns");
        String[] columns = rowMeta.getFieldNames();
        for (int c = 0; c < outputIndexes.length; c++) {
          // Use each column's name to look up its index in the output stream
          logDebug("    Mapping " + columns[c]);
          outputIndexes[c] = data.outputRowMeta.indexOfValue(columns[c]);
        }
      }
      
      // Allocate an output row array
      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      
      // For each result field, map and convert to an output field (or ignore it)
      for (int f = 0; f < rowMeta.size(); f++) {
        int outIndex = outputIndexes[f];
        if (outIndex >= 0) { // Only set mapped field values
          // Get field value
          ValueMetaInterface fromMeta = rowMeta.getValueMeta(f);
          ValueMetaInterface toMeta = data.outputRowMeta.getValueMeta(f);
          if (toMeta == null) {
            logError("Unable to fetch output value meta for column " + f);
          }
          Object val = dbMeta.getValueFromResultSet(rs, fromMeta, f);

          // Convert to output type and set output value
          if (val != null) {
            if (val instanceof String) { // Trim any string values before converting
              val = ((String)val).trim();
            }
            outputRow[outIndex] = toMeta.convertData(fromMeta, val);
          }
          else
            outputRow[outIndex] = null;
        }
      }

      // Write the row to the log
      logRowlevel("Read row: " + data.outputRowMeta.getString(outputRow));

      // Pass the row to the next step
      putRow(data.outputRowMeta, outputRow);
      rowCount++;
    }
  } catch(Exception e) {
    throw new KettleException("Unable to execute stored procedure", e);
  } finally {
    if (rs != null) db.closeQuery(rs);
    if (stmt != null) db.closePreparedStatement(stmt);
  }

  // Signal that we are ready for more
	return true;
}

public boolean init(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
{
  if (parent.initImpl(stepMetaInterface, stepDataInterface)) 
  {
    // Retrieve the name of the procedure to execute
    procName = getParameter("procName");
    if (procName != null && procName.trim().length() > 0) {
      
      // Check for maxRows parameter - limits number of rows returned
      String maxRowsParam = getParameter("maxRows");
      if (maxRowsParam != null && maxRowsParam.trim().length() > 0) {
        maxRows = Integer.parseInt(maxRowsParam);
        logDetailed("Max rows = " + maxRows);
      }
      
      // Check for fetchSize parameter - sets number of rows to fetch on each round-trip
      String fetchSizeParam = getParameter("fetchSize");
      if (fetchSizeParam != null && fetchSizeParam.trim().length() > 0) {
        fetchSize = Integer.parseInt(fetchSizeParam);
        logDetailed("Fetch size = " + fetchSize);
      }

      // Connect to the source database
      String connectionName = getParameter("connectionName");
      try{
        db = new Database(this.parent, getTransMeta().findDatabase(connectionName));
        db.shareVariablesWith(this.parent);
        db.connect();
        logDetailed("Connected to database [" + connectionName + "]");

        // Fetch parameter information for procedure
        logDetailed("Fetching Parameter Information for " + procName);
        procParams = getProcParams(procName);
        if (procParams != null)

        return true;
      }
      catch(KettleDatabaseException e){
          logError("Error connecting to " + connectionName + " - " + e.getMessage());
      }
    }
  }

  // Something went wrong...
  setErrors(1);
  stopAll();
  return false;
}

public void stopRunning(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)  throws KettleException {
  stopFlag = true;
  parent.stopRunningImpl(stepMetaInterface, stepDataInterface);
}

public void dispose(StepMetaInterface smi, StepDataInterface sdi)
{
    if (db != null) 
      db.disconnect();
    parent.disposeImpl(smi, sdi);
}

// Private members

private CallableStatement createStatement() throws Exception {

  // Assemble the query string
  String query = "{call " + procName + "(";
  for (int p = 0; p < procParams.size(); p++)
    query += "?,";    
  query += "?)}"; // Output parameter
 
	logDetailed("Query: '" + query + "'");

	// Create a callable statement.
	CallableStatement stmt = db.getConnection().prepareCall(query);

	return stmt;
}

private ResultSet getResultSet(CallableStatement stmt, Object[] r) throws Exception {

  // Set the output type to an Oracle Cursor
  stmt.registerOutParameter(cursorParamIndex + 1, OracleTypes.CURSOR);

  String paramValues = "";
	// Set the parameters...
	for (int p = 0; p < procParams.size(); p++) {
    int index = paramIndexes[p];
		ValueMetaInterface valueMeta = infoMeta.getValueMeta(index);
    Object valueData = r[index];
    db.setValue(stmt, valueMeta, valueData, p + 1);
    paramValues += valueData + "|";
	}

  if (fetchSize > 0) {
    logDetailed("Set fetch size : " + fetchSize);
    stmt.setFetchSize(fetchSize);
  }
  
 	// Execute the stored procedure...
	logDetailed("Parameter Values: " + paramValues);
	logDetailed("Executing : " + procName);
	stmt.execute();

	// Get the results...
  ResultSet rs = (ResultSet)stmt.getObject(cursorParamIndex + 1);

	return rs;
}

private List getProcParams(String procName)
{
  String[] nameParts = procName.split("\\.");
  if (nameParts.length > 2) {
    logError("Invalid procedure name: " + procName);
    return null;
  }
  procName = nameParts[nameParts.length - 1];

  // Fetch parameter information from the database
  ResultSet paramSet = null;
  String sql = "SELECT ARGUMENT_NAME, POSITION, DATA_TYPE, IN_OUT FROM USER_ARGUMENTS WHERE OBJECT_NAME = '" + procName + "' ";
  if (nameParts.length == 2)
    sql += " AND PACKAGE_NAME = '" + nameParts[0] + "' ";
  sql += " ORDER BY POSITION ASC";
  logDebug("Executing: " + sql);
  try {
    paramSet = db.openQuery(sql); // Execute SQL
  } catch(KettleDatabaseException e){
    logError("Error retrieving procedure parameters for " + procName + " - " + e.getMessage());
  }  
  
  List params = new ArrayList(MAX_PARAMS);
  try {
    for (Object[] row = db.getRow(paramSet); row != null; row = db.getRow(paramSet)) {
      if (row[3].equals("OUT")) {
        if (row[2].equals("REF CURSOR")) {
        cursorParamIndex = params.size();
        logDebug("Found Output Parameter at: " + cursorParamIndex);
        return params; // We don't support any more input parameters after this...
        }
        else {
          logError("Invalid output parameter type for procedure " + procName + " at position: " + row[1]);
          break;
        }
      }
      else {
        logDebug("Parameter " + params.size() + ": " + row[0]);
        params.add(row[0]);
      }
    }
    if (params.size() == 0)
      logError("Invalid procedure: " + procName);
    else // If we got here, we didn't find an OUT parameter...
      logError("Failed to find suitable output parameter type for procedure " + procName);
  } catch(KettleDatabaseException e){
    logError("Error retrieving procedure parameters for " + procName + " - " + e.getMessage());  
  } finally {
    db.closeQuery(paramSet);
  }
  
  return null;  
}
