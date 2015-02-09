package mimir;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.AttributedCharacterIterator.Attribute;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Map.Entry;

import oracle.jdbc.pool.OracleDataSource;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.experiment.DatabaseUtils;
import weka.experiment.InstanceQuery;
import moa.classifiers.Classifier;
import moa.core.InstancesHeader;
import moa.streams.ArffFileStream;
import moa.streams.InstanceStream;

//a result wrapper, Instances is to save the structure of the data, relation name, attribute name and type.
public class Analysis {
  
  
  public static class Model{
    public Classifier classifier;
    public int total;
    public int correct;
    public Instances instances;
    public Model(Classifier classifier,Instances instances,int total,int correct){
      this.classifier=classifier;
      this.total=total;
      this.correct=correct;
      this.instances=instances;
    }
  }
  
	public static final String JDBC_PROPERTY="jdbc.example.property";
//not used for now. Read data from an .arff file.
	public static Model reTrain(Model model,String fname){
		ArffFileStream stream=new ArffFileStream(fname,-1);
		//learner.setModelContext(stream.getHeader());
		//learner.prepareForUse();
		int prenumberSamplesCorrect=model.correct;
		int prenumberSamples=model.total;
		int numberSamplesCorrect=0;
		int numberSamples=0;
		Classifier learner=model.classifier;
		
		while(stream.hasMoreInstances()){
			Instance trainInst=stream.nextInstance();
			
			if(learner.correctlyClassifies(trainInst)){
				numberSamplesCorrect++;
			}
			learner.trainOnInstance(trainInst);
			numberSamples++;
		}
		return new Model(learner,model.instances,numberSamples+prenumberSamples,numberSamplesCorrect+prenumberSamplesCorrect);
	}
//retrain the model given feedback in dataset	
public static Model reTrain(Model model,ArrayList[] dataset,String tname,String col){
	int prenumberSamplesCorrect=model.correct;
	int prenumberSamples=model.total;
	int numberSamplesCorrect=0;
	int numberSamples=0;
	
	Classifier learner=model.classifier;
	Instance trainInst=null;
	int numAttributes=learner.getModelContext().numAttributes();
	for(int i=0;i<dataset.length;i++){
	trainInst=new DenseInstance(numAttributes);
	
	trainInst.setDataset(model.instances);
	for(int j=0;j<dataset[i].size();j++){
		if(model.instances.attribute(j).isNominal())
		{
			trainInst.setValue(j, String.valueOf(dataset[i].get(j)));
		}
		else{trainInst.setValue(j, Double.valueOf(String.valueOf((dataset[i].get(j)))));}
	}
// System.out.println(trainInst.toString());		
		if(learner.correctlyClassifies(trainInst)){
			numberSamplesCorrect++;
		}		
		learner.trainOnInstance(trainInst);
		numberSamples++;
	}
	return new Model(learner,model.instances,numberSamples+prenumberSamples,numberSamplesCorrect+prenumberSamplesCorrect);
}
//train with data from database, train on the first round.
public static Model getTrained(Classifier learner, String sql, int classIndex){
	int numberSamplesCorrect=0;
	int numberSamples=0;
	
	InstanceQuery query;
	Instances data=null;
	try {
		query = new InstanceQuery();		
		Properties props = new Properties();
        FileInputStream fis = new FileInputStream(JDBC_PROPERTY);
        //loading properites from properties file
        props.loadFromXML(fis);
        //reading proeprty
        query.setUsername(props.getProperty("user"));
		query.setPassword(props.getProperty("password"));
		query.setQuery(sql);
		
		data = query.retrieveInstances();
		data.setClassIndex(classIndex);
		learner.setModelContext(new InstancesHeader(data));
		learner.prepareForUse();
		for(int i=0;i<data.numInstances();i++){
			Instance trainInst=data.instance(i);
			if(learner.correctlyClassifies(trainInst)){
				numberSamplesCorrect++;
			}
			learner.trainOnInstance(trainInst);
			numberSamples++;
		}	
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return new Model(learner,data,numberSamples,numberSamplesCorrect);
}

public static Connection getConn(){
		Connection conn=null;
		OracleDataSource ods;
		String url = "???";
		try {
			ods = new OracleDataSource();
        // Set the user name, password, driver type and network protocol    
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(JDBC_PROPERTY);
        //loading properites from properties file
        props.loadFromXML(fis);
        //reading proeprty
        ods.setUser(props.getProperty("user"));
        ods.setPassword(props.getProperty("password"));
        url=props.getProperty("url");
        ods.setURL(url);
        // Retrieve a connection                                                                                                                                                                            
         conn = ods.getConnection();
		} catch (Exception e) {
  		  System.err.println("Connection failed to : "+url);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;	
	}
//(used in getTableInfoFromDB, so not used for now.)	
static String getColumnRange(String tablename,String colname){
		StringBuilder sb=new StringBuilder();
		try{     String query="select distinct("+colname+") from "+tablename;                                                                                                                                                   
		        Connection conn = getConn();
		        Statement stmt = conn.createStatement ();
				ResultSet rset=stmt.executeQuery(query);				
				sb.append("{");
				//System.out.println(rset.next()+rset.getString(1));
				while(rset.next ()){
					sb.append(rset.getString(1)+",");
				}
				if(sb.lastIndexOf(",")==-1){sb.append("0}"); System.err.println(sb);}
				else sb.replace(sb.lastIndexOf(","), sb.length(), "}");
		        // Close the RseultSet                                                                                                                                                                              
		       rset.close();
		        rset =  null;
		        // Close the Statement                                                                                                                                                                              
		        stmt.close();
		        stmt = null;
		        // Close the connection                                                                                                                                                                             
		        conn.close();
		        conn = null;
		    	} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("getColumnRange");
					e.printStackTrace();
				}
		return sb.toString();
	}

//(not used anymore since reading from database sets metadata directly)metadata management
static HashMap<String,String> getTableInfoFromDB(String tablename,String colname){
		tablename=tablename.toUpperCase();
		HashMap<String,String> result=new HashMap<String,String>();
		HashMap<String,String>converter= getTypeConverter();
		String sql="SELECT column_name,data_type"+
				" FROM user_tab_columns"+" where table_name='"+tablename+"'";
		try{
	        // Create a OracleDataSource instance explicitly                                                                                                                                                                                                                                                                                                                         
	        Connection conn = getConn();
	        Statement stmt = conn.createStatement ();
			ResultSet rset=stmt.executeQuery(sql);
				 while (rset.next ()){
					 String key=rset.getString(1);
					 String value=rset.getString(2);
					 if(colname.equalsIgnoreCase(key))
					 {result.put(key, getColumnRange(tablename,colname));}
					 else if(converter.keySet().contains(value)){result.put(key,converter.get(value));}
				 }
	        // Close the RseultSet                                                                                                                                                                              
	       rset.close();
	        rset =  null;
	        // Close the Statement                                                                                                                                                                              
	        stmt.close();
	        stmt = null;
	        // Close the connection                                                                                                                                                                             
	        conn.close();
	        conn = null;
	    	} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("getTableInfoFromDB");
				e.printStackTrace();
			}
		return result;
	}	
//transfer sql  type to classifier type.
public static HashMap<String,String> getTypeConverter(){
	HashMap<String,String> result=new HashMap<String,String>();
	result.put("NVARCHAR2", "nominal");
	result.put("CHAR", "nominal");
	result.put("NVARCHAR", "nominal");
	
	result.put("NUMBER", "numeric");
	result.put("INTEGER", "numeric");
	result.put("DOUBLE", "numeric");
	result.put("FLOAT", "numeric");
	
	result.put("DATE", "date");
	result.put("INTEGER", "numeric");
	return result;
}

//not used for now. Read from database and write to a .arff file
public static String getFile(Map<String,String> schema,String query,String tableName,String colName,int col){	
		try {
			//write header
			BufferedWriter writer=new BufferedWriter(new FileWriter(tableName+".arff"));
			//write @RELATION relation name.
			writer.write("@RELATION "+tableName);
			writer.newLine();
			//write @ATTRIBUTE <attribute> <type>.
			for (String key : schema.keySet()) {
				if(!colName.equalsIgnoreCase(key)){
		        writer.write("@ATTRIBUTE "+key + " " + schema.get(key));
		        writer.newLine();
				}
		    }
			//class and type,put them at last. 
			writer.write("@ATTRIBUTE class " + schema.get(colName));
	        writer.newLine();
			//write data now
			writer.write("@DATA");
			writer.newLine();
			// read from DB and write to the file.                                                                                                                                                                                                                                                                                                                             
		        Connection conn = getConn();
		        Statement stmt = conn.createStatement ();
		        ResultSet rset=stmt.executeQuery(query);
				StringBuilder sb=new StringBuilder();
				//System.out.println(rset.next()+" "+rset.getInt(1));
				while (rset.next ()){
					for(int i=0;i<schema.keySet().size();i++){
						if(i+1!=col){
						sb.append(rset.getString(i+1));
						sb.append(",");}
					}
					sb.append(rset.getString(col));
					writer.write(sb.toString());
					writer.newLine();
					sb=new StringBuilder();
				}                                                                                                                                                                    
		       rset.close();
		        rset =  null;
		                                                                                                                                                                                     
		        stmt.close();
		        stmt = null;
		                                                                                                                                                                                 
		        conn.close();
		        conn = null;
		        writer.flush();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return tableName+".arff";	
	}
//not used for now. Write data to in String[] data to an .arff file.
public static String getFile(Map<String,String> schema,String[] data,String tableName,String colName,int col){	
		try {
			//write header
			BufferedWriter writer=new BufferedWriter(new FileWriter(tableName+".arff"));
			//write @RELATION relation name.
			writer.write("@RELATION "+tableName);
			writer.newLine();
			//write @ATTRIBUTE <attribute> <type>.
			for (String key : schema.keySet()) {
				if(!colName.equalsIgnoreCase(key)){
		        writer.write("@ATTRIBUTE "+key + " " + schema.get(key));
		        writer.newLine();
				}
		    }
			//class and type,put them at last. 
			writer.write("@ATTRIBUTE class " + schema.get(colName.toUpperCase()));
	        writer.newLine();
			//write data now
			writer.write("@DATA");
			writer.newLine();
			//read data in string[] data
			for(int i=0;i<data.length;i++){
				writer.write(data[i]);
				writer.newLine();
			}
		        writer.flush();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return tableName+".arff";
		
	}
//turn data in file into insert statement and insert into database.
public static void writerInsetionStatement(String tname,String fname){
	try {BufferedReader reader=new BufferedReader(new FileReader(fname));
	//BufferedWriter writer=new BufferedWriter(new FileWriter(out+".txt"));
	String line="";
	String sql=null;
	while((line=reader.readLine())!=null){
		if(!line.startsWith("@")&&(!line.trim().isEmpty())){
			sql="Insert into "+tname+" values ("+line+");";
			InsertTuple(sql);
		}	
	}
	
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

static boolean InsertTuple(String sql){
	sql=sql.replace(";", "");
	boolean i=true;
	try{                                                                                                                                                                          
	        Connection conn = getConn();
	        Statement stmt = conn.createStatement ();
			int j=stmt.executeUpdate(sql);  
			System.out.println("Inserted tuple:"+j);
	        stmt.close();
	        stmt = null;
	        // Close the connection  
	        conn.commit();
	        conn.close();
	        conn = null;
	    	} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("InsertTuple error: "+sql);
				e.printStackTrace();
			}
	return i;
}
//given class name return Classifier object.
public static Classifier getLearner(String classfier){
		Classifier learner=null;
		try {Class c = Class.forName(classfier);
		// System.out.println(c.getName());
		learner=(Classifier) c.newInstance();
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return learner;
	}

//public static void main(String[] args) {
//		//Map<String,String> schema=new HashMap<String,String>();
//		//schema.put("erythema", "{0,1,2,3}");
//		//schema.put("scaling", "{0,1,2,3}");
//		String query="select * from dermatology";
//		String tablename="dermatology";
//		int col=34;
//		String colname="CLASS";
//		String[] data=new String[]{"2,2"
//				,"3,3","3,3"};
//	   //getColumnRange(tablename,colname);
//		//Map<String,String> schema=getTableInfoFromDB(tablename,colname);
//		//String fname=getFile(schema,query,tablename,colname,col);
//		//Model m=getTrained(getLearner("moa.classifiers.bayes.NaiveBayes"),fname);
//		//writerInsetionStatement(tablename,"dermatologytrain.arff");
//		Model m=getTrained(getLearner("moa.classifiers.bayes.NaiveBayes"), query,col);
//		//System.out.println("total: "+m.total+"\n correct: "+m.correct);	
//		double[] d={2,2,2,3,2,0,0,0,2,3,1,0,0,1,0,0,2,2,2,2,2,2,0,2,0,3,0,0,0,0,0,2,0,9};
//		double[] dd={2,3,2,3,2,0,0,0,3,2,0,0,0,1,0,0,3,2,3,2,2,2,0,3,0,3,0,0,0,0,0,0,0,36};
//		ArrayList v=new ArrayList();
//		for(int i=0;i<d.length;i++){
//			v.add(d[i]);
//		}
//		v.add('1');
//		ArrayList v1=new ArrayList();
//		for(int i=0;i<dd.length;i++){
//			v1.add(dd[i]);
//		}
//		v1.add('1');
//		ArrayList[] arr=new ArrayList[2];
//		arr[0]=v;
//		arr[1]=v;
//		//Model m1=reTrain(m,"dermatologytest1.arff");
//		Model m1=reTrain(m,arr,"dermatology","class");
//		System.out.println("classifier: "+m1.classifier.toString()+"\n"+"total: "+m1.total+"\n correct: "+m1.correct);	
//	}
}
