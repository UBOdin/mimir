package mimir.gprom;

import java.util.List;

import org.gprom.jdbc.jna.GProMMetadataLookupPlugin;
import org.gprom.jdbc.jna.GProM_JNA;

//import com.sun.jna.ptr.PointerByReference;


public class MimirGProMMetadataPlugin {
	
	private GProMMetadataLookupPlugin plugin;
	public MimirGProMMetadataPlugin(){
		plugin = new GProMMetadataLookupPlugin();
		plugin.isInitialized = new GProMMetadataLookupPlugin.isInitialized_callback() {
			
			@Override
			public int apply() {
				// TODO Auto-generated method stub
				return 1;
			}
		};
		plugin.initMetadataLookupPlugin = new GProMMetadataLookupPlugin.initMetadataLookupPlugin_callback() {
			
			@Override
			public int apply() {
				// TODO Auto-generated method stub
				return 1;
			}
		};
		plugin.databaseConnectionOpen = new GProMMetadataLookupPlugin.databaseConnectionOpen_callback() {
			
			@Override
			public int apply() {
				// TODO Auto-generated method stub
				return 1;
			}
		};
		plugin.databaseConnectionClose = new GProMMetadataLookupPlugin.databaseConnectionClose_callback() {
			
			@Override
			public int apply() {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		plugin.shutdownMetadataLookupPlugin = new GProMMetadataLookupPlugin.shutdownMetadataLookupPlugin_callback() {
			
			@Override
			public int apply() {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		plugin.catalogTableExists = new GProMMetadataLookupPlugin.catalogTableExists_callback() {
			
			@Override
			public int apply(String arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		plugin.catalogViewExists = new GProMMetadataLookupPlugin.catalogViewExists_callback() {
			
			@Override
			public int apply(String arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		plugin.getDataTypes = new GProMMetadataLookupPlugin.getDataTypes_callback() {
			
			@Override
			public String apply(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		}; 
		plugin.getAttributeNames = new GProMMetadataLookupPlugin.getAttributeNames_callback() {
			
			@Override
			public String apply(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		plugin.getAttributeDefaultVal = new GProMMetadataLookupPlugin.getAttributeDefaultVal_callback() {
			
			@Override
			public String apply(String arg0, String arg1, String arg2) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		plugin.isAgg = new GProMMetadataLookupPlugin.isAgg_callback() {
			
			@Override
			public int apply(String arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		plugin.isWindowFunction = new GProMMetadataLookupPlugin.isWindowFunction_callback() {
			
			@Override
			public int apply(String arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		/*plugin.getFuncReturnType = new GProMMetadataLookupPlugin.getFuncReturnType_callback() {
			@Override
			public String apply(String fName, PointerByReference args, int numArgs){
				
			}
		};*/
		/*plugin.getOpReturnType = new GProMMetadataLookupPlugin.getOpReturnType_callback() {
			@Override
			public String apply(String fName, PointerByReference args, int numArgs){
				
			}
		};*/
		plugin.getTableDefinition = new GProMMetadataLookupPlugin.getTableDefinition_callback() {
			
			@Override
			public String apply(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		plugin.getViewDefinition = new GProMMetadataLookupPlugin.getViewDefinition_callback() {
			
			@Override
			public String apply(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		plugin.getKeyInformation = new GProMMetadataLookupPlugin.getKeyInformation_callback() {
			
			@Override
			public String apply(String arg0) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		org.gprom.jdbc.jna.GProM_JNA.INSTANCE.gprom_registerMetadataLookupPlugin(plugin);
	}
}