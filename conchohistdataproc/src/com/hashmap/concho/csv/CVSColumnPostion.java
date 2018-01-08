/**
 * 
 */
package com.hashmap.concho.csv;

import java.util.Comparator;

/**
 * @author miteshrathore
 * @param <T>
 * @param <T>
 *
 */
public class CVSColumnPostion  implements Comparator<CVSColumnPostion>
{
	
	private int sourceIndex ;
	private int destinationIndex;
    private String prefix;
	private String sourceName = null;
	private String destinationName = null;

	public int getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	public int getDestinationIndex() {
		return destinationIndex;
	}

	public void setDestinationIndex(int destinationIndex) {
		this.destinationIndex = destinationIndex;
	}

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}


	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getDestinationName() {
		return destinationName;
	}

	public void setDestinationName(String destinationName) {
		this.destinationName = destinationName;
	}

	@Override
	public String toString() {
		return "ColumnPostion [sourceIndex=" + sourceIndex + ", destinationIndex=" + destinationIndex + ", prefix="
				+ prefix + ", sourceName=" + sourceName + ", destinationName=" + destinationName + "]";
	}

	 

	public int compare(CVSColumnPostion o1, CVSColumnPostion o2) {
		// TODO Auto-generated method stub
		return o1.destinationIndex-o2.destinationIndex;
		
		
	}

	
	
	

	
	

}
