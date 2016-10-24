package TVData;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerTask extends Partitioner<Text, IntWritable> {
	@Override
	public int getPartition(Text key, IntWritable value, int numPartition) {
		
		int iRet = 3;
		if(key.toString().toLowerCase().charAt(0) >= 'a' && 
		   key.toString().toLowerCase().charAt(0) <= 'f')
			iRet = 0;
		else if(key.toString().toLowerCase().charAt(0) >= 'g' && 
				key.toString().toLowerCase().charAt(0) <= 'l')
			iRet = 1;
		else if(key.toString().toLowerCase().charAt(0) >= 'm' && 
				key.toString().toLowerCase().charAt(0) <= 'r')
			iRet = 2;
		else 
			iRet = 3;
		
		if (iRet >= numPartition)
			iRet = 3;
		
		return iRet;
	}
}
