package com.epam.hbase;


import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {
    public static boolean tryToPutData(String rowName, String familyName, String qualifier, String value, Table table) throws IOException{
        return table.checkAndPut(Bytes.toBytes(rowName), Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value),
                createPutSentenceAddColumn(rowName, familyName, qualifier, value));
    }

    private static Put createPutSentenceAddColumn(String rowName, String familyName, String qualifier, String value){
        Put putSentence = new Put(Bytes.toBytes(rowName));
        putSentence.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));

        return putSentence;
    }

    private static Get createGetSentence(String rowName, String familyName, String qualifier){
        Get getSentence = new Get(Bytes.toBytes(rowName));
        return getSentence.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
    }
}
