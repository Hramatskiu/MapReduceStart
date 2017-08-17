package com.epam.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HBaseQueryHelper {
    private HBaseQueryHelper(){ }

    public static void tryToPutData(String rowName, String familyName, String qualifier, String value, Table table) throws IOException {
        table.put(createPutSentenceAddColumn(rowName, familyName, qualifier, value));
    }

    public static Get createGetSentenceForColumn(String rowName, String familyName, String qualifier){
        Get getSentence = new Get(Bytes.toBytes(rowName));
        return getSentence.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
    }

    private static Put createPutSentenceAddColumn(String rowName, String familyName, String qualifier, String value){
        Put putSentence = new Put(Bytes.toBytes(rowName));
        putSentence.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));

        return putSentence;
    }

    public static Get createGetSentence(String rowName){
        return new Get(Bytes.toBytes(rowName));
    }

    public static Scan createScan(int caching, boolean isCacheBlocks){
        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setCacheBlocks(isCacheBlocks);

        return scan;
    }

    public static Scan createScanForColumn(String familyName, String qualifierName){
        Map<String, String> familyQualifierMap = new HashMap<>();
        familyQualifierMap.put(familyName, qualifierName);

        return createScanForColumns(familyQualifierMap);
    }

    public static Scan createScanForColumns(Map<String, String> familyQualifierMap){
        Scan scan = new Scan();

        familyQualifierMap.forEach((family, qualifier) -> {scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));});

        return scan;
    }
}
