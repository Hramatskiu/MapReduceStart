package com.epam.sqoop.creator;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;

import java.util.Map;

public class SqoopJobCreator {
    public static MJob createJob(SqoopClient sqoopClient, String fromLink, String toLink, String name, String user, Map<String, String> jobFromConfigMap,
                                  Map<String, String> jobToConfigMap, Map<String, String> driverConfigMap){
        MJob job = sqoopClient.createJob(fromLink, toLink);
        job.setName(name);
        job.setCreationUser(user);

        jobFromConfigMap.forEach((key, value) -> job.getFromJobConfig().getStringInput(key).setValue(value));
        jobToConfigMap.forEach((key, value) -> job.getToJobConfig().getStringInput(key).setValue(value));
        driverConfigMap.forEach((key, value) -> job.getDriverConfig().getStringInput(key).setValue(value));

        return job;
    }
}
