package com.amazonaws.samples;

import java.io.IOException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class EMRClass {

	public static void main(String[] args) {
		
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e1) {
		    System.out.println("Credentials were not properly entered into AwsCredentials.properties.");
		    System.out.println(e1.getMessage());
		    System.exit(-1);
		}
		
		AmazonElasticMapReduce client = AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion("us-west-2").build();
		
		

	    // predefined steps. See StepFactory for list of predefined steps
	    StepConfig hive = new StepConfig("Hive", new StepFactory().newInstallHiveStep());

	    // A custom step
	    HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
	        .withJar("s3://rfidrecord/wordcount.jar") // optional main class, this can be omitted if jar above has a manifest
	        .withArgs("s3://rfidrecord/test.txt", "s3://rfidrecord/output"); // optional list of arguments
	    StepConfig customStep = new StepConfig("Step1", hadoopConfig1);

	    AddJobFlowStepsResult result = client.addJobFlowSteps(new AddJobFlowStepsRequest()
		  .withJobFlowId("j-1RS1RB8CYW7EC")
		  .withSteps(hive, customStep));
	    
          System.out.println(result.getStepIds());

	}
}
