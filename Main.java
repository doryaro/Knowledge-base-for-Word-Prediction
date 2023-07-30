package org.example;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }
        AWSCredentials credentials = credentials_profile;
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        //Step1
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step1-with-combiner.jar") // This should be a full map reduce application.
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", "s3://emr-logs-mevuzarot/output/output-file1");
        StepConfig stepConfig = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Step2
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step2-with-combiner.jar") // This should be a full map reduce application.
                .withArgs("s3://emr-logs-mevuzarot/output/output-file1", "s3://emr-logs-mevuzarot/output/output-file2");
        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Step3
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step3-with-combiner.jar") // This should be a full map reduce application.
                .withArgs("s3://emr-logs-mevuzarot/output/output-file2", "s3://emr-logs-mevuzarot/output/output-file3");
        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Step4
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step4.jar") // This should be a full map reduce application.
                .withArgs("s3://emr-logs-mevuzarot/output/output-file3", "s3://emr-logs-mevuzarot/output/output-file4");
        StepConfig stepConfig4 = new StepConfig()
                .withName("step4")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //Step5
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step5.jar") // This should be a full map reduce application.
                .withArgs("s3://emr-logs-mevuzarot/output/output-file4", "s3://emr-logs-mevuzarot/output/output-file5");
        StepConfig stepConfig5 = new StepConfig()
                .withName("step5")
                .withHadoopJarStep(hadoopJarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        //Step6
        HadoopJarStepConfig hadoopJarStep6 = new HadoopJarStepConfig()
                .withJar("s3://emr-logs-mevuzarot/step6.jar") // This should be a full map reduce application.
                .withArgs("s3://emr-logs-mevuzarot/output/output-file5", "s3://emr-logs-mevuzarot/output/output-file6");
        StepConfig stepConfig6 = new StepConfig()
                .withName("step6")
                .withHadoopJarStep(hadoopJarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");



        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType("m4.xlarge")
                .withSlaveInstanceType("m4.xlarge")
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("keyPair1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("cluster from Java WITH COMBINERS - 1")
                .withInstances(instances)
                .withSteps(stepConfig,stepConfig2,stepConfig3,stepConfig4,stepConfig5,stepConfig6)

                .withLogUri("s3://emr-logs-mevuzarot/logs/")
                .withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.20.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}