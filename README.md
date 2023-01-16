# s3_performance_test

Version with SDK v.2 and TransferManager. 
Part size is 5MB because same part size is used in default SDK v.1 TransferManager.

nohup java -Xmx32g -Xms32g -jar s3_performance_test-1.0.jar 10 output_bucket_name output_prefix &    
