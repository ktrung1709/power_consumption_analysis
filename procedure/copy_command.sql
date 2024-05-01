copy power_consumption from 's3://electricity-consumption-master-data/'
iam_role 'arn:aws:iam::232291906525:role/service-role/AmazonRedshift-CommandsAccessRole-20240409T013151'
IGNOREHEADER 1
delimiter ',';