# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import boto3
import csv


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    bucketName = 'zj2-hw3'
    dynamoTableName = 'zj2-hw3-dynamo-table1'
    accessKey = 'AKIA4OW4GUDNOMWQ3WFH'
    secretKey = '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'

    # connect with S3
    s3 = boto3.resource('s3', aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
    # create a bucket
    # try:
    #     s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    # except Exception as e:
    #     print(e)
    # make the bucket public
    bucket = s3.Bucket(bucketName)
    # bucket.Acl().put(ACL='public-read')

    dyndb = boto3.resource('dynamodb',
                           region_name='us-west-2',
                           aws_access_key_id=accessKey,
                           aws_secret_access_key=secretKey
                           )
    try:
        table = dyndb.create_table(
            TableName=dynamoTableName,
            KeySchema=[
                {
                    'AttributeName': 'Id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print(e)
        # if there is an exception, the table may already exist.
        table = dyndb.Table(dynamoTableName)

    # wait for the table to be created
    table.meta.client.get_waiter('table_exists').wait(TableName=dynamoTableName)
    print("Table created successfully!")

    i = 0

    with open('experiments.csv', 'rt') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        for item in csvf:
            if i == 0:
                i = i + 1
                continue

            print(item)
            rowKey = item[0]
            temp = item[1]
            conductivity = item[2]
            concentration = item[3]
            expName = item[4]

            body = open(expName, 'rb')
            s3.Object(bucketName, expName).put(Body=body)
            md = s3.Object(bucketName, expName).Acl().put(ACL='public-read')
            url = "https://s3-us-west-2.amazonaws.com/" + bucketName + "/" + expName
            metadata_item = {'Id': rowKey, 'Temp': temp,
                             'Conductivity': conductivity, 'Concentration': concentration, 'Url': url}
            try:
                table.put_item(Item=metadata_item)
            except Exception as e:
                print(e)

    queryIdNum = '1'
    response1 = table.get_item(
        Key={
            'Id': queryIdNum
        }
    )
    print("Query the result for exp" + queryIdNum + ":")

    item = response1['Item']
    print("Query Result:")
    print(item)

    print("Query Response:")
    print(response1)
