import * as cdk from '@aws-cdk/core';
import { Function, Runtime, Code, StartingPosition } from '@aws-cdk/aws-lambda';
import { SqsEventSource, KinesisEventSource } from '@aws-cdk/aws-lambda-event-sources';
import { Queue } from '@aws-cdk/aws-sqs';
import { CfnLedger, CfnStream } from '@aws-cdk/aws-qldb';
import { Stream } from '@aws-cdk/aws-kinesis';
import * as iam from '@aws-cdk/aws-iam';


export class CdkSqsLQldbStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here
    const queue = new Queue(this, 'queue', {
      queueName: 'queue',
    });

    const myLambda = new Function(this, 'MyLambda', {
      // The name of the method within your code that Lambda calls to execute your function. 
      // The format includes the file name. 
      handler: 'save-data-handler.handler',
      code: Code.fromAsset('./handlers'),
      runtime: Runtime.PYTHON_3_8,
    });

    myLambda.addEventSource(new SqsEventSource(queue));

    const ledgerName = 'my-db'
    new CfnLedger(this, 'MyDB', {
      permissionsMode: 'ALLOW_ALL',
      name: ledgerName,
    });

    // add statement to lambda's policy to be able to interract with QLDB
    myLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW, 
      actions: [ 'qldb:*' ], 
      resources: [ `arn:aws:qldb:us-west-2:765423797119:ledger/${ledgerName}` ],
    }));

    // add Kinesis stream to write QLDB stream
    const kinesisStream = new Stream(this, 'MyKinesisStream', {
        streamName: 'my-kinesis-stream'
    });

    // create role for principal: QLDB service
    const qldbStreamRole = new iam.Role(this, 'QLDBStreamRole', {
      assumedBy: new iam.ServicePrincipal('qldb.amazonaws.com')
    });

    const qldbStreamPolicyStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [ 'kinesis:PutRecord*', 'kinesis:DescribeStream', 'kinesis:ListShards' ],
      resources: [ kinesisStream.streamArn ],
    })

    new iam.Policy(this, 'QLDBStreamPolicy', {
      roles: [ qldbStreamRole ],
      statements: [ qldbStreamPolicyStatement ], 
    });

    // create Amazon QLDB journal stream
    new CfnStream(this, 'MyQLDBStream', {
      inclusiveStartTime: new Date('26 November 2020 00:00 UTC').toISOString(),
      ledgerName: ledgerName,
      roleArn: qldbStreamRole.roleArn,
      streamName: `${ledgerName}-stream`,
      kinesisConfiguration: {streamArn: kinesisStream.streamArn},
    });

    // If your function returns an error, Lambda retries the batch until processing succeeds or the data expires. 
    // To avoid stalled shards, you can configure the event source mapping to retry with a smaller batch size, 
    // limit the number of retries, or discard records that are too old.

    const queueBenefiaCompany = new Queue(this, 'queue-benefia-company', {
      queueName: 'queue-benefia-company'
    });

    const queueErgohestiaCompnay = new Queue(this, 'queue-ergohestia-company', {
      queueName: 'queue-ergohestia-company'
    });

    // create lambda to read from Kinesis stream
    const kinesisLambda = new Function(this, 'KinesisLambda', {
      handler: 'kinesis-stream-handler.handler',
      code: Code.fromAsset('./handlers'),
      runtime: Runtime.PYTHON_3_8,
    });

    // add to lambda's role policy statement to be able to manage Kinesis stream
    kinesisLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [        
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
        'sqs:ReceiveMessage'
      ],
      resources: [ queueBenefiaCompany.queueArn, queueErgohestiaCompnay.queueArn ]
    }));

    // subscribe lambda to event
    kinesisLambda.addEventSource(new KinesisEventSource(kinesisStream, {
      startingPosition: StartingPosition.TRIM_HORIZON  // return oldest records from shard first
    }));

  }
}
