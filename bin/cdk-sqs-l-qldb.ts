#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkSqsLQldbStack } from '../lib/cdk-sqs-l-qldb-stack';

const app = new cdk.App();
new CdkSqsLQldbStack(app, 'MyShinyStack', {
    env: {
        account: '765423797119',
        region: 'us-west-2',
    }
});
