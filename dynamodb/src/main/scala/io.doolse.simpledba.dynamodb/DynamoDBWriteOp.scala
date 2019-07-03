package io.doolse.simpledba.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{DeleteItemRequest, PutItemRequest}

sealed trait DynamoDBWriteOp

case class PutItem(request: PutItemRequest) extends DynamoDBWriteOp

case class DeleteItem(request: DeleteItemRequest) extends DynamoDBWriteOp
