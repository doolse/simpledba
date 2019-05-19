package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.WriteOp
import software.amazon.awssdk.services.dynamodb.model.{DeleteItemRequest, PutItemRequest}

sealed trait DynamoDBWriteOp extends WriteOp

case class PutItem(request: PutItemRequest) extends DynamoDBWriteOp

case class DeleteItem(request: DeleteItemRequest) extends DynamoDBWriteOp
