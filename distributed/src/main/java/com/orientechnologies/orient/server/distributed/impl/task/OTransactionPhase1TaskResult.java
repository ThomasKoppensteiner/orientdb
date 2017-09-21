/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OStreamable;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tglman
 */
public class OTransactionPhase1TaskResult implements OStreamable {

  private OTransactionResultPayload resultPayload;

  public OTransactionPhase1TaskResult() {

  }

  public OTransactionPhase1TaskResult(OTransactionResultPayload resultPayload) {
    this.resultPayload = resultPayload;
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeInt(resultPayload.getResponseType());
    switch (resultPayload.getResponseType()) {
    case OTxSuccess.ID:
    case OTxLockTimeout.ID:
      break;
    case OTxConcurrentModification.ID:
      OTxConcurrentModification pl = (OTxConcurrentModification) resultPayload;
      out.writeInt(pl.getRecordId().getClusterId());
      out.writeLong(pl.getRecordId().getClusterPosition());
      out.writeInt(pl.getVersion());
      break;
    case OTxException.ID:
      throw new UnsupportedOperationException(); //TODO!
    case OTxUniqueIndex.ID:
      OTxUniqueIndex pl2 = (OTxUniqueIndex) resultPayload;
      //RID
      out.writeInt(pl2.getRecordId().getClusterId());
      out.writeLong(pl2.getRecordId().getClusterPosition());
      //index name
      byte[] indexNameBytes = pl2.getIndex().getBytes();
      out.writeInt(indexNameBytes.length);
      out.write(indexNameBytes);
      //index key
      OType type = OType.getTypeByValue(pl2.getKey());
      int typeId = type.getId();
      out.writeInt(typeId);
      byte[] value = ORecordSerializerNetworkV37.INSTANCE.serializeValue(pl2.getKey(), type);
      out.writeInt(value.length);
      out.write(value);
    }
  }

  @Override
  public void fromStream(final DataInput in) throws IOException {
    int type = in.readInt();
    switch (type) {
    case OTxSuccess.ID:
      this.resultPayload = new OTxSuccess();
      break;
    case OTxLockTimeout.ID:
      this.resultPayload = new OTxLockTimeout();
      break;
    case OTxConcurrentModification.ID:
      ORecordId rid = new ORecordId(in.readInt(), in.readLong());
      int version = in.readInt();
      this.resultPayload = new OTxConcurrentModification(rid, version);
      break;
    case OTxException.ID:
      throw new UnsupportedOperationException(); //TODO!
    case OTxUniqueIndex.ID:
      //RID
      ORecordId id2 = new ORecordId(in.readInt(), in.readLong());
      //index name
      int arraySize = in.readInt();
      byte[] indexBytes = new byte[arraySize];
      in.readFully(indexBytes);
      String indexName = new String(indexBytes);
      //index key
      int keyTypeId = in.readInt();
      int keySize = in.readInt();
      byte[] keyBytes = new byte[keySize];
      in.readFully(keyBytes);
      OType keyType = OType.getById((byte) keyTypeId);
      Object key = ORecordSerializerNetworkV37.INSTANCE.deserializeValue(keyBytes, keyType);
      //instantiate payload
      this.resultPayload = new OTxUniqueIndex(id2, indexName, key);
    }
  }

  public OTransactionResultPayload getResultPayload() {
    return resultPayload;
  }
}
