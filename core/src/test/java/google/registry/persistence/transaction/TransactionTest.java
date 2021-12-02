// Copyright 2020 The Nomulus Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package google.registry.persistence.transaction;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.truth.Truth.assertThat;
import static google.registry.model.ofy.ObjectifyService.auditedOfy;
import static google.registry.persistence.transaction.TransactionManagerFactory.jpaTm;
import static google.registry.persistence.transaction.TransactionManagerFactory.ofyTm;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import google.registry.model.ImmutableObject;
import google.registry.model.ofy.CommitLogManifest;
import google.registry.model.ofy.CommitLogMutation;
import google.registry.model.ofy.Ofy;
import google.registry.persistence.VKey;
import google.registry.testing.AppEngineExtension;
import google.registry.testing.DatabaseHelper;
import google.registry.testing.FakeClock;
import google.registry.testing.InjectExtension;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.util.Comparator;
import java.util.List;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TransactionTest {

  private final FakeClock fakeClock =
      new FakeClock(DateTime.parse("2000-01-01TZ")).setAutoIncrementByOneMilli();

  @RegisterExtension
  final AppEngineExtension appEngine =
      AppEngineExtension.builder()
          .withDatastoreAndCloudSql()
          .withClock(fakeClock)
          .withOfyTestEntities(TestEntity.class)
          .withJpaUnitTestEntities(TestEntity.class)
          .build();

  @RegisterExtension public final InjectExtension inject = new InjectExtension();

  private TestEntity fooEntity, barEntity;

  @BeforeEach
  void beforeEach() {
    inject.setStaticField(Ofy.class, "clock", fakeClock);
    fooEntity = new TestEntity("foo");
    barEntity = new TestEntity("bar");
  }

  @AfterEach
  void afterEach() {
    DatabaseHelper.removeDatabaseMigrationSchedule();
  }

  @Test
  void testTransactionReplay() {
    Transaction txn = new Transaction.Builder().addUpdate(fooEntity).addUpdate(barEntity).build();
    txn.writeToDatastore();

    ofyTm()
        .transact(
            () -> {
              assertThat(ofyTm().loadByKey(fooEntity.key())).isEqualTo(fooEntity);
              assertThat(ofyTm().loadByKey(barEntity.key())).isEqualTo(barEntity);
            });

    txn = new Transaction.Builder().addDelete(barEntity.key()).build();
    txn.writeToDatastore();
    assertThat(ofyTm().exists(barEntity.key())).isEqualTo(false);

    assertThat(
            auditedOfy().load().type(CommitLogMutation.class).list().stream()
                .map(clm -> auditedOfy().load().<TestEntity>fromEntity(clm.getEntity()))
                .collect(toImmutableSet()))
        .containsExactly(fooEntity, barEntity);
    List<CommitLogManifest> manifests = auditedOfy().load().type(CommitLogManifest.class).list();
    manifests.sort(Comparator.comparing(CommitLogManifest::getCommitTime));
    assertThat(manifests.get(0).getDeletions()).isEmpty();
    assertThat(manifests.get(1).getDeletions()).containsExactly(Key.create(barEntity));
  }

  @Test
  void testSerialization() throws Exception {
    Transaction txn = new Transaction.Builder().addUpdate(barEntity).build();
    txn.writeToDatastore();

    txn = new Transaction.Builder().addUpdate(fooEntity).addDelete(barEntity.key()).build();
    txn = Transaction.deserialize(txn.serialize());

    txn.writeToDatastore();

    ofyTm()
        .transact(
            () -> {
              assertThat(ofyTm().loadByKey(fooEntity.key())).isEqualTo(fooEntity);
              assertThat(ofyTm().exists(barEntity.key())).isEqualTo(false);
            });
  }

  @Test
  void testDeserializationErrors() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeInt(12345);
    out.close();
    assertThrows(IllegalArgumentException.class, () -> Transaction.deserialize(baos.toByteArray()));

    // Test with a short byte array.
    assertThrows(
        StreamCorruptedException.class, () -> Transaction.deserialize(new byte[] {1, 2, 3, 4}));
  }

  @Test
  void testTransactionSerialization() throws IOException {
    DatabaseHelper.setMigrationScheduleToSqlPrimary(fakeClock);
    jpaTm()
        .transact(
            () -> {
              jpaTm().insert(fooEntity);
              jpaTm().insert(barEntity);
            });
      TransactionEntity txnEnt =
          jpaTm().transact(() -> jpaTm().loadByKey(VKey.createSql(TransactionEntity.class, 1L)));
      Transaction txn = Transaction.deserialize(txnEnt.getContents());
      txn.writeToDatastore();
      ofyTm()
          .transact(
              () -> {
                assertThat(ofyTm().loadByKey(fooEntity.key())).isEqualTo(fooEntity);
                assertThat(ofyTm().loadByKey(barEntity.key())).isEqualTo(barEntity);
              });

      // Verify that no transaction was persisted for the load transaction.
      assertThat(
              jpaTm().transact(() -> jpaTm().exists(VKey.createSql(TransactionEntity.class, 2L))))
          .isFalse();
  }

  @Test
  void testTransactionSerializationDisabledByDefault() {
    jpaTm()
        .transact(
            () -> {
              jpaTm().insert(fooEntity);
              jpaTm().insert(barEntity);
            });
    assertThat(jpaTm().transact(() -> jpaTm().exists(VKey.createSql(TransactionEntity.class, 1L))))
        .isFalse();
  }

  @Entity(name = "TxnTestEntity")
  @javax.persistence.Entity(name = "TestEntity")
  private static class TestEntity extends ImmutableObject {
    @Id @javax.persistence.Id private String name;

    private TestEntity() {}

    private TestEntity(String name) {
      this.name = name;
    }

    public VKey<TestEntity> key() {
      return VKey.create(TestEntity.class, name, Key.create(this));
    }
  }
}
