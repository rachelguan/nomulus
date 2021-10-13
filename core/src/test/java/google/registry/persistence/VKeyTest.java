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
package google.registry.persistence;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static google.registry.testing.DatabaseHelper.newDomainBase;
import static google.registry.testing.DatabaseHelper.persistActiveContact;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.annotation.Entity;
import google.registry.model.billing.BillingEvent.OneTime;
import google.registry.model.domain.DomainBase;
import google.registry.model.registrar.RegistrarContact;
import google.registry.testing.AppEngineExtension;
import google.registry.testing.TestObject;
import google.registry.util.SerializeUtils;
import java.io.Serializable;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for {@link VKey}. */
class VKeyTest {

  @RegisterExtension
  final AppEngineExtension appEngineExtension =
      AppEngineExtension.builder()
          .withDatastoreAndCloudSql()
          .withOfyTestEntities(TestObject.class)
          .build();

  @Test
  void testOptionalAccessors() {
    VKey<TestObject> key =
        VKey.create(TestObject.class, "foo", Key.create(TestObject.create("foo")));
    assertThat(key.maybeGetSqlKey().isPresent()).isTrue();
    assertThat(key.maybeGetOfyKey().isPresent()).isTrue();
    assertThat(VKey.createSql(TestObject.class, "foo").maybeGetSqlKey()).hasValue("foo");
  }

  @Test
  void testCreateById_failsWhenParentIsNullButShouldntBe() {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> VKey.create(OneTime.class, 134L));
    assertThat(thrown).hasMessageThat().contains("BackupGroupRoot");
  }

  @Test
  void testCreateByName_failsWhenParentIsNullButShouldntBe() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> VKey.create(RegistrarContact.class, "fake@example.com"));
    assertThat(thrown).hasMessageThat().contains("BackupGroupRoot");
  }

  @Test
  void testRestoreOfy() {
    assertThat(VKey.restoreOfyFrom(null, TestObject.class, 100)).isNull();

    VKey<TestObject> key = VKey.createSql(TestObject.class, "foo");
    VKey<TestObject> restored = key.restoreOfy(TestObject.class, "bar");
    assertThat(restored.getOfyKey())
        .isEqualTo(Key.create(Key.create(TestObject.class, "bar"), TestObject.class, "foo"));
    assertThat(restored.getSqlKey()).isEqualTo("foo");

    assertThat(VKey.restoreOfyFrom(key).getOfyKey()).isEqualTo(Key.create(TestObject.class, "foo"));

    restored = key.restoreOfy(OtherObject.class, "baz", TestObject.class, "bar");
    assertThat(restored.getOfyKey())
        .isEqualTo(
            Key.create(
                Key.create(Key.create(OtherObject.class, "baz"), TestObject.class, "bar"),
                TestObject.class,
                "foo"));

    // Verify that we can use a key as the first argument.
    restored = key.restoreOfy(Key.create(TestObject.class, "bar"));
    assertThat(restored.getOfyKey())
        .isEqualTo(Key.create(Key.create(TestObject.class, "bar"), TestObject.class, "foo"));

    // Verify that we get an exception when a key is not the first argument.
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> key.restoreOfy(TestObject.class, "foo", Key.create(TestObject.class, "bar")));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Objectify keys may only be used for the first argument");

    // Verify other exception cases.
    thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> key.restoreOfy(TestObject.class, TestObject.class));
    assertThat(thrown)
        .hasMessageThat()
        .contains("class google.registry.testing.TestObject used as a key value.");

    thrown =
        assertThrows(IllegalArgumentException.class, () -> key.restoreOfy(TestObject.class, 1.5));
    assertThat(thrown).hasMessageThat().contains("Key value 1.5 must be a string or long.");

    thrown = assertThrows(IllegalArgumentException.class, () -> key.restoreOfy(TestObject.class));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Missing value for last key of type class google.registry.testing.TestObject");
  }

  @Test
  void testFromWebsafeKey() {
    // Creating an objectify key instead of a datastore key as this should get a correctly formatted
    // key path.  We have to one of our actual model object classes for this, TestObject can not be
    // reconstructed by the VKeyTranslatorFactory.
    DomainBase domain = newDomainBase("example.com", "ROID-1", persistActiveContact("contact-1"));
    Key<DomainBase> key = Key.create(domain);
    VKey<DomainBase> vkey = VKey.fromWebsafeKey(key.getString());
    assertThat(vkey.getKind()).isEqualTo(DomainBase.class);
    assertThat(vkey.getOfyKey()).isEqualTo(key);
    assertThat(vkey.getSqlKey()).isEqualTo("ROID-1");
  }

  @Test
  void testSerializeUtils_Vkey_success() throws Exception {
    Serializable sqlkey = 11111;
    VKey<TestObject> vkey = VKey.createSql(TestObject.class, sqlkey);
    byte[] bytes = SerializeUtils.serialize(vkey);
    String sqlKeyString = Base64.getEncoder().encodeToString(bytes);
    byte[] converted = Base64.getDecoder().decode(sqlKeyString);
    VKey<TestObject> newVkey = SerializeUtils.deserialize(VKey.class, converted);
    assertThat(newVkey).isEqualTo(vkey);
  }
  // test with sql only keys
  // different ways to create sql key: 1) long 2) string
  // try with more than one kind of class
  // maybe try to have parent kind as well
  @Test
  void testStringifyThenCreate_sqlKeyOnly_testObject_stringKey_success() throws Exception {
    VKey<TestObject> vkey = VKey.createSql(TestObject.class, "foo");
    String stringified = vkey.stringify();
    VKey<TestObject> newKey = VKey.create(stringified);
    assertThat(newKey).isEqualTo(vkey);
  }

  @Test
  void testStringifyThenCreate_sqlKeyOnly_domainBase_stringKey_success() throws Exception {
    VKey<DomainBase> vkey = VKey.createSql(DomainBase.class, "foo");
    String stringified = vkey.stringify();
    VKey<TestObject> newKey = VKey.create(stringified);
    assertThat(newKey).isEqualTo(vkey);
  }

  @Test
  void testStringifyThenCreate_sqlKeyOnly_testObject_longKey_success() throws Exception {
    long sqlKey = 12345;
    VKey<TestObject> vkey = VKey.createSql(TestObject.class, sqlKey);
    VKey<TestObject> newKey = VKey.create(vkey.stringify());
    assertThat(newKey).isEqualTo(vkey);
  }

  @Test
  void testStringifyThenCreate_sqlKeyOnly_domainBase_longKey_success() throws Exception {
    long sqlKey = 12345;
    VKey<DomainBase> vkey = VKey.createSql(DomainBase.class, sqlKey);
    VKey<DomainBase> newKey = VKey.create(vkey.stringify());
    assertThat(newKey).isEqualTo(vkey);
  }

  // try different ways to create ofykey
  // try with different class type
  @Test
  void testStringifyThenCreate_ofyKeyOnly_testObject_success() throws Exception {
    Key<TestObject> key = Key.create(TestObject.class, "tmpKey");
    VKey<TestObject> vkey = VKey.createOfy(TestObject.class, key);
    VKey<TestObject> newVkey = VKey.create(vkey.stringify());
    assertThat(vkey).isEqualTo(newVkey);
  }

  @Test
  void testStringifyThenCreate_ofyKeyOnly_domainBase_success() throws Exception {
    DomainBase domain = newDomainBase("example.com", "ROID-1", persistActiveContact("contact-1"));
    Key<DomainBase> key = Key.create(domain);
    VKey<DomainBase> vkey = VKey.createOfy(DomainBase.class, key);
    VKey<DomainBase> newVkey = VKey.create(vkey.stringify());
    assertThat(vkey).isEqualTo(newVkey);
  }

  @Test
  void testStringifyThenCreate_ofyKeyOnly_testObject_websafeString_success() throws Exception {
    // TODO: figure out how to make this work

    // Unknown Key type: TestObject
    // java.lang.IllegalArgumentException: Unknown Key type: TestObject
    //
    // Key<TestObject> key = Key.create(TestObject.create("foo"));
    // VKey<TestObject> vkey = VKey.fromWebsafeKey(key.getString());
    // VKey<TestObject> newVkey = VKey.create(vkey.stringify());
    // assertThat(vkey).isEqualTo(newVkey);
  }

  @Test
  void testStringifyThenCreate_ofyKeyOnly_domainBase_websafeString_success() throws Exception {
    DomainBase domain = newDomainBase("example.com", "ROID-1", persistActiveContact("contact-1"));
    Key<DomainBase> key = Key.create(domain);
    VKey<DomainBase> vkey = VKey.fromWebsafeKey(key.getString());
    VKey<DomainBase> newVkey = VKey.create(vkey.stringify());
    assertThat(vkey).isEqualTo(newVkey);
  }

  // test with vkey with both ofy and sql key
  @Test
  void testStringifyThenCreate_sqlAndofyKey_success() throws Exception {
    VKey<TestObject> originalKey1 =
        VKey.create(TestObject.class, "foo", Key.create(TestObject.create("foo")));
    String originalKeyString1 = originalKey1.stringify();
    VKey<TestObject> newKey1 = VKey.create(originalKeyString1);
    assertThat(originalKey1).isEqualTo(newKey1);

    VKey<TestObject> originalKey2 =
        VKey.create(TestObject.class, "sqlkey", Key.create(TestObject.create("test")));
    String originalKeyString2 = originalKey2.stringify();
    VKey<TestObject> newKey2 = VKey.create(originalKeyString2);
    assertThat(originalKey2).isEqualTo(newKey2);
  }

  @Test
  void testStringifyThenCreate_symmetricVkeyViaLong_success() throws Exception {
    // TODO: figure out why it doesnt work when replacing "foo" with 123, or any long

    // VKey<DomainBase> originalKey =
    //     new VKey<DomainBase>(DomainBase.class, Key.create(DomainBase.class, 123456), 123456);
    // String originalKeyString = originalKey.stringify();
    // VKey<DomainBase> newKey= VKey.create(originalKeyString);
    // assertThat(originalKey).isEqualTo(newKey);
    //
    // long key = 12234;
    // VKey<DomainBase> originalKey = VKey.create(DomainBase.class,key, Key.create(DomainBase.class,
    // key) );
    // String originalKeyString = originalKey.stringify();
    // VKey<DomainBase> newKey= VKey.create(originalKeyString);
    // assertThat(originalKey).isEqualTo(newKey);
  }

  @Test
  void testStringifyThenCreate_symmetricVkeyViaString_success() throws Exception {
    VKey<DomainBase> originalKey =
        new VKey<DomainBase>(DomainBase.class, Key.create(DomainBase.class, "foo"), "foo");
    String originalKeyString = originalKey.stringify();
    VKey<DomainBase> newKey = VKey.create(originalKeyString);
    assertThat(originalKey).isEqualTo(newKey);
  }

  @Test
  void testCreate_fromInvalidString_failure() throws Exception {
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> VKey.create("notAValidVkeyString"));
    assertThat(thrown).hasMessageThat().contains("is not a valid entry");
  }

  @Test
  void testCreate_missingClassType_failure() throws Exception {
    NullPointerException thrown =
        assertThrows(
            NullPointerException.class,
            () -> VKey.create("kindz:google.registry.testing.TestObject|sql:sqlTestKey"));
    assertThat(thrown).hasMessageThat().contains("Class type is not specified");
  }

  @Entity
  static class OtherObject {}
}
