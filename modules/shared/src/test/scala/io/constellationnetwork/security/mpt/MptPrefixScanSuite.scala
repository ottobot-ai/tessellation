package io.constellationnetwork.security.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.StringCodec._
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import weaver.MutableIOSuite

object MptPrefixScanSuite extends MutableIOSuite {

  type Res = (HasherSelector[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit kryo =>
      JsonSerializer.forAsync[IO].asResource.map { implicit json =>
        (
          HasherSelector.forSync[IO](
            Hasher.forJson[IO],
            Hasher.forKryo[IO],
            hashSelect = new HashSelect { def select(ordinal: SnapshotOrdinal): HashLogic = KryoHash }
          ),
          json
        )
      }
    }

  private val addr1 = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val addr2 = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")
  private val addr3 = Address("DAG5bvqxSJmbWVwcKWEU7nb3sgTnN1QZMPi4F8Cc")

  test("entriesWithPrefix isolates entries to one field across mixed inserts") { implicit res =>
    implicit val (hs, js) = res
    val _ = js
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()
        store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])

        balKey1 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr1)
        balKey2 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr2)
        txKey1 = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr1)
        txKey3 = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr3)

        _ <- store.insert(balKey1, "balance-1")
        _ <- store.insert(balKey2, "balance-2")
        _ <- store.insert(txKey1, "tx-1")
        _ <- store.insert(txKey3, "tx-3")

        balPrefix <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.Balances)
        txPrefix <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.LastTxRefs)

        balEntries <- producer.entriesWithPrefix(balPrefix)
        txEntries <- producer.entriesWithPrefix(txPrefix)
        allEntries <- producer.entries
      } yield
        expect.all(
          allEntries.size == 4,
          balEntries.size == 2,
          txEntries.size == 2,
          balEntries.keySet.intersect(txEntries.keySet).isEmpty,
          balEntries.keySet.union(txEntries.keySet) == allEntries.keySet
        )
    }
  }

  test("getAllForPrefix decodes each value with the given codec") { implicit res =>
    implicit val (hs, js) = res
    val _ = js
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()
        store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])

        balKey1 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr1)
        balKey2 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr2)
        txKey1 = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr1)

        _ <- store.insert(balKey1, "balance-1")
        _ <- store.insert(balKey2, "balance-2")
        _ <- store.insert(txKey1, "tx-1")

        balPrefix <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.Balances)
        decoded <- store.getAllForPrefix[String](balPrefix)
      } yield
        expect.all(
          decoded.size == 2,
          decoded.values.toSet == Set("balance-1", "balance-2")
        )
    }
  }

  test("getAllForPrefix fails closed on an undecodable matched value while point get remains optional") { implicit res =>
    implicit val (hs, js) = res
    val _ = js
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()
        store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
        key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr1)
        hex <- GlobalStateKey.toHex[IO](key)
        _ <- producer.insertBytes(Map(hex -> Array[Byte](1))).flatMap(_.liftTo[IO])
        prefix <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.Balances)
        pointRead <- store.get[String](key)
        prefixRead <- store.getAllForPrefix[String](prefix).attempt
      } yield
        expect.all(
          pointRead.isEmpty,
          prefixRead.left.exists(e => e.getMessage.contains("undecodable bytes") && e.getMessage.contains(hex.value))
        )
    }
  }

  test("hypergraphFieldPrefix with contract scopes to that contract only") { implicit res =>
    implicit val (hs, js) = res
    val _ = js
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()
        store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])

        contractA = addr1
        contractB = addr2

        // Same field (ActiveAllowSpends), different contract scopes
        keyA1 = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, contractA, addr3)
        keyB1 = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, contractB, addr3)
        // No-contract for the same field
        keyNone1 = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, addr3)

        _ <- store.insert(keyA1, "v-A")
        _ <- store.insert(keyB1, "v-B")
        _ <- store.insert(keyNone1, "v-None")

        prefixA <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.ActiveAllowSpends, Some(contractA))
        prefixNone <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.ActiveAllowSpends)

        decodedA <- store.getAllForPrefix[String](prefixA)
        decodedNone <- store.getAllForPrefix[String](prefixNone)
      } yield
        expect.all(
          decodedA.values.toSet == Set("v-A"),
          decodedNone.values.toSet == Set("v-None")
        )
    }
  }
}
