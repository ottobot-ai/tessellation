package io.constellationnetwork.security.smt

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for the versioned / historical SMT layer (Part C.2 slice S3): "prove key K = V at finalized ordinal N" — inclusion AND absence at
  * a past version, root persistence across later commits, and bounded retention.
  */
object VersionedSmtSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))
  private def key(b: Byte): Hex = Hex.fromBytes(Array.fill[Byte](32)(b))
  private def bytes(s: String): Array[Byte] = s.getBytes("UTF-8")

  test("prove (key, V) at a PAST version after later commits mutate the key") { res =>
    implicit val (_, h, _) = res
    val verifier = SmtVerifier.make[IO]
    val k = key(1)

    for {
      vsmt <- VersionedSmt.make[IO](retention = 8)
      // v1: k = "first"
      _ <- vsmt.commit(ord(1), Map(k -> bytes("first")), Set.empty)
      // v2: k = "second"
      _ <- vsmt.commit(ord(2), Map(k -> bytes("second")), Set.empty)
      // v3: k removed
      _ <- vsmt.commit(ord(3), Map.empty, Set(k))

      root1 <- vsmt.rootAt(ord(1)).map(_.get)
      root2 <- vsmt.rootAt(ord(2)).map(_.get)

      // prove k == "first" AT v1
      p1 <- vsmt.proveAt(ord(1), k)
      v1ok <- p1 match {
        case Right(incl: SmtProof.Inclusion) =>
          verifier.verify(root1, incl).map {
            case Right(verified) =>
              verified.value match {
                case SmtEntry.Present(_, v) => expect(v.sameElements(bytes("first")))
                case other                  => failure(s"expected Present(first), got $other")
              }
            case Left(err) => failure(s"verify@v1 failed: $err")
          }
        case other => IO.pure(failure(s"expected Inclusion@v1, got $other"))
      }

      // prove k == "second" AT v2 (different root, different value)
      p2 <- vsmt.proveAt(ord(2), k)
      v2ok <- p2 match {
        case Right(incl: SmtProof.Inclusion) =>
          verifier.verify(root2, incl).map {
            case Right(verified) =>
              verified.value match {
                case SmtEntry.Present(_, v) => expect(v.sameElements(bytes("second")))
                case other                  => failure(s"expected Present(second), got $other")
              }
            case Left(err) => failure(s"verify@v2 failed: $err")
          }
        case other => IO.pure(failure(s"expected Inclusion@v2, got $other"))
      }

      // roots at distinct versions differ
      rootsDiffer = expect(root1 =!= root2)
    } yield v1ok && v2ok && rootsDiffer
  }

  test("prove ABSENCE at a version where the key was removed; the SAME key is present at an earlier version") { res =>
    implicit val (_, h, _) = res
    val verifier = SmtVerifier.make[IO]
    val k = key(2)
    val other = key(3)

    for {
      vsmt <- VersionedSmt.make[IO](retention = 8)
      _ <- vsmt.commit(ord(10), Map(k -> bytes("v"), other -> bytes("o")), Set.empty)
      _ <- vsmt.commit(ord(11), Map.empty, Set(k)) // remove k at v11

      root11 <- vsmt.rootAt(ord(11)).map(_.get)
      pAbs <- vsmt.proveAt(ord(11), k)
      absOk <- pAbs match {
        case Right(abs: SmtProof.Absence) =>
          verifier.verify(root11, abs).map {
            case Right(verified) =>
              verified.value match {
                case SmtEntry.Absent(_) => success
                case other2             => failure(s"expected Absent@v11, got $other2")
              }
            case Left(err) => failure(s"verify absence@v11 failed: $err")
          }
        case other2 => IO.pure(failure(s"expected Absence@v11, got $other2"))
      }

      // k is still present at v10
      root10 <- vsmt.rootAt(ord(10)).map(_.get)
      pIncl <- vsmt.proveAt(ord(10), k)
      inclOk <- pIncl match {
        case Right(incl: SmtProof.Inclusion) => verifier.verify(root10, incl).map(r => expect(r.isRight))
        case other2                          => IO.pure(failure(s"expected Inclusion@v10, got $other2"))
      }
    } yield absOk && inclOk
  }

  test("rootAt / proveAt on an unknown or pruned version ⇒ UnknownVersion") { res =>
    implicit val (_, h, _) = res
    val k = key(4)

    for {
      vsmt <- VersionedSmt.make[IO](retention = 2) // keep only the latest 2 versions
      _ <- vsmt.commit(ord(1), Map(k -> bytes("a")), Set.empty)
      _ <- vsmt.commit(ord(2), Map(k -> bytes("b")), Set.empty)
      _ <- vsmt.commit(ord(3), Map(k -> bytes("c")), Set.empty) // evicts v1

      retained <- vsmt.retainedVersions
      rootV1 <- vsmt.rootAt(ord(1)) // pruned
      proveV1 <- vsmt.proveAt(ord(1), k) // pruned
      proveV99 <- vsmt.proveAt(ord(99), k) // never committed

      rootV3 <- vsmt.rootAt(ord(3)) // present
    } yield
      expect(retained === List(ord(2), ord(3))) &&
        expect(rootV1.isEmpty) &&
        expect(proveV1 === Left(SmtProofError.UnknownVersion(ord(1)))) &&
        expect(proveV99 === Left(SmtProofError.UnknownVersion(ord(99)))) &&
        expect(rootV3.isDefined)
  }

  test("a version's root is STABLE across later commits (historical root does not drift)") { res =>
    implicit val (_, h, _) = res
    val k1 = key(5)
    val k2 = key(6)

    for {
      vsmt <- VersionedSmt.make[IO](retention = 8)
      r1Committed <- vsmt.commit(ord(1), Map(k1 -> bytes("x")), Set.empty)
      _ <- vsmt.commit(ord(2), Map(k2 -> bytes("y")), Set.empty)
      _ <- vsmt.commit(ord(3), Map(k1 -> bytes("z")), Set.empty)
      r1Later <- vsmt.rootAt(ord(1)).map(_.get)
    } yield expect(r1Committed === r1Later)
  }

  test("re-committing the same version with the same changes reproduces the same root (order-independent commit)") { res =>
    implicit val (_, h, _) = res
    val k1 = key(7)
    val k2 = key(8)
    val k3 = key(9)

    for {
      a <- VersionedSmt.make[IO](retention = 4)
      rootA <- a.commit(ord(1), Map(k1 -> bytes("1"), k2 -> bytes("2"), k3 -> bytes("3")), Set.empty)

      b <- VersionedSmt.make[IO](retention = 4)
      // different Map iteration content order, same logical set
      rootB <- b.commit(ord(1), Map(k3 -> bytes("3"), k1 -> bytes("1"), k2 -> bytes("2")), Set.empty)
    } yield expect(rootA === rootB)
  }
}
