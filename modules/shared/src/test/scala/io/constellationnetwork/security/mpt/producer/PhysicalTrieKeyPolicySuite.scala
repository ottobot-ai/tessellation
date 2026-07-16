package io.constellationnetwork.security.mpt.producer

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

object PhysicalTrieKeyPolicySuite extends SimpleIOSuite {

  private def repositoryRoot(start: Path): Path = {
    val candidates = Iterator.iterate(start)(_.getParent).takeWhile(_ != null)
    candidates
      .find(path => Files.exists(path.resolve("build.sbt")) && Files.isDirectory(path.resolve("modules")))
      .getOrElse(throw new IllegalStateException(s"Unable to locate repository root from $start"))
  }

  test("concrete producer constructors remain companion-only capabilities") {
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val declarations = List(
        "InMemoryMerklePatriciaProducer" ->
          root.resolve(
            "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/producer/InMemoryMerklePatriciaProducer.scala"
          ),
        "FileSystemMerklePatriciaProducer" ->
          root.resolve(
            "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/producer/FileSystemMerklePatriciaProducer.scala"
          )
      )

      declarations.map {
        case (name, path) =>
          val source = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
          val declaration = source.linesIterator.find(_.startsWith(s"final class $name")).getOrElse("")
          expect(declaration.endsWith(" private (")) &&
          expect(source.sliding(s"new $name".length).count(_ == s"new $name") == 1)
      }.reduce(_ && _)
    }
  }

  test("fixed-width policy construction rejects zero, negative, and overflowing widths") {
    IO.pure(
      expect.all(
        PhysicalTrieKeyPolicy.fixedWidth(0) == Left(PhysicalTrieKeyPolicy.InvalidFixedWidth(0)),
        PhysicalTrieKeyPolicy.fixedWidth(-1) == Left(PhysicalTrieKeyPolicy.InvalidFixedWidth(-1)),
        PhysicalTrieKeyPolicy.fixedWidth(Int.MaxValue) == Left(PhysicalTrieKeyPolicy.InvalidFixedWidth(Int.MaxValue))
      )
    )
  }

  test("fixed-width policy enforces width and canonical spelling on complete images and every mutation") {
    val policy = PhysicalTrieKeyPolicy.fixedWidth(2).toOption.get
    val short = Hex("aa")
    val exact = Hex("aabb")
    val uppercase = Hex("AABB")

    IO.pure(
      expect.all(
        policy.exactWidthBytes.contains(2),
        policy.validateComplete(Vector(exact)) == Right(()),
        policy.validateComplete(Vector(short)) == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
        policy.validateEach(Vector(short)) == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
        policy.validateInsertion(Set(exact), Vector(short)) == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
        policy.validateEach(Vector(uppercase)) == Left(NonCanonicalPhysicalTrieKey(uppercase, exact))
      )
    )
  }

  test("fixed-width insertion does not enumerate retained keys; generic insertion still performs ROOT-011 comparison") {
    val fixed = PhysicalTrieKeyPolicy.fixedWidth(2).toOption.get
    var fixedEnumerated = false
    var genericEnumerated = false

    val fixedResult = fixed.validateInsertion(
      {
        fixedEnumerated = true
        throw new AssertionError("fixed-width insertion enumerated retained keys")
      },
      Vector(Hex("aabb"))
    )
    val genericResult = PhysicalTrieKeyPolicy.Generic.validateInsertion(
      {
        genericEnumerated = true
        Set(Hex("aa"))
      },
      Vector(Hex("aa00"))
    )

    IO.pure(
      expect.all(
        fixedResult == Right(()),
        !fixedEnumerated,
        genericEnumerated,
        genericResult == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00")))
      )
    )
  }
}
