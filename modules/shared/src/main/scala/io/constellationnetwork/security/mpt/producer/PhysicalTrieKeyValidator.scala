package io.constellationnetwork.security.mpt.producer

import java.util.Locale

import io.constellationnetwork.security.hex.Hex

/** Generic physical trie-key grammar.
  *
  * This validates byte-addressed trie mechanics only: lowercase even-length hex and an unambiguous set of terminal paths. It deliberately
  * does not know `GlobalStateKey`, field identifiers, namespaces, or value codecs. Consensus consumers must apply their semantic key
  * grammar separately.
  *
  * The empty key is a valid generic root terminal when it is the only key. It collides with every non-empty key, so a candidate containing
  * both is rejected. GL0's semantic key grammar is responsible for rejecting an empty key where no rooted field permits one.
  */
object PhysicalTrieKeyValidator {

  /** Validate one operation key without assuming anything about the rest of the trie. Intended for reuse by insert/remove boundaries. */
  def validateKey(key: Hex): Either[PhysicalTrieKeyError, Unit] =
    validateShape(key).flatMap { _ =>
      val canonical = canonicalize(key)
      Either.cond(key == canonical, (), NonCanonicalPhysicalTrieKey(key, canonical))
    }

  /** Validate operation-key spelling without treating the supplied keys as a candidate trie. This is appropriate for removals: two keys in
    * the request may have a prefix relationship even though at most one can exist in a valid trie.
    */
  def validateEachKey(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] =
    keys.iterator.foldLeft[Either[PhysicalTrieKeyError, Unit]](Right(())) {
      case (Right(_), key)     => validateKey(key)
      case (left @ Left(_), _) => left
    }

  /** Validate a complete candidate or operation key set without normalizing or merging any physical key. */
  def validateKeys(keys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] = {
    val sortedPhysical = keys.iterator.toVector.sortBy(_.value)

    for {
      _ <- sortedPhysical.foldLeft[Either[PhysicalTrieKeyError, Unit]](Right(())) {
        case (Right(_), key)     => validateShape(key)
        case (left @ Left(_), _) => left
      }
      normalized = sortedPhysical.map(key => canonicalize(key) -> key).sortBy { case (path, physical) => (path.value, physical.value) }
      _ <- rejectDuplicatePaths(normalized)
      _ <- sortedPhysical.foldLeft[Either[PhysicalTrieKeyError, Unit]](Right(())) {
        case (right @ Right(_), key) =>
          val canonical = canonicalize(key)
          if (key == canonical) right else Left(NonCanonicalPhysicalTrieKey(key, canonical))
        case (left @ Left(_), _) => left
      }
      _ <- rejectTerminalCollisions(normalized)
    } yield ()
  }

  /** Validate a complete entry sequence before materializing it as a map. This is load-bearing for typed-key encoders: calling `.toMap`
    * first would silently discard two distinct logical entries that encode to the same physical path.
    */
  def materializeEntries[V](entries: Iterable[(Hex, V)]): Either[PhysicalTrieKeyError, Map[Hex, V]] = {
    val materialized = entries.iterator.toVector
    validateKeys(materialized.map(_._1)).map(_ => materialized.toMap)
  }

  /** Validate an upsert batch against an already-validated current image. Exact current-key replacements are allowed. This avoids sorting
    * or copying the complete image on every ordinary insert: terminal collisions are found by checking proper byte-prefix membership in the
    * current and incoming key sets.
    */
  def validateInsertion(currentKeys: Set[Hex], upsertKeys: Iterable[Hex]): Either[PhysicalTrieKeyError, Unit] = {
    val incoming = upsertKeys.iterator.toVector

    validateKeys(incoming).flatMap { _ =>
      val incomingSet = incoming.toSet
      val currentTerminalCollisions = incoming.iterator
        .filterNot(currentKeys.contains)
        .flatMap { descendant =>
          properPrefixes(descendant).find(currentKeys.contains).map(TerminalPhysicalTrieKeyCollision(_, descendant))
        }
      val incomingTerminalCollisions = currentKeys.iterator
        .filterNot(incomingSet.contains)
        .flatMap { descendant =>
          properPrefixes(descendant).find(incomingSet.contains).map(TerminalPhysicalTrieKeyCollision(_, descendant))
        }

      (currentTerminalCollisions ++ incomingTerminalCollisions).toVector
        .sortBy(error => (error.terminal.value, error.descendant.value))
        .headOption
        .toLeft(())
    }
  }

  private def validateShape(key: Hex): Either[PhysicalTrieKeyError, Unit] = {
    val value = key.value

    if ((value.length & 1) != 0) Left(OddLengthPhysicalTrieKey(key))
    else {
      val invalidIndex = value.indexWhere { character =>
        !(character >= '0' && character <= '9') &&
        !(character >= 'a' && character <= 'f') &&
        !(character >= 'A' && character <= 'F')
      }
      if (invalidIndex >= 0) Left(InvalidDigitPhysicalTrieKey(key, invalidIndex, value.charAt(invalidIndex)))
      else Right(())
    }
  }

  private def canonicalize(key: Hex): Hex =
    Hex(key.value.toLowerCase(Locale.ROOT))

  private def properPrefixes(key: Hex): Iterator[Hex] =
    (0 until key.value.length by 2).iterator.map(length => Hex(key.value.take(length)))

  private def rejectDuplicatePaths(normalized: Vector[(Hex, Hex)]): Either[PhysicalTrieKeyError, Unit] =
    normalized
      .zip(normalized.drop(1))
      .collectFirst {
        case ((path, first), (nextPath, second)) if path == nextPath =>
          DuplicatePhysicalTriePath(path, first, second)
      }
      .toLeft(())

  private def rejectTerminalCollisions(normalized: Vector[(Hex, Hex)]): Either[PhysicalTrieKeyError, Unit] =
    normalized
      .zip(normalized.drop(1))
      .collectFirst {
        case ((path, terminal), (nextPath, descendant)) if nextPath.value.startsWith(path.value) =>
          TerminalPhysicalTrieKeyCollision(terminal, descendant)
      }
      .toLeft(())
}
