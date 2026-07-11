package io.constellationnetwork.security.smt

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import scala.annotation.tailrec

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

/** A Sparse Merkle Tree that is **byte-compatible** with the TypeScript `@zk-kit/smt` v1.0.2 verifier as configured by `ded-smt-service`
  * (`new SMT(sha256Hash, false)`, hex/string mode).
  *
  * ==Why this exists (and why it does NOT use `Hasher`)==
  * The Tessellation `Hasher[F]` typeclass hashes `SHA-256(0x?? ++ Brotli(circeJSON(...)))` — a Brotli-inside-the-preimage scheme that no
  * off-the-shelf TS verifier can reproduce. A TS light client verifies a root derived from globally re-executed state with the stock
  * `@zk-kit/smt` `verifyProof` and ZERO custom code. That requires reproducing `@zk-kit/smt`'s hashing EXACTLY, which means bypassing
  * `Hasher` entirely and calling `java.security.MessageDigest.getInstance("SHA-256")` directly. This class is a deliberate sibling of
  * `SparseMerkleTree[F]` (which is Brotli-bound) — they do not and must not share the hash seam.
  *
  * ==The exact contract replicated (quoted against the `@zk-kit/smt` source)==
  *   - hash function (`smt-store.ts` `sha256Hash`): `createHash('sha256').update(childNodes.map(String).join('')).digest('hex')`. There is
  *     NO separator, NO domain prefix, NO Brotli — just the ASCII concatenation of the child hex strings, SHA-256'd, lowercase hex out.
  *   - node digest = `hash([left, right])` = `SHA-256_hex( utf8(leftHex + rightHex) )`.
  *   - leaf digest (`smt.ts` `add`: `this.hash([key, value, this.entryMark])`) = `SHA-256_hex( utf8(keyHex + valueHex + "1") )` — the
  *     literal entry-mark char `"1"` is appended.
  *   - zero node (`smt.ts` ctor: `this.zeroNode = "0"`) = the literal string `"0"`; `H(0,0) = 0` (an absent subtree contributes `"0"`).
  *   - key→path (`utils.ts` `keyToPath`): `hexToBin(key)` then `.padStart(256, "0").split("").reverse()` — left-pad to 256 bits then
  *     REVERSE (LSB-first). `calculateRoot` folds siblings from index `siblings.length-1` down to `0`, choosing `path(i) ? [sibling, node]
  *     : [node, sibling]`.
  *   - keys/values must match `/^[0-9A-Fa-f]{1,64}$/` (`utils.ts` `checkHex`).
  *
  * ==Model==
  * This mirrors `@zk-kit/smt`'s own internal representation 1:1 — a `Map[node, childNodes]` plus a `root` string — and re-implements `add`
  * / `retrieveEntry` / `createProof` / `verifyProof` with the identical bottom-up node-rebuild. Because it IS the same algorithm over the
  * same map, the resulting `root` and proofs are byte-identical to the TS library's, which the cross-language KAT proves. The tree is
  * append-only here (members are distinct keys); persistence / update / delete / compaction are intentionally out of scope (the
  * light-client commitment only needs build-root + inclusion/absence proof).
  *
  * Insertion-order independence (the `@zk-kit/smt` property): the final [[root]] is a pure function of the live key→value SET, regardless
  * of the order [[add]] is called — verified by the order-independence KAT.
  */
final class LightClientSmt private (
  private val nodes: Map[String, List[String]],
  val root: String
) {
  import LightClientSmt._

  /** Adds a NEW entry `(keyHex -> valueHex)` and returns a new tree (structural copy of the node map with the path re-hashed bottom-up).
    *
    * Mirrors `SMT.add`: retrieve the entry (collecting siblings + an optional matching leaf), then rebuild the path from the new leaf up to
    * the root. Throws if `keyHex` is already present (matching `@zk-kit/smt`, which throws `Key "..." already exists`) or if either operand
    * is not a `checkHex` hex string.
    */
  def insert(keyHex: String, valueHex: String): LightClientSmt = {
    require(checkHex(keyHex), s"key must be hex (/^[0-9A-Fa-f]{1,64}$$/): $keyHex")
    require(checkHex(valueHex), s"value must be hex (/^[0-9A-Fa-f]{1,64}$$/): $valueHex")

    val resp = retrieveEntry(keyHex)
    if (resp.entry.lift(1).isDefined)
      throw new IllegalArgumentException(s"""Key "$keyHex" already exists""")

    val path = keyToPath(keyHex)

    // `node` is the first node to fold up from: the matching leaf's digest if the path hit another leaf, else the zero node.
    val node0 = resp.matchingEntry match {
      case Some(m) => hashChildNodes(m)
      case None    => ZeroNode
    }

    // If a matching entry exists, push N zero siblings (one per additional shared path bit beyond the siblings already collected) and then
    // the matching node itself — exactly `SMT.add`'s non-membership scaffolding so the leaf lands one level below the deepest common bit.
    val siblings1: List[String] = resp.matchingEntry match {
      case None => resp.siblings
      case Some(m) =>
        val matchingPath = keyToPath(m.head)
        val extra = scala.collection.mutable.ListBuffer.empty[String]
        var i = resp.siblings.length
        while (i < 256 && matchingPath(i) == path(i)) {
          extra += ZeroNode
          i += 1
        }
        resp.siblings ++ extra.toList :+ node0
    }

    val newLeaf = List(keyHex, valueHex, EntryMark)
    val newLeafDigest = hashChildNodes(newLeaf)

    // Rebuild the path bottom-up, accumulating every (digest -> childNodes) created. Start from the existing node map plus the new leaf.
    val acc0 = nodes + (newLeafDigest -> newLeaf)
    val (newNodes, newRoot) = addNewNodes(newLeafDigest, path, siblings1, acc0)
    new LightClientSmt(newNodes, newRoot)
  }

  /** Looks up the value bound to `keyHex`, or `None` if absent. */
  def get(keyHex: String): Option[String] =
    retrieveEntry(keyHex).entry.lift(1)

  /** Builds a membership or non-membership [[MerkleProof]] for `keyHex`, in the exact shape `@zk-kit/smt`'s `verifyProof` consumes
    * (`SMT.createProof`): `{ entry, matchingEntry, siblings, root, membership: !!entry[1] }`.
    */
  def createProof(keyHex: String): MerkleProof = {
    require(checkHex(keyHex), s"key must be hex (/^[0-9A-Fa-f]{1,64}$$/): $keyHex")
    val resp = retrieveEntry(keyHex)
    MerkleProof(
      entry = resp.entry,
      matchingEntry = resp.matchingEntry,
      siblings = resp.siblings,
      root = root,
      membership = resp.entry.lift(1).isDefined
    )
  }

  /** A Scala-side mirror of `@zk-kit/smt`'s `verifyProof` — for self-checks; the authoritative verifier is the TS library. Recomputes the
    * root from the proof's `entry`/`matchingEntry`/`siblings` and compares to `proof.root` (and, for a matching-entry non-membership proof,
    * checks the sibling-depth ≤ first-common-bits bound).
    */
  def verifyProof(proof: MerkleProof): Boolean = LightClientSmt.verifyProof(proof)

  /** The internal node map size (parity with the TS `getSize()`); diagnostic only. */
  def size: Int = nodes.size

  // --- internals (1:1 with @zk-kit/smt `SMT`) ---------------------------------------------------------------------------------------------

  /** `SMT.retrieveEntry`: walk from the root down the key's path. Returns the found leaf (member), or `[key]` with an optional matching
    * leaf (non-member), always with the siblings collected along the way.
    */
  private def retrieveEntry(keyHex: String): EntryResponse = {
    val path = keyToPath(keyHex)
    val siblings = scala.collection.mutable.ListBuffer.empty[String]

    @tailrec def loop(node: String, i: Int): EntryResponse =
      if (node == ZeroNode) EntryResponse(List(keyHex), None, siblings.toList) // path led to a zero node
      else {
        val childNodes = nodes(node)
        val direction = path(i)
        if (childNodes.lift(2).isDefined) {
          // leaf node
          if (childNodes.head == keyHex) EntryResponse(childNodes, None, siblings.toList)
          else EntryResponse(List(keyHex), Some(childNodes), siblings.toList)
        } else {
          val next = childNodes(direction)
          siblings += childNodes(1 - direction)
          loop(next, i + 1)
        }
      }

    loop(root, 0)
  }

  /** `SMT.addNewNodes`: fold the start `node` with each sibling from `siblings.length-1` down to `0`, hashing `path(i) ? [sibling, node] :
    * [node, sibling]`, recording every created node. Returns the (updated map, root).
    */
  private def addNewNodes(
    start: String,
    path: Vector[Int],
    siblings: List[String],
    acc: Map[String, List[String]]
  ): (Map[String, List[String]], String) = {
    val sibVec = siblings.toVector
    var node = start
    var m = acc
    var i = sibVec.length - 1
    while (i >= 0) {
      val childNodes = if (path(i) == 1) List(sibVec(i), node) else List(node, sibVec(i))
      node = hashChildNodes(childNodes)
      m = m + (node -> childNodes)
      i -= 1
    }
    (m, node)
  }
}

object LightClientSmt {

  /** `SMT.retrieveEntry`'s result: the found/placeholder `entry` (`[key,value,"1"]` member or `[key]` non-member), an optional matching
    * leaf (non-member collision), and the siblings collected root→leaf. Internal to the tree walk.
    */
  private final case class EntryResponse(entry: List[String], matchingEntry: Option[List[String]], siblings: List[String])

  /** The literal zero node (`SMT` ctor: `this.zeroNode = "0"`). An absent subtree contributes `"0"`; `H(0,0)=0`. */
  final val ZeroNode: String = "0"

  /** The literal leaf entry-mark appended in the leaf pre-image (`SMT` ctor: `this.entryMark = "1"`). */
  final val EntryMark: String = "1"

  /** The empty tree (root = zero node, no materialized internal nodes). */
  val empty: LightClientSmt = new LightClientSmt(Map.empty, ZeroNode)

  /** Builds a tree from a `Map[keyHex, valueHex]`. The final root is INDEPENDENT of iteration order (the `@zk-kit/smt` set-determinism
    * property) — proven by the order-independence KAT.
    */
  def fromMap(entries: Map[String, String]): LightClientSmt =
    entries.foldLeft(empty) { case (t, (k, v)) => t.insert(k, v) }

  /** Builds a tree from a sequence of `(keyHex, valueHex)` pairs (left-to-right insert; keys must be distinct). */
  def fromEntries(pairs: Seq[(String, String)]): LightClientSmt =
    pairs.foldLeft(empty) { case (t, (k, v)) => t.insert(k, v) }

  // --- pure hashing / path (byte-exact `@zk-kit/smt`) ---------------------------------------------------------------------------------------

  /** `sha256Hash(childNodes)` from `ded-smt-service/src/smt-store.ts`: `sha256(childNodes.map(String).join('')).digest('hex')`. Here every
    * child is already a `String`, so this is `SHA-256_hex( utf8(concat(childNodes)) )` with NO separator / prefix / Brotli. Uses
    * `MessageDigest.getInstance("SHA-256")` directly — NOT the Tessellation `Hasher` (which is Brotli-bound and incompatible).
    */
  def hashChildNodes(childNodes: List[String]): String =
    sha256Hex(childNodes.mkString)

  /** SHA-256 of the UTF-8 bytes of `s`, rendered as lowercase hex (matching Node's `.digest('hex')`). */
  def sha256Hex(s: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val digest = md.digest(s.getBytes(StandardCharsets.UTF_8))
    bytesToHex(digest)
  }

  private def bytesToHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
      val b = bytes(i) & 0xff
      sb.append(HexChars((b >>> 4) & 0xf))
      sb.append(HexChars(b & 0xf))
      i += 1
    }
    sb.toString
  }

  private val HexChars: Array[Char] = "0123456789abcdef".toCharArray

  /** `utils.ts` `checkHex`: `/^[0-9A-Fa-f]{1,64}$/`. */
  def checkHex(n: String): Boolean =
    n != null && HexRegex.pattern.matcher(n).matches()

  private val HexRegex = "^[0-9A-Fa-f]{1,64}$".r

  /** `utils.ts` `hexToBin`: first nibble via `Number("0x"+n[0]).toString(2)` (NO pad), each subsequent nibble `.padStart(4,"0")`. */
  def hexToBin(n: String): String = {
    val sb = new StringBuilder
    sb.append(Integer.parseInt(n.substring(0, 1), 16).toBinaryString) // first nibble: no left-pad (mirrors JS exactly)
    var i = 1
    while (i < n.length) {
      val nibble = Integer.parseInt(n.substring(i, i + 1), 16).toBinaryString
      sb.append("0" * (4 - nibble.length)).append(nibble) // padStart(4, "0")
      i += 1
    }
    sb.toString
  }

  /** `utils.ts` `keyToPath`: `hexToBin(key).padStart(256, "0").split("").reverse().map(Number)` — left-pad to 256 bits, then REVERSE
    * (LSB-first). The result is a length-256 vector of 0/1.
    */
  def keyToPath(keyHex: String): Vector[Int] = {
    val bin = hexToBin(keyHex)
    val padded = ("0" * math.max(0, 256 - bin.length)) + bin // padStart(256, "0")
    padded.reverse.map(c => c - '0').toVector
  }

  /** `utils.ts` `getFirstCommonElements`: the longest shared prefix of two int arrays. */
  def getFirstCommonElements(a: Vector[Int], b: Vector[Int]): Vector[Int] = {
    val min = if (a.length < b.length) a else b
    var i = 0
    while (i < min.length) {
      if (a(i) != b(i)) return min.take(i)
      i += 1
    }
    min
  }

  /** `SMT.calculateRoot`: fold `node` with each sibling from `siblings.length-1` down to `0` using `path(i) ? [sibling,node] :
    * [node,sibling]`.
    */
  private def calculateRoot(node0: String, path: Vector[Int], siblings: List[String]): String = {
    val sib = siblings.toVector
    var node = node0
    var i = sib.length - 1
    while (i >= 0) {
      val childNodes = if (path(i) == 1) List(sib(i), node) else List(node, sib(i))
      node = hashChildNodes(childNodes)
      i -= 1
    }
    node
  }

  /** Scala mirror of `SMT.verifyProof` (see [[LightClientSmt.verifyProof(proof:* the instance method]]). */
  def verifyProof(proof: MerkleProof): Boolean =
    proof.matchingEntry match {
      case None =>
        // No matching entry: membership iff entry has a value; node is the leaf digest (member) or the zero node (non-member).
        val path = keyToPath(proof.entry.head)
        val node = if (proof.entry.lift(1).isDefined) hashChildNodes(proof.entry) else ZeroNode
        calculateRoot(node, path, proof.siblings) == proof.root
      case Some(matching) =>
        // Matching-entry non-membership: the matching leaf must reach the root, and the path must diverge no earlier than the proof depth.
        val matchingPath = keyToPath(matching.head)
        val node = hashChildNodes(matching)
        if (calculateRoot(node, matchingPath, proof.siblings) == proof.root) {
          val path = keyToPath(proof.entry.head)
          val firstMatchingBits = getFirstCommonElements(path, matchingPath)
          proof.siblings.length <= firstMatchingBits.length
        } else false
    }

  // --- proof shape (serializes to exactly what `@zk-kit/smt` `verifyProof` accepts) ----------------------------------------------------------

  /** The `SMT.createProof` result, byte-for-byte the JSON `@zk-kit/smt`'s `verifyProof` consumes:
    *   - `entry`: `[key, value, "1"]` for a member, `[key]` for a non-member.
    *   - `matchingEntry`: `Some([matchKey, matchValue, "1"])` only for a non-member whose path collided with an existing leaf, else `None`
    *     (serialized as `undefined`/absent — see [[MerkleProof.encoder]]).
    *   - `siblings`: the authentication path (hex node strings), root→leaf order as collected.
    *   - `root`: the tree root (hex, or `"0"` for the empty tree).
    *   - `membership`: `!!entry[1]`.
    */
  final case class MerkleProof(
    entry: List[String],
    matchingEntry: Option[List[String]],
    siblings: List[String],
    root: String,
    membership: Boolean
  )

  object MerkleProof {

    private val derived: Encoder[MerkleProof] = deriveEncoder[MerkleProof]

    /** Encodes `matchingEntry: None` by OMITTING the field (so the JSON has no `matchingEntry` key), matching how `@zk-kit/smt` emits
      * `matchingEntry: undefined` — and crucially how its `verifyProof` branches on `!merkleProof.matchingEntry` (absent ⇒ falsy ⇒ the
      * no-matching branch). Encoding `null` would also be falsy in JS, but omission is the exact JSON the library produces.
      */
    implicit val encoder: Encoder[MerkleProof] = derived.mapJson { json =>
      json.asObject.fold(json) { obj =>
        val cleaned = if (obj("matchingEntry").exists(_.isNull)) obj.remove("matchingEntry") else obj
        io.circe.Json.fromJsonObject(cleaned)
      }
    }

    implicit val decoder: Decoder[MerkleProof] = deriveDecoder[MerkleProof]
  }
}
