/*
 * Adapted from Bifrost (https://github.com/Topl/Bifrost), originally
 * licensed under the Mozilla Public License 2.0. The Tessellation
 * project is otherwise Apache-2.0; this file remains under MPL-2.0.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package io.constellationnetwork.numerics.algebras

import io.constellationnetwork.numerics.Ratio

/** Tagless-final algebra for the exponential function, evaluated in exact rational arithmetic. */
trait Exp[F[_]] {
  def evaluate(x: Ratio): F[Ratio]
}
