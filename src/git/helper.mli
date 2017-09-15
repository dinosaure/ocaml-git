(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 * and Romain Calascibetta <romain.calascibetta@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

val ppe : name:string -> 'a Fmt.t -> 'a Fmt.t

module BaseBytes :
sig
  include S.BASE with type t = Bytes.t

  val to_hex : t -> string
  val of_hex : string -> t
end

module MakeDecoder (A : S.ANGSTROM)
  : S.DECODER with type t = A.t
               and type raw = Cstruct.t
               and type init = Cstruct.t
               and type error = [ `Decoder of string ]

module MakeInflater (Z : S.INFLATE) (A : S.ANGSTROM)
  : S.DECODER with type t = A.t
               and type raw = Cstruct.t
               and type init = Z.window * Cstruct.t * Cstruct.t
               and type error = [ `Decoder of string | `Inflate of Z.error ]

module MakeEncoder (M : S.MINIENC)
  : S.ENCODER with type t = M.t
               and type raw = Cstruct.t
               and type init = int * M.t
               and type error = [ `Never ]

module MakeDeflater (Z : S.DEFLATE) (M : S.MINIENC)
  : S.ENCODER with type t = M.t
               and type raw = Cstruct.t
               and type init = int * M.t * int * Cstruct.t
               and type error = [ `Deflate of Z.error ]

val digest :
  (module S.IDIGEST with type buffer = Cstruct.t
                     and type t = 'hash) ->
  (module S.FARADAY with type t = 't) -> kind:string -> 't -> 'hash

val fdigest :
  (module S.IDIGEST with type buffer = Cstruct.t
                     and type t = 'hash) ->
  (module S.ENCODER with type t = 't
                     and type raw = Cstruct.t
                     and type init = int * 't
                     and type error = [ `Never ]) ->
  ?capacity:int -> tmp:Cstruct.t -> kind:string -> length:('t -> int64) -> 't -> 'hash

module type ENCODER =
sig
  type state
  type raw
  type result
  type error

  val raw_length : raw -> int
  val raw_blit   : raw -> int -> raw -> int -> int -> unit

  val eval  : raw -> state -> [ `Flush of state | `End of (state * result) | `Error of (state * error) ] Lwt.t
  val used  : state -> int
  val flush : int -> int -> state -> state
end

type ('state, 'raw, 'result, 'error) encoder =
  (module ENCODER with type state  = 'state
                   and type raw    = 'raw
                   and type result = 'result
                   and type error  = 'error)
and ('fd, 'raw, 'error) writer = 'raw -> ?off:int -> ?len:int -> 'fd -> (int, 'error) result Lwt.t

val safe_encoder_to_file :
  limit:int ->
  ('state, 'raw, 'res, 'err_encoder) encoder ->
  ('fd, 'raw, 'err_writer) writer ->
  'fd -> 'raw -> 'state ->
  ('res, [ `Stack | `Encoder of 'err_encoder | `Writer of 'err_writer ]) result Lwt.t
