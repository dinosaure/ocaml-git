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

module type S = sig

  module Hash: S.HASH
  module FS: S.FS

  type t = [ `Peeled of Hash.t | `Ref of string * Hash.t ] list

  module A: S.ANGSTROM with type t = t
  module D: S.DECODER
    with type t = t
     and type init = Cstruct.t
     and type error = Error.Decoder.t
  module M: S.MINIENC with type t = t
  module E: S.ENCODER
    with type t = t
     and type init = int * t

  type error =
    [ Error.Decoder.t
    | FS.error Error.FS.t ]

  val pp_error: error Fmt.t

  val write: fs:FS.t -> root:Fpath.t -> ?capacity:int -> raw:Cstruct.t -> t ->
    (unit, error) result Lwt.t

  val read: fs:FS.t -> root:Fpath.t -> dtmp:Cstruct.t -> raw:Cstruct.t ->
    (t, error) result Lwt.t
end

module Make (H: S.HASH) (FS: S.FS) = struct

  module Hash = H
  module FS = Helper.FS(FS)

  type t = [ `Peeled of hash | `Ref of string * hash ] list
  and hash = Hash.t

  module A = struct
    type nonrec t = t

    open Angstrom

    let hash =
      take (Hash.Digest.length * 2)
      >>| Hash.of_hex

    let end_of_line =
      skip_while (function '\r' | '\n' -> false | _ -> true)
      *> peek_char >>= function
      | Some '\n' -> take 1 >>= fun _ -> return ()
      | Some '\r' -> take 2 >>= fun _ -> return ()
      | Some _ -> assert false
      | None -> return ()

    let info =
      (option false (char '^' *> return true)) >>= fun peeled ->
      hash >>= fun hash -> match peeled with
      | true  -> end_of_line *> return (`Peeled hash)
      | false ->
        take_while (function '\x20'
                           | '\x09' -> true
                           | _      -> false)
        *> take_while (function '\000' .. '\039' -> false
                              | '\127'           -> false
                              | '~' | '^'
                              | ':' | '?' | '*'
                              | '['              -> false
                              | _                -> true)
        >>= fun refname ->
        end_of_line >>= fun () ->
        return (`Ref (refname, hash))

    let decoder =
      fix @@ fun m ->
      (peek_char >>= function
        | Some '#' ->
          end_of_line *> m
        | Some _ ->
          info >>= fun x -> m >>= fun r -> return (x :: r)
        | None -> return [])
  end

  module M = struct
    type nonrec t = t

    open Minienc

    let write_newline k e =
      if Sys.win32
      then write_string "\r\n" k e
      else write_string "\n" k e

    let write_hash x k e =
      write_string (Hash.to_hex x) k e

    let write_info x k e = match x with
      | `Peeled hash ->
        (write_char '^'
         @@ write_hash hash k)
          e
      | `Ref (refname, hash) ->
        (write_hash hash
         @@ write_char ' '
         @@ write_string refname k)
          e

    let write_list ?(sep = fun k e -> k e) write_data lst k e =
      let rec aux l e = match l with
        | [] -> k e
        | [ x ] -> write_data x k e
        | x :: r ->
          (write_data x
           @@ sep
           @@ aux r)
            e
      in
      aux lst e

    let encoder l k e =
      (write_string "# pack-refs with: peeled fully-peeled"
       @@ write_newline
       @@ write_list ~sep:write_newline write_info l k)
        e
  end

  module D = Helper.MakeDecoder(A)
  module E = Helper.MakeEncoder(M)

  type fs_error = FS.error Error.FS.t
  type error =
    [ fs_error
    | Error.Decoder.t ]

  let pp_error ppf = function
    | #Error.Decoder.t as err -> Error.Decoder.pp_error ppf err
    | #fs_error as err -> Error.FS.pp_error FS.pp_error ppf err

  open Lwt.Infix

  let read ~fs ~root ~dtmp ~raw =
    let decoder = D.default dtmp in
    let path = Fpath.(root / "packed-refs") in
    FS.with_open_r fs path @@ fun read ->
    let rec loop decoder = match D.eval decoder with
      | `End (_, value)               -> Lwt.return (Ok value)
      | `Error (_, (#Error.Decoder.t as err)) ->
        Lwt.return Error.(v @@ Error.Decoder.with_path path err)
      | `Await decoder ->
        FS.File.read raw read >>= function
        | Error err -> Lwt.return Error.(v @@ Error.FS.err_read path err)
        | Ok 0      -> loop (D.finish decoder)
        | Ok n      ->
          match D.refill (Cstruct.sub raw 0 n) decoder with
          | Ok decoder              -> loop decoder
          | Error (#Error.Decoder.t as err) ->
            Lwt.return Error.(v @@ Error.Decoder.with_path path err)
    in
    loop decoder

  module Encoder = struct
    module E = struct
      type state  = E.encoder
      type result = int
      type error  = E.error
      type rest = [ `Flush of state | `End of (state * result) ]
      let used  = E.used
      let flush = E.flush
      let eval raw state = match E.eval raw state with
        | `Error err    -> Lwt.return (`Error (state, err))
        | #rest as rest -> Lwt.return rest
    end
    include Helper.Encoder(E)(FS)
  end

  let err_stack = Error (`IO "Impossible to store the packed-refs file")

  let write ~fs ~root ?(capacity = 0x100) ~raw value =
    let state = E.default (capacity, value) in
    let path = Fpath.(root / "packed-refs") in
    Encoder.to_file fs path raw state >|= function
    | Ok _ -> Ok ()
    | Error #fs_error as err -> err
    | Error (`Encoder #Error.never) -> assert false
end
