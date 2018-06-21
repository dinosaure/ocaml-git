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

module type S =
sig
  module Hash: S.HASH
  module Inflate: S.INFLATE
  module Deflate: S.DEFLATE
  module FS: S.FS

  module HDec: Unpack.H with module Hash := Hash
  module PDec: Unpack.P
    with module Hash := Hash
     and module Inflate := Inflate
     and module Hunk := HDec
  module PInfo: Pack_info.S
    with module Hash := Hash
     and module Inflate := Inflate
     and module HDec := HDec
     and module PDec := PDec
  module RPDec: Unpack.D
    with module Hash := Hash
     and type Mapper.fd = FS.Mapper.fd
     and type Mapper.error = FS.error
     and module Inflate := Inflate
     and module Hunk := HDec
     and module Pack := PDec

  type status =
    | Resolved of Crc32.t * Hash.t
    | Root
    | Unresolved

  val pp_status: status Fmt.t

  val second_pass: RPDec.pack -> [ `Normalized of PInfo.path ] PInfo.t -> (int64 * (PInfo.delta * status)) array Lwt.t
end

module Make
    (Hash: S.HASH)
    (FS: S.FS)
    (Inflate: S.INFLATE)
    (Deflate: S.DEFLATE)
    (HDec: Unpack.H with module Hash := Hash)
    (PDec: Unpack.P with module Hash := Hash
                     and module Inflate := Inflate
                     and module Hunk := HDec)
    (PInfo: Pack_info.S with module Hash := Hash
                         and module Inflate := Inflate
                         and module HDec := HDec
                         and module PDec := PDec)
    (RPDec: Unpack.D with module Hash := Hash
                      and type Mapper.fd = FS.Mapper.fd
                      and type Mapper.error = FS.error
                      and module Inflate := Inflate
                      and module Hunk := HDec
                      and module Pack := PDec)
  : S with module Hash = Hash
       and module Inflate = Inflate
       and module Deflate = Deflate
       and module FS = FS
       and module HDec := HDec
       and module PDec := PDec
       and module PInfo := PInfo
       and module RPDec := RPDec
= struct

  module Hash = Hash
  module Inflate = Inflate
  module Deflate = Deflate
  module FS = Helper.FS(FS)

  module HDec = HDec
  module PDec = PDec
  module PInfo = PInfo
  module RPDec = RPDec

  open Lwt.Infix

  type status =
    | Resolved of Crc32.t * Hash.t
    | Root
    | Unresolved

  let pp_status ppf = function
    | Resolved (crc, hash) ->
      Fmt.pf ppf "(Resolved (%a, %a))" Crc32.pp crc Hash.pp hash
    | Root ->
      Fmt.pf ppf "Root"
    | Unresolved ->
      Fmt.pf ppf "Unresolved"

  type 'a protected =
    { mutable value : 'a
    ; mutex : Lwt_mutex.t }

  let is_not_root (_k, (_v, status)) = match status with
    | Root -> false
    | _ -> true

  module RefMap = Map.Make(Hash)
  module OfsMap = Map.Make(Int64)

  type context =
    { protected_idx : int protected
    ; queue : (int64 * (PInfo.delta * status)) array
    ; ofs_deltas : int list OfsMap.t
    ; ref_deltas : int list RefMap.t
    ; cache_needed : (int64, int) RPDec.Cache.t
    ; cache_object : (int64, RPDec.kind * Cstruct.t * int * RPDec.Ascendant.s) RPDec.Cache.t
    ; decoder : RPDec.pack }

  let find abs_off array =
    let rec go off len =
      if len = 0 then raise Not_found;

      let (abs_off', _) = Array.get array (off + (len / 2)) in
      if abs_off = abs_off'
      then (off + (len / 2))
      else begin
        if abs_off < abs_off'
        then go off (len / 2 - 1)
        else go (off + (len / 2) + 1) (len / 2)
      end in go 0 (Array.length array)

  let resolver ~ztmp ~zwin context (_, (value, _)) =
    match value with
    | PInfo.Unresolved _ | PInfo.Delta _ ->
      assert false
    | PInfo.Internal { length; abs_off; _ } ->
      let base = Cstruct.create length in
      let children (abs_off, hash) =
        List.map
          (fun idx -> let (abs_off, _) = Array.get context.queue idx in abs_off)
          ((try RefMap.find hash context.ref_deltas with _ -> [])
           @ (try OfsMap.find abs_off context.ofs_deltas with _ -> [])) in

      RPDec.Descendant.get_from_absolute_offset ~ztmp ~zwin
        ~cache:(context.cache_needed, context.cache_object)
        ~children
        base context.decoder abs_off >>= function
      | Ok (RPDec.Descendant.Root { children; _ }) ->
        let rec go parent depth = function
          | RPDec.Descendant.Node { patch; children; } :: rest ->
            let idx = find patch.RPDec.Descendant.offset context.queue in
            let value = PInfo.Delta { hunks_descr = patch.RPDec.Descendant.descr
                                    ; inserts = patch.RPDec.Descendant.inserts
                                    ; depth
                                    ; from = parent } in
            Array.set context.queue idx (patch.RPDec.Descendant.offset,
                                         (value, Resolved (patch.RPDec.Descendant.crc, patch.RPDec.Descendant.hash)));
            go value (succ depth) children;
            go parent depth rest
          | RPDec.Descendant.Leaf patch :: rest ->
            let idx = find patch.RPDec.Descendant.offset context.queue in
            let value = PInfo.Delta { hunks_descr = patch.RPDec.Descendant.descr
                                    ; inserts = patch.RPDec.Descendant.inserts
                                    ; depth
                                    ; from = parent } in
            Array.set context.queue idx (patch.RPDec.Descendant.offset,
                                         (value, Resolved (patch.RPDec.Descendant.crc, patch.RPDec.Descendant.hash)));
            go parent depth rest
          | [] -> () in
        go value 1 children;
        Lwt.return_unit
      | Error _ -> Lwt.return_unit

  (* XXX(dinosaure): dispatch the next root to the thread. *)
  let rec dispatcher ~ztmp ~zwin context =
    Lwt_mutex.lock context.protected_idx.mutex >>= fun () ->

    while is_not_root (Array.get context.queue context.protected_idx.value)
          && context.protected_idx.value < Array.length context.queue
    do context.protected_idx.value <- context.protected_idx.value + 1 done;

    if context.protected_idx.value >= Array.length context.queue
    then begin
      Lwt_mutex.unlock context.protected_idx.mutex;
      Lwt.return_unit
    end else begin
      let root = context.protected_idx.value in
      context.protected_idx.value <- context.protected_idx.value + 1;
      Lwt_mutex.unlock context.protected_idx.mutex;
      resolver ~ztmp ~zwin context (Array.get context.queue root) >>= fun () -> dispatcher ~ztmp ~zwin context
    end

  let second_pass decoder info =
    let matrix =
      Hashtbl.fold (fun k v acc ->
          let status = match v with
            | PInfo.Delta _ | PInfo.Unresolved _ -> Unresolved
            | PInfo.Internal _ -> Root in
          (k, (v, status)) :: acc) info.PInfo.delta []
      |> List.sort (fun (ka, _) (kb, _) -> Int64.compare ka kb)
      |> Array.of_list in

    let cache_needed = { RPDec.Cache.find = (fun _ -> None)
                       ; promote = (fun _ _ -> ()) } in
    let cache_object = RPDec.Ascendant.apply_cache (94 * 1024 * 1024) in

    let ofs_deltas, ref_deltas =
      let add_ofs ofs idx map = OfsMap.add ofs (idx :: (try OfsMap.find ofs map with _ -> [])) map in
      let add_ref hash idx map = RefMap.add hash (idx :: (try RefMap.find hash map with _ -> [])) map in

      Array.fold_left (fun (ofs_deltas, ref_deltas, idx) (abs_off, (value, _)) ->
          match value with
          | PInfo.Delta { hunks_descr = { HDec.reference = HDec.Hash hash; _ }; _ }
          | PInfo.Unresolved { hash; _ } ->
            (ofs_deltas, add_ref hash idx ref_deltas, idx + 1)
          | PInfo.Delta { hunks_descr = { HDec.reference = HDec.Offset rel_off; _ }; _ } ->
            (add_ofs Int64.(sub abs_off rel_off) idx ofs_deltas, ref_deltas, idx + 1)
          | _ -> (ofs_deltas, ref_deltas, idx + 1))
        (OfsMap.empty, RefMap.empty, 0) matrix
      |> fun (ofs_deltas, ref_deltas, _) -> (ofs_deltas, ref_deltas) in

    let context =
      { protected_idx = { value = 0
                        ; mutex = Lwt_mutex.create () }
      ; queue = matrix
      ; ofs_deltas
      ; ref_deltas
      ; cache_needed
      ; cache_object
      ; decoder } in

    let pool = Lwt_pool.create 4
        (fun () ->
           let ztmp = Cstruct.create 0x8000 in
           let zwin = Inflate.window () in

           Lwt.return (ztmp, zwin)) in

    Lwt.join
      [ Lwt_pool.use pool (fun (ztmp, zwin) -> dispatcher ~ztmp ~zwin context)
      ; Lwt_pool.use pool (fun (ztmp, zwin) -> dispatcher ~ztmp ~zwin context)
      ; Lwt_pool.use pool (fun (ztmp, zwin) -> dispatcher ~ztmp ~zwin context)
      ; Lwt_pool.use pool (fun (ztmp, zwin) -> dispatcher ~ztmp ~zwin context) ] >>= fun () ->

    let pp_data ppf (abs_off, (delta, status)) =
      Fmt.pf ppf "{ @[<hov>abs_off = %Ld;@ \
                           delta = %a;@ \
                           status = %a;@] }"
        abs_off PInfo.pp_delta delta pp_status status in

    Fmt.(pf stderr) "second_pass: %a.\n%!" (Fmt.Dump.array pp_data) matrix;
    Lwt.return matrix
end
